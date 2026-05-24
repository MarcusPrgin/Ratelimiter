package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/yourname/ratelimiter/internal/cache"
	"github.com/yourname/ratelimiter/internal/config"
	"github.com/yourname/ratelimiter/internal/fallback"
	"github.com/yourname/ratelimiter/internal/limiter"
	"github.com/yourname/ratelimiter/internal/middleware"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// ── Redis client ────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Warn("redis unavailable at startup — will use fallback strategy",
			"addr", cfg.Redis.Addr, "error", err)
	} else {
		slog.Info("redis connected", "addr", cfg.Redis.Addr)
	}

	// ── Primary limiter (Redis) ─────────────────────────────────────────────
	limCfg := limiter.Config{
		Limit:    cfg.Limiter.Limit,
		Window:   cfg.Limiter.Window,
		BurstMax: cfg.Limiter.BurstMax,
	}

	var primary limiter.Limiter
	redisLimiter, redisErr := limiter.NewRedisLimiter(rdb, limCfg)
	if redisErr != nil {
		slog.Warn("could not create redis limiter, falling back to in-memory", "error", redisErr)
		primary = limiter.NewSlidingWindowCounter(limCfg)
	} else {
		primary = redisLimiter
	}

	// ── Local fallback limiter (in-memory) ──────────────────────────────────
	localLimiter := buildLocalLimiter(cfg.Limiter.Algorithm, limCfg)

	// ── Fallback handler ────────────────────────────────────────────────────
	fb := fallback.New(primary, localLimiter, fallback.Strategy(cfg.Limiter.FallbackStrategy))

	// ── Local cache (in front of Redis) ─────────────────────────────────────
	var localCache *cache.LocalCache
	if cfg.Limiter.CacheTTL > 0 {
		localCache = cache.New(cfg.Limiter.CacheTTL)
		slog.Info("local cache enabled", "ttl", cfg.Limiter.CacheTTL)
	}

	// ── Key extractor ────────────────────────────────────────────────────────
	extractor := extractorFor(cfg.Limiter.KeyType)

	// ── Middleware ───────────────────────────────────────────────────────────
	rl := middleware.New(fb, localCache, extractor)

	// ── HTTP routes ──────────────────────────────────────────────────────────
	mux := http.NewServeMux()

	// demo endpoint
	mux.HandleFunc("/api/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"message":"hello","time":%d}`, time.Now().Unix())
	})

	// health check — not rate limited
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Prometheus metrics
	if cfg.Metrics.Enabled {
		mux.Handle(cfg.Metrics.Path, promhttp.Handler())
	}

	// apply rate limiting middleware to all /api/* routes
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.URL.Path) >= 4 && r.URL.Path[:4] == "/api" {
			rl.Handler(mux).ServeHTTP(w, r)
		} else {
			mux.ServeHTTP(w, r)
		}
	})

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      handler,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// ── Graceful shutdown ────────────────────────────────────────────────────
	go func() {
		slog.Info("server starting", "port", cfg.Server.Port,
			"algorithm", cfg.Limiter.Algorithm,
			"limit", cfg.Limiter.Limit,
			"window", cfg.Limiter.Window,
			"fallback", cfg.Limiter.FallbackStrategy,
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down server...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}
	slog.Info("server stopped")
}

func buildLocalLimiter(algorithm string, cfg limiter.Config) limiter.Limiter {
	switch algorithm {
	case "fixed_window":
		return limiter.NewFixedWindow(cfg)
	case "sliding_window_log":
		return limiter.NewSlidingWindowLog(cfg)
	case "token_bucket":
		return limiter.NewTokenBucket(cfg)
	default:
		return limiter.NewSlidingWindowCounter(cfg)
	}
}

func extractorFor(keyType string) middleware.KeyExtractor {
	switch keyType {
	case "user":
		return middleware.ByUserID
	case "tenant":
		return middleware.ByTenant
	default:
		return middleware.ByIP
	}
}
