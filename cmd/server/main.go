package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/yourname/ratelimiter/internal/cache"
	"github.com/yourname/ratelimiter/internal/config"
	"github.com/yourname/ratelimiter/internal/fallback"
	"github.com/yourname/ratelimiter/internal/limiter"
	"github.com/yourname/ratelimiter/internal/metrics"
	"github.com/yourname/ratelimiter/internal/middleware"
	"github.com/yourname/ratelimiter/internal/penalty"
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

	// application-level context — cancelled on shutdown to stop all background goroutines
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	// ── Redis client ─────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	})

	pingCtx, pingCancel := context.WithTimeout(appCtx, 3*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		slog.Warn("redis unavailable at startup — will use fallback strategy",
			"addr", cfg.Redis.Addr, "error", err)
	} else {
		slog.Info("redis connected", "addr", cfg.Redis.Addr)
	}

	// ── Base limiter config ──────────────────────────────────────────────────
	limCfg := limiter.Config{
		Limit:    cfg.Limiter.Limit,
		Window:   cfg.Limiter.Window,
		BurstMax: cfg.Limiter.BurstMax,
	}

	// ── Primary limiter (Redis or in-memory fallback) ────────────────────────
	var primary limiter.Limiter
	redisLimiter, redisErr := limiter.NewRedisLimiter(rdb, limCfg)
	if redisErr != nil {
		slog.Warn("could not create redis limiter, falling back to in-memory", "error", redisErr)
		primary = limiter.NewSlidingWindowCounter(limCfg)
	} else {
		primary = redisLimiter
	}

	// ── Chained (hierarchical) limiter ───────────────────────────────────────
	// Adds a per-tenant tier and a global tier on top of the per-key limit.
	if cfg.Limiter.Chain.Enabled {
		tenantCfg := limiter.Config{Limit: cfg.Limiter.Chain.TenantLimit, Window: cfg.Limiter.Window}
		globalCfg := limiter.Config{Limit: cfg.Limiter.Chain.GlobalLimit, Window: cfg.Limiter.Window}

		primary = limiter.NewChainedLimiter(
			limiter.ChainTier{
				Name:    "per_key",
				Limiter: primary,
			},
			limiter.ChainTier{
				Name:    "per_tenant",
				Limiter: limiter.NewSlidingWindowCounter(tenantCfg),
				KeyFunc: func(key string) string {
					// extract tenant prefix from "tenant:<id>:user:<uid>" or fall back to "global"
					if i := strings.Index(key, ":"); i >= 0 {
						return "tenant:" + key[:i]
					}
					return "tenant:global"
				},
			},
			limiter.ChainTier{
				Name:    "global",
				Limiter: limiter.NewSlidingWindowCounter(globalCfg),
				KeyFunc: func(_ string) string { return "global" },
			},
		)
		slog.Info("chain limiter enabled",
			"tenant_limit", cfg.Limiter.Chain.TenantLimit,
			"global_limit", cfg.Limiter.Chain.GlobalLimit)
	}

	// ── Adaptive limiter ─────────────────────────────────────────────────────
	// Wraps the primary (or chained) limiter and sheds load when latency rises.
	var adaptiveLimiter *limiter.AdaptiveLimiter
	if cfg.Limiter.Adaptive.Enabled {
		adaptiveCfg := limiter.AdaptiveConfig{
			LowWatermarkMs:  cfg.Limiter.Adaptive.LowWatermarkMs,
			HighWatermarkMs: cfg.Limiter.Adaptive.HighWatermarkMs,
			DecreaseRatio:   cfg.Limiter.Adaptive.DecreaseRatio,
			IncreaseStep:    cfg.Limiter.Adaptive.IncreaseStep,
			MinMultiplier:   cfg.Limiter.Adaptive.MinMultiplier,
			EWMAAlpha:       cfg.Limiter.Adaptive.EWMAAlpha,
		}
		adaptiveLimiter = limiter.NewAdaptiveLimiter(primary, adaptiveCfg)
		primary = adaptiveLimiter
		slog.Info("adaptive limiter enabled",
			"high_watermark_ms", cfg.Limiter.Adaptive.HighWatermarkMs,
			"min_multiplier", cfg.Limiter.Adaptive.MinMultiplier)
	}

	// ── Fallback handler ─────────────────────────────────────────────────────
	localLimiter := buildLocalLimiter(cfg.Limiter.Algorithm, limCfg)
	fb := fallback.New(primary, localLimiter, fallback.Strategy(cfg.Limiter.FallbackStrategy))

	// ── Local cache ──────────────────────────────────────────────────────────
	var localCache *cache.LocalCache
	if cfg.Limiter.CacheTTL > 0 {
		localCache = cache.New(appCtx, cfg.Limiter.CacheTTL)
		slog.Info("local cache enabled", "ttl", cfg.Limiter.CacheTTL)
	}

	// ── Penalty box ──────────────────────────────────────────────────────────
	var penaltyBox *penalty.Box
	if cfg.Limiter.Penalty.Enabled {
		penaltyBox = penalty.New(rdb, penalty.Config{
			Threshold:    cfg.Limiter.Penalty.Threshold,
			StrikeWindow: cfg.Limiter.Penalty.StrikeWindow,
			BasePenalty:  cfg.Limiter.Penalty.BasePenalty,
			MaxPenalty:   cfg.Limiter.Penalty.MaxPenalty,
		})
		slog.Info("penalty box enabled",
			"threshold", cfg.Limiter.Penalty.Threshold,
			"base_penalty", cfg.Limiter.Penalty.BasePenalty)
	}

	// ── Cost function (cost-weighted limiting) ───────────────────────────────
	costMap := buildCostMap(cfg.Routes)
	costFn := func(r *http.Request) int64 {
		if cost, ok := costMap[r.URL.Path]; ok {
			return cost
		}
		return 1
	}

	// ── Middleware ───────────────────────────────────────────────────────────
	rl := middleware.New(fb, localCache, extractorFor(cfg.Limiter.KeyType), costFn, penaltyBox)

	// ── HTTP routes ──────────────────────────────────────────────────────────
	mux := http.NewServeMux()

	mux.HandleFunc("/api/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"message":"hello","time":%d}`, time.Now().Unix())
	})

	// Health check — not rate limited
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	if cfg.Metrics.Enabled {
		mux.Handle(cfg.Metrics.Path, promhttp.Handler())
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api") {
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

	// ── Background: emit adaptive multiplier metric ──────────────────────────
	if adaptiveLimiter != nil {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-appCtx.Done():
					return
				case <-ticker.C:
					metrics.AdaptiveMultiplier.
						WithLabelValues(adaptiveLimiter.Name()).
						Set(adaptiveLimiter.Multiplier())
				}
			}
		}()
	}

	// ── Graceful shutdown ────────────────────────────────────────────────────
	go func() {
		slog.Info("server starting",
			"port", cfg.Server.Port,
			"algorithm", cfg.Limiter.Algorithm,
			"limit", cfg.Limiter.Limit,
			"window", cfg.Limiter.Window,
			"fallback", cfg.Limiter.FallbackStrategy,
			"chain", cfg.Limiter.Chain.Enabled,
			"adaptive", cfg.Limiter.Adaptive.Enabled,
			"penalty", cfg.Limiter.Penalty.Enabled,
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	appCancel() // stops cache eviction goroutine and adaptive metrics goroutine

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

// buildCostMap builds an exact-path → cost lookup from route config.
// O(1) per request; prefix matching can be added with a sorted slice if needed.
func buildCostMap(routes []config.RouteConfig) map[string]int64 {
	m := make(map[string]int64, len(routes))
	for _, r := range routes {
		if r.Cost > 1 {
			m[r.Path] = r.Cost
		}
	}
	return m
}
