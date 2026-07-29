// Command server runs the rate limiting HTTP service.
//
// It wires the limiter as a chain of decorators, outermost first:
//
//	middleware → penalty box → lease cache → fallback+breaker → adaptive → chain → Redis
//
// The order is load-bearing:
//
//   - The penalty box is outermost so a blocked caller is refused before it can
//     consume a lease or a Redis round trip.
//   - The lease cache sits above the fallback so a local hit costs nothing at all.
//   - The fallback wraps the components that can actually fail, so it observes
//     Redis errors directly.
//   - The adaptive limiter sits below the lease cache on purpose. Above it, it
//     would measure lease hits (~1µs), the average would never approach the
//     watermark, and load shedding would never engage.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/MarcusPrgin/Ratelimiter/internal/config"
	"github.com/MarcusPrgin/Ratelimiter/internal/fallback"
	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
	"github.com/MarcusPrgin/Ratelimiter/internal/metrics"
	"github.com/MarcusPrgin/Ratelimiter/internal/middleware"
	"github.com/MarcusPrgin/Ratelimiter/internal/penalty"
)

func main() {
	if err := run(); err != nil {
		// Logging here rather than at every failure point keeps startup errors on
		// one path, and returning from main lets deferred cleanup run — which
		// os.Exit inside a goroutine, as an earlier version did, silently skips.
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config.yaml", "path to the config file")
	checkOnly := flag.Bool("check", false, "validate the config and exit")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if *checkOnly {
		_, _ = fmt.Fprintln(os.Stdout, "configuration is valid")
		return nil
	}

	log := cfg.NewLogger(os.Stdout)
	slog.SetDefault(log)

	// Cancelled on shutdown, so every background goroutine has one stop signal.
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	rdb := newRedisClient(cfg)
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Warn("closing redis client", "error", err)
		}
	}()

	pingRedis(appCtx, rdb, cfg, log)

	app, err := build(cfg, rdb, log)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:           app.handler,
		ReadHeaderTimeout: cfg.Server.ReadTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return appCtx },
	}

	log.Info("server starting",
		"port", cfg.Server.Port,
		"algorithm", cfg.Limiter.Algorithm,
		"limit", cfg.Limiter.Limit,
		// slog renders a Duration as raw nanoseconds; String() keeps it readable.
		"window", cfg.Limiter.Window.String(),
		"key_type", cfg.Limiter.KeyType,
		"fallback", cfg.Limiter.Fallback.Strategy,
		"lease", cfg.Limiter.Lease.Enabled,
		"chain", cfg.Limiter.Chain.Enabled,
		"adaptive", cfg.Limiter.Adaptive.Enabled,
		"penalty", cfg.Limiter.Penalty.Enabled,
	)

	// A failed listen must reach the shutdown path, not exit from a goroutine.
	serveErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case sig := <-quit:
		log.Info("shutdown signal received", "signal", sig.String())
	}

	// Stop accepting and drain. Cancel the app context only afterwards: doing it
	// first would cancel the request contexts of the very requests being drained.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed, closing connections", "error", err)
		_ = srv.Close()
	}
	appCancel()

	log.Info("server stopped")
	return nil
}

// app holds what run needs after wiring.
type app struct {
	handler http.Handler
}

// build assembles the limiter chain, metrics and HTTP routes.
func build(cfg *config.Config, rdb *redis.Client, log *slog.Logger) (*app, error) {
	algo := cfg.Algorithm()
	// From the config package, not rebuilt here: two places constructing this is how
	// the validated config and the one actually used drift apart.
	core := cfg.LimiterCore()

	// ── innermost: the shared limiter, optionally in tiers ───────────────────
	primary, err := limiter.NewRedisLimiter(rdb, algo, core)
	if err != nil {
		return nil, fmt.Errorf("redis limiter: %w", err)
	}

	var tierNames []string
	if cfg.Limiter.Chain.Enabled {
		chain, err := buildChain(rdb, algo, core, cfg)
		if err != nil {
			return nil, err
		}
		tierNames = chain.Tiers()
		primary = chain
	}

	// ── adaptive load shedding ───────────────────────────────────────────────
	var adaptive *limiter.AdaptiveLimiter
	if cfg.Limiter.Adaptive.Enabled {
		adaptive, err = limiter.NewAdaptiveLimiter(primary, cfg.AdaptiveConfig())
		if err != nil {
			return nil, fmt.Errorf("adaptive limiter: %w", err)
		}
		primary = adaptive
	}

	// ── failure strategy + circuit breaker ───────────────────────────────────
	// Built only for local_fallback: the other strategies never consult it, and an
	// in-memory limiter allocates a 256-shard map that would sit empty forever.
	fbCfg := cfg.FallbackConfig()
	var local limiter.Limiter
	if fbCfg.Strategy == fallback.LocalFallback {
		local = newLocalLimiter(algo, core)
	}
	fb, err := fallback.New(primary, local, fbCfg, log)
	if err != nil {
		return nil, fmt.Errorf("fallback: %w", err)
	}
	var chained limiter.Limiter = fb

	// ── local quota leasing ──────────────────────────────────────────────────
	var lease *limiter.LeaseCache
	if cfg.Limiter.Lease.Enabled {
		lease, err = limiter.NewLeaseCache(chained, cfg.LeaseConfig())
		if err != nil {
			return nil, fmt.Errorf("lease cache: %w", err)
		}
		chained = lease
	}

	// ── penalty box (outermost limiter stage) ────────────────────────────────
	var box *penalty.Box
	if cfg.Limiter.Penalty.Enabled {
		box, err = penalty.New(chained, rdb, cfg.PenaltyConfig(), log)
		if err != nil {
			return nil, fmt.Errorf("penalty box: %w", err)
		}
		chained = box
	}

	// ── metrics ──────────────────────────────────────────────────────────────
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(
		collectors.ProcessCollectorOpts{}))

	deniedBy := append([]string{
		penalty.DeniedBy, limiter.ShedDeniedBy, fallback.DeniedByUnavailable,
	}, tierNames...)

	rec := metrics.NewRecorder(reg, chained.Name(), cfg.Limiter.KeyType, deniedBy)
	metrics.RegisterSources(reg, chained.Name(),
		buildSources(adaptive, lease, box, fb, local))

	// ── HTTP ─────────────────────────────────────────────────────────────────
	costs := cfg.CostMap()
	mwCfg := middleware.Config{
		KeyMode:          cfg.KeyMode(),
		TrustedProxyHops: cfg.Limiter.TrustedProxyHops,
		Cost: func(r *http.Request) int64 {
			if c, ok := costs[r.URL.Path]; ok {
				return c
			}
			return 1
		},
	}
	mw, err := middleware.New(chained, mwCfg, rec, log)
	if err != nil {
		return nil, fmt.Errorf("middleware: %w", err)
	}

	return &app{handler: routes(cfg, rdb, mw, reg, log)}, nil
}

// buildChain builds the hierarchical tiers.
//
// Every tier is backed by Redis. An earlier version used in-memory limiters for
// the tenant and global tiers, which made a "global limit across the entire
// service" actually a per-process limit — N nodes admitted N× the configured
// ceiling, and the tier that was supposed to be the last line of defence was the
// one that did not work.
func buildChain(rdb *redis.Client, algo limiter.Algorithm, core limiter.Config,
	cfg *config.Config) (*limiter.ChainedLimiter, error) {

	perKey, err := limiter.NewRedisLimiter(rdb, algo, core)
	if err != nil {
		return nil, fmt.Errorf("chain per_key tier: %w", err)
	}

	tenantCore := core
	tenantCore.Limit = cfg.Limiter.Chain.TenantLimit
	tenantCore.BurstMax = 0 // derive from the tier's own limit
	perTenant, err := limiter.NewRedisLimiter(rdb, algo, tenantCore)
	if err != nil {
		return nil, fmt.Errorf("chain per_tenant tier: %w", err)
	}

	globalCore := core
	globalCore.Limit = cfg.Limiter.Chain.GlobalLimit
	globalCore.BurstMax = 0
	global, err := limiter.NewRedisLimiter(rdb, algo, globalCore)
	if err != nil {
		return nil, fmt.Errorf("chain global tier: %w", err)
	}

	return limiter.NewChainedLimiter(
		limiter.ChainTier{Name: "per_key", Limiter: perKey},
		limiter.ChainTier{
			Name:    "per_tenant",
			Limiter: perTenant,
			// Derive the tenant from the composite key.
			//
			// The previous implementation took everything before the first colon of
			// "tenant:acme|user:alice", which is the literal string "tenant" — so
			// every tenant shared a single bucket and the per-tenant tier was a
			// second global tier wearing a different label.
			KeyFunc: func(key string) string {
				if t := middleware.TenantOf(key); t != "" {
					return "tenant:" + t
				}
				return "tenant:_untenanted"
			},
		},
		limiter.ChainTier{
			Name:    "global",
			Limiter: global,
			KeyFunc: func(string) string { return "global" },
		},
	)
}

// buildSources wires live component state to the pull-based collectors. Only
// enabled components contribute, so a minimal deployment exports a minimal
// surface.
func buildSources(adaptive *limiter.AdaptiveLimiter, lease *limiter.LeaseCache,
	box *penalty.Box, fb *fallback.Handler, local limiter.Limiter) metrics.Sources {

	s := metrics.Sources{
		TrackedKeys: map[string]func() float64{},
		Degraded:    func() float64 { return float64(fb.Stats().Degraded) },
		BreakerOpen: func() float64 {
			if fb.Stats().Open {
				return 1
			}
			return 0
		},
	}

	if adaptive != nil {
		s.AdaptiveShed = func() float64 { return float64(adaptive.Shed()) }
		s.AdaptiveMultiplier = adaptive.Multiplier
		s.AdaptiveLatencyMs = adaptive.EWMA
	}
	if lease != nil {
		s.LeaseHits = func() float64 { return float64(lease.Stats().Hits) }
		s.LeaseMisses = func() float64 { return float64(lease.Stats().Misses) }
		s.LeaseHitRatio = func() float64 { return lease.Stats().HitRate() }
		s.TrackedKeys["lease"] = func() float64 { return float64(lease.Stats().Keys) }
	}
	if box != nil {
		s.PenaltyDenied = func() float64 { return float64(box.Stats().Denied) }
		s.PenaltyEscalations = func() float64 { return float64(box.Stats().Escalations) }
		s.TrackedKeys["penalty"] = func() float64 { return float64(box.Stats().Keys) }
	}
	if k, ok := local.(interface{ Keys() int }); ok {
		s.TrackedKeys["local_fallback"] = func() float64 { return float64(k.Keys()) }
	}
	return s
}

// routes builds the HTTP mux. Rate limiting applies to /api/* only; health and
// metrics endpoints must stay reachable precisely when the service is throttling.
func routes(cfg *config.Config, rdb *redis.Client, mw *middleware.RateLimiter,
	reg *prometheus.Registry, log *slog.Logger) http.Handler {

	api := http.NewServeMux()
	api.HandleFunc("/api/hello", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"message": "hello",
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})
	// Declared so the documented cost-weighting examples resolve instead of 404ing
	// after passing through the limiter.
	api.HandleFunc("/api/search", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"results": []string{},
			"query":   r.URL.Query().Get("q"),
		})
	})
	api.HandleFunc("/api/export", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "accepted"})
	})

	root := http.NewServeMux()
	// Built once. Calling mw.Handler per request, as an earlier version did,
	// allocates a closure and an http.HandlerFunc on every request.
	root.Handle("/api/", mw.Handler(api))

	// Liveness: is the process itself healthy. Must not depend on Redis — a
	// dependency outage would otherwise make the orchestrator restart every node
	// during the exact incident the fallback strategy exists to survive.
	root.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Readiness: can this node serve correctly, which does include Redis.
	root.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := rdb.Ping(ctx).Err(); err != nil {
			log.WarnContext(ctx, "readiness check failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "degraded", "redis": err.Error(),
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "redis": "ok"})
	})

	// Kept as an alias so existing probes and compose healthchecks keep working.
	root.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	if cfg.Metrics.Enabled {
		root.Handle(cfg.Metrics.Path, promhttp.HandlerFor(reg, promhttp.HandlerOpts{
			ErrorHandling: promhttp.ContinueOnError,
		}))
	}
	return root
}

func newRedisClient(cfg *config.Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		MinIdleConns: cfg.Redis.MinIdleConns,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
		MaxRetries:   cfg.Redis.MaxRetries,
	})
}

// pingRedis reports connectivity at startup but never blocks it: the configured
// failure strategy already defines what to do without Redis, and refusing to boot
// would turn a recoverable dependency outage into an outage of this service too.
func pingRedis(ctx context.Context, rdb *redis.Client, cfg *config.Config, log *slog.Logger) {
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Warn("redis unreachable at startup, the fallback strategy will apply",
			"addr", cfg.Redis.Addr,
			"strategy", cfg.Limiter.Fallback.Strategy,
			"error", err)
		return
	}
	log.Info("redis connected", "addr", cfg.Redis.Addr)
}

// newLocalLimiter builds the in-memory limiter used by the local_fallback strategy.
func newLocalLimiter(algo limiter.Algorithm, cfg limiter.Config) limiter.Limiter {
	if algo == limiter.TokenBucketAlgo {
		return limiter.NewTokenBucket(cfg)
	}
	return limiter.NewSlidingWindowCounter(cfg)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
