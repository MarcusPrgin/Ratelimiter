// Package config loads, defaults and validates the application configuration
// from a YAML file plus environment overrides.
//
// Validation is exhaustive and happens before anything is wired up. A rate
// limiter fails in a particularly quiet way: a misconfigured one still returns
// 200s, so nothing looks wrong until the thing it was protecting falls over. Every
// enum is therefore parsed rather than defaulted, and Validate reports all
// problems at once instead of only the first.
package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"

	"github.com/MarcusPrgin/Ratelimiter/internal/fallback"
	"github.com/MarcusPrgin/Ratelimiter/internal/limiter"
	"github.com/MarcusPrgin/Ratelimiter/internal/middleware"
	"github.com/MarcusPrgin/Ratelimiter/internal/penalty"
)

// EnvPrefix is the prefix for environment overrides, e.g.
// RATELIMITER_LIMITER_LIMIT=500.
const EnvPrefix = "RATELIMITER"

type Config struct {
	Server  ServerConfig  `mapstructure:"server"`
	Redis   RedisConfig   `mapstructure:"redis"`
	Limiter LimiterConfig `mapstructure:"limiter"`
	Routes  []RouteConfig `mapstructure:"routes"`
	Metrics MetricsConfig `mapstructure:"metrics"`
	Log     LogConfig     `mapstructure:"log"`
}

type ServerConfig struct {
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout"`
	// ShutdownTimeout bounds how long in-flight requests get to finish on SIGTERM.
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"pool_size"`
	// MinIdleConns keeps warm connections so a traffic spike does not pay TCP and
	// auth setup on the request path.
	MinIdleConns int           `mapstructure:"min_idle_conns"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	// MaxRetries is per-command retries inside the client. Keep it low: the
	// circuit breaker and fallback strategy handle sustained failure, and retrying
	// a limiter call multiplies latency during exactly the incident where added
	// latency is most damaging.
	MaxRetries int `mapstructure:"max_retries"`
}

type LimiterConfig struct {
	Algorithm string        `mapstructure:"algorithm"`
	Limit     int64         `mapstructure:"limit"`
	Window    time.Duration `mapstructure:"window"`
	BurstMax  int64         `mapstructure:"burst_max"`
	KeyType   string        `mapstructure:"key_type"`
	// TrustedProxyHops is how many reverse proxies sit in front of the service.
	// Zero ignores X-Forwarded-For; see middleware.Config.
	TrustedProxyHops int `mapstructure:"trusted_proxy_hops"`
	// MaxKeys bounds in-memory key cardinality per component.
	MaxKeys int `mapstructure:"max_keys"`

	Fallback FallbackConfig `mapstructure:"fallback"`
	Lease    LeaseConfig    `mapstructure:"lease"`
	Chain    ChainConfig    `mapstructure:"chain"`
	Adaptive AdaptiveConfig `mapstructure:"adaptive"`
	Penalty  PenaltyConfig  `mapstructure:"penalty"`
}

type FallbackConfig struct {
	Strategy         string        `mapstructure:"strategy"`
	BreakerThreshold int64         `mapstructure:"breaker_threshold"`
	BreakerCooldown  time.Duration `mapstructure:"breaker_cooldown"`
}

type LeaseConfig struct {
	Enabled       bool          `mapstructure:"enabled"`
	TTL           time.Duration `mapstructure:"ttl"`
	Prefetch      int64         `mapstructure:"prefetch"`
	NegativeCache bool          `mapstructure:"negative_cache"`
}

type ChainConfig struct {
	Enabled     bool  `mapstructure:"enabled"`
	TenantLimit int64 `mapstructure:"tenant_limit"`
	GlobalLimit int64 `mapstructure:"global_limit"`
}

type AdaptiveConfig struct {
	Enabled         bool          `mapstructure:"enabled"`
	LowWatermarkMs  float64       `mapstructure:"low_watermark_ms"`
	HighWatermarkMs float64       `mapstructure:"high_watermark_ms"`
	DecreaseRatio   float64       `mapstructure:"decrease_ratio"`
	IncreaseStep    float64       `mapstructure:"increase_step"`
	MinMultiplier   float64       `mapstructure:"min_multiplier"`
	EWMAAlpha       float64       `mapstructure:"ewma_alpha"`
	AdjustInterval  time.Duration `mapstructure:"adjust_interval"`
	ShedRetryAfter  time.Duration `mapstructure:"shed_retry_after"`
}

type PenaltyConfig struct {
	Enabled       bool          `mapstructure:"enabled"`
	Threshold     int64         `mapstructure:"threshold"`
	StrikeWindow  time.Duration `mapstructure:"strike_window"`
	BasePenalty   time.Duration `mapstructure:"base_penalty"`
	MaxPenalty    time.Duration `mapstructure:"max_penalty"`
	CheckInterval time.Duration `mapstructure:"check_interval"`
}

// RouteConfig overrides the quota cost for an exact request path.
//
// Per-route limit and window are deliberately absent. They would need a separate
// limiter instance and Redis keyspace per route; accepting the fields and ignoring
// them — as an earlier version did — is worse than not offering them.
type RouteConfig struct {
	Path string `mapstructure:"path"`
	// Cost is how many quota units this route consumes per request.
	Cost int64 `mapstructure:"cost"`
}

type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

type LogConfig struct {
	// Level is debug, info, warn or error.
	Level string `mapstructure:"level"`
	// Format is json or text.
	Format string `mapstructure:"format"`
}

// setDefaults seeds every key.
//
// Defaults for a component come from that component's own DefaultConfig rather than
// being restated here. Restating them means two sources of truth for the same value,
// and the copy that drifts is whichever one nobody is looking at.
func setDefaults(v *viper.Viper) {
	fbDefaults := fallback.DefaultConfig()
	adDefaults := limiter.DefaultAdaptiveConfig()
	penDefaults := penalty.DefaultConfig()

	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", "5s")
	v.SetDefault("server.write_timeout", "10s")
	v.SetDefault("server.idle_timeout", "60s")
	v.SetDefault("server.shutdown_timeout", "15s")

	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 20)
	v.SetDefault("redis.min_idle_conns", 5)
	v.SetDefault("redis.dial_timeout", "2s")
	v.SetDefault("redis.read_timeout", "500ms")
	v.SetDefault("redis.write_timeout", "500ms")
	v.SetDefault("redis.max_retries", 1)

	v.SetDefault("limiter.algorithm", string(limiter.SlidingWindowCounterAlgo))
	v.SetDefault("limiter.limit", 100)
	v.SetDefault("limiter.window", "1s")
	v.SetDefault("limiter.burst_max", 0)
	v.SetDefault("limiter.key_type", string(middleware.KeyByIP))
	v.SetDefault("limiter.trusted_proxy_hops", 0)
	v.SetDefault("limiter.max_keys", 1<<20)

	v.SetDefault("limiter.fallback.strategy", string(fbDefaults.Strategy))
	v.SetDefault("limiter.fallback.breaker_threshold", fbDefaults.BreakerThreshold)
	v.SetDefault("limiter.fallback.breaker_cooldown", fbDefaults.BreakerCooldown)

	v.SetDefault("limiter.lease.enabled", true)
	v.SetDefault("limiter.lease.ttl", "50ms")
	v.SetDefault("limiter.lease.prefetch", 4)
	v.SetDefault("limiter.lease.negative_cache", true)

	v.SetDefault("limiter.chain.enabled", false)
	v.SetDefault("limiter.chain.tenant_limit", 1000)
	v.SetDefault("limiter.chain.global_limit", 10000)

	v.SetDefault("limiter.adaptive.enabled", false)
	v.SetDefault("limiter.adaptive.low_watermark_ms", adDefaults.LowWatermarkMs)
	v.SetDefault("limiter.adaptive.high_watermark_ms", adDefaults.HighWatermarkMs)
	v.SetDefault("limiter.adaptive.decrease_ratio", adDefaults.DecreaseRatio)
	v.SetDefault("limiter.adaptive.increase_step", adDefaults.IncreaseStep)
	v.SetDefault("limiter.adaptive.min_multiplier", adDefaults.MinMultiplier)
	v.SetDefault("limiter.adaptive.ewma_alpha", adDefaults.EWMAAlpha)
	v.SetDefault("limiter.adaptive.adjust_interval", adDefaults.AdjustInterval)
	v.SetDefault("limiter.adaptive.shed_retry_after", adDefaults.ShedRetryAfter)

	v.SetDefault("limiter.penalty.enabled", false)
	v.SetDefault("limiter.penalty.threshold", penDefaults.Threshold)
	v.SetDefault("limiter.penalty.strike_window", penDefaults.StrikeWindow)
	v.SetDefault("limiter.penalty.base_penalty", penDefaults.BasePenalty)
	v.SetDefault("limiter.penalty.max_penalty", penDefaults.MaxPenalty)
	v.SetDefault("limiter.penalty.check_interval", penDefaults.CheckInterval)

	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
}

// Load reads the config file, applies environment overrides and validates.
func Load(path string) (*Config, error) {
	v := viper.New()
	setDefaults(v)

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("reading config file %q: %w", path, err)
		}
	}

	v.SetEnvPrefix(EnvPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	// ErrorUnused turns a typo into a startup failure. Silently ignoring an
	// unrecognised key means a setting someone believed they had enabled — a
	// fallback strategy, a penalty threshold — quietly not existing.
	if err := v.Unmarshal(&cfg, func(dc *mapstructure.DecoderConfig) {
		dc.ErrorUnused = true
	}); err != nil {
		return nil, fmt.Errorf("decoding config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate checks every field and reports all problems together.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	// ── server ──
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		add("server.port must be in 1-65535, got %d", c.Server.Port)
	}
	for name, d := range map[string]time.Duration{
		"server.read_timeout":     c.Server.ReadTimeout,
		"server.write_timeout":    c.Server.WriteTimeout,
		"server.idle_timeout":     c.Server.IdleTimeout,
		"server.shutdown_timeout": c.Server.ShutdownTimeout,
	} {
		if d <= 0 {
			add("%s must be > 0, got %s", name, d)
		}
	}

	// ── redis ──
	if c.Redis.Addr == "" {
		add("redis.addr must not be empty")
	}
	if c.Redis.DB < 0 {
		add("redis.db must be >= 0, got %d", c.Redis.DB)
	}
	if c.Redis.PoolSize < 1 {
		add("redis.pool_size must be >= 1, got %d", c.Redis.PoolSize)
	}
	if c.Redis.MinIdleConns < 0 || c.Redis.MinIdleConns > c.Redis.PoolSize {
		add("redis.min_idle_conns must be in 0-%d, got %d",
			c.Redis.PoolSize, c.Redis.MinIdleConns)
	}
	if c.Redis.MaxRetries < 0 {
		add("redis.max_retries must be >= 0, got %d", c.Redis.MaxRetries)
	}

	// ── limiter ──
	algo, err := limiter.ParseAlgorithm(c.Limiter.Algorithm)
	if err != nil {
		add("limiter.algorithm: %v", err)
	}
	if err := c.LimiterCore().Validate(); err != nil {
		add("limiter: %v", err)
	}
	if algo == limiter.TokenBucketAlgo && c.Limiter.BurstMax > 0 &&
		c.Limiter.BurstMax < c.Limiter.Limit {
		add("limiter.burst_max (%d) must be >= limiter.limit (%d) for the token "+
			"bucket, otherwise the bucket cannot hold one window of quota",
			c.Limiter.BurstMax, c.Limiter.Limit)
	}
	if _, err := middleware.ParseKeyMode(c.Limiter.KeyType); err != nil {
		add("limiter.key_type: %v", err)
	}
	if c.Limiter.TrustedProxyHops < 0 {
		add("limiter.trusted_proxy_hops must be >= 0, got %d", c.Limiter.TrustedProxyHops)
	}
	if c.Limiter.MaxKeys < 0 {
		add("limiter.max_keys must be >= 0, got %d", c.Limiter.MaxKeys)
	}

	// ── fallback ──
	if err := c.FallbackConfig().Validate(); err != nil {
		add("limiter.fallback: %v", err)
	}

	// ── lease ──
	if c.Limiter.Lease.Enabled {
		lc := c.LeaseConfig()
		if err := lc.Validate(); err != nil {
			add("limiter.lease: %v", err)
		} else if !lc.Enabled() {
			add("limiter.lease is enabled but ttl=%s prefetch=%d negative_cache=%t "+
				"does nothing; set prefetch > 0 or negative_cache: true",
				lc.TTL, lc.Prefetch, lc.NegativeCache)
		}
	}

	// ── chain ──
	if c.Limiter.Chain.Enabled {
		if c.Limiter.Chain.TenantLimit < 1 {
			add("limiter.chain.tenant_limit must be >= 1, got %d", c.Limiter.Chain.TenantLimit)
		}
		if c.Limiter.Chain.GlobalLimit < 1 {
			add("limiter.chain.global_limit must be >= 1, got %d", c.Limiter.Chain.GlobalLimit)
		}
		if c.Limiter.Chain.TenantLimit < c.Limiter.Limit {
			add("limiter.chain.tenant_limit (%d) is below limiter.limit (%d), so the "+
				"per-tenant tier would deny before a single caller reaches its own quota",
				c.Limiter.Chain.TenantLimit, c.Limiter.Limit)
		}
		if c.Limiter.Chain.GlobalLimit < c.Limiter.Chain.TenantLimit {
			add("limiter.chain.global_limit (%d) is below tenant_limit (%d), so the "+
				"tenant tier can never bind",
				c.Limiter.Chain.GlobalLimit, c.Limiter.Chain.TenantLimit)
		}
		if c.Limiter.KeyType != string(middleware.KeyByTenant) {
			add("limiter.chain.enabled requires limiter.key_type: %q, got %q — "+
				"without a tenant in the key every caller shares one tenant bucket",
				middleware.KeyByTenant, c.Limiter.KeyType)
		}
	}

	// ── adaptive ──
	if c.Limiter.Adaptive.Enabled {
		if err := c.AdaptiveConfig().Validate(); err != nil {
			add("limiter.adaptive: %v", err)
		}
	}

	// ── penalty ──
	if c.Limiter.Penalty.Enabled {
		if err := c.PenaltyConfig().Validate(); err != nil {
			add("limiter.penalty: %v", err)
		}
	}

	// ── routes ──
	seen := make(map[string]struct{}, len(c.Routes))
	for i, r := range c.Routes {
		switch {
		case r.Path == "":
			add("routes[%d].path must not be empty", i)
		case !strings.HasPrefix(r.Path, "/"):
			add("routes[%d].path %q must start with /", i, r.Path)
		}
		if _, dup := seen[r.Path]; dup {
			add("routes[%d]: duplicate path %q", i, r.Path)
		}
		seen[r.Path] = struct{}{}

		if r.Cost < 1 {
			add("routes[%d].cost must be >= 1, got %d", i, r.Cost)
			continue
		}
		// A route whose cost exceeds the capacity can never be served, so it would
		// return 400 for every request. Catch it at startup, not in production.
		//
		// With the chain enabled the request must fit *every* tier, so the binding
		// capacity is the smallest of them. Checking only the per-caller capacity
		// misses the token bucket case, where burst_max can exceed a tier's limit and
		// a cost between the two is admitted by the first tier and then permanently
		// rejected by a later one.
		capacity := c.LimiterCore().Capacity(algo)
		if c.Limiter.Chain.Enabled {
			capacity = min(capacity, c.Limiter.Chain.TenantLimit, c.Limiter.Chain.GlobalLimit)
		}
		if r.Cost > capacity {
			add("routes[%d] %q: cost %d exceeds the per-window capacity %d, so every "+
				"request to it would be rejected", i, r.Path, r.Cost, capacity)
		}
	}

	// ── metrics / log ──
	if c.Metrics.Enabled && !strings.HasPrefix(c.Metrics.Path, "/") {
		add("metrics.path %q must start with /", c.Metrics.Path)
	}
	if _, err := ParseLogLevel(c.Log.Level); err != nil {
		add("log.level: %v", err)
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		add("log.format must be json or text, got %q", c.Log.Format)
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid configuration:\n%w", errors.Join(errs...))
	}
	return nil
}

// ── Typed accessors ──────────────────────────────────────────────────────────
// These keep the mapping from config to component config in one place, so a
// renamed field cannot drift out of sync between Validate and main.

// LimiterCore returns the core limiter config. main builds the limiters from this
// rather than assembling its own copy, so what Validate checked is what runs.
func (c *Config) LimiterCore() limiter.Config {
	return limiter.Config{
		Limit:    c.Limiter.Limit,
		Window:   c.Limiter.Window,
		BurstMax: c.Limiter.BurstMax,
		MaxKeys:  c.Limiter.MaxKeys,
	}
}

// Algorithm returns the validated algorithm.
func (c *Config) Algorithm() limiter.Algorithm {
	algo, _ := limiter.ParseAlgorithm(c.Limiter.Algorithm)
	return algo
}

// KeyMode returns the validated key mode.
func (c *Config) KeyMode() middleware.KeyMode {
	mode, _ := middleware.ParseKeyMode(c.Limiter.KeyType)
	return mode
}

// FallbackConfig maps to the fallback package's config.
func (c *Config) FallbackConfig() fallback.Config {
	strategy, _ := fallback.ParseStrategy(c.Limiter.Fallback.Strategy)
	return fallback.Config{
		Strategy:         strategy,
		BreakerThreshold: c.Limiter.Fallback.BreakerThreshold,
		BreakerCooldown:  c.Limiter.Fallback.BreakerCooldown,
	}
}

// LeaseConfig maps to the limiter package's lease config.
func (c *Config) LeaseConfig() limiter.LeaseConfig {
	return limiter.LeaseConfig{
		TTL:           c.Limiter.Lease.TTL,
		Prefetch:      c.Limiter.Lease.Prefetch,
		NegativeCache: c.Limiter.Lease.NegativeCache,
		MaxKeys:       c.Limiter.MaxKeys,
	}
}

// AdaptiveConfig maps to the limiter package's adaptive config.
func (c *Config) AdaptiveConfig() limiter.AdaptiveConfig {
	return limiter.AdaptiveConfig{
		LowWatermarkMs:  c.Limiter.Adaptive.LowWatermarkMs,
		HighWatermarkMs: c.Limiter.Adaptive.HighWatermarkMs,
		DecreaseRatio:   c.Limiter.Adaptive.DecreaseRatio,
		IncreaseStep:    c.Limiter.Adaptive.IncreaseStep,
		MinMultiplier:   c.Limiter.Adaptive.MinMultiplier,
		EWMAAlpha:       c.Limiter.Adaptive.EWMAAlpha,
		AdjustInterval:  c.Limiter.Adaptive.AdjustInterval,
		ShedRetryAfter:  c.Limiter.Adaptive.ShedRetryAfter,
	}
}

// PenaltyConfig maps to the penalty package's config.
func (c *Config) PenaltyConfig() penalty.Config {
	return penalty.Config{
		Threshold:     c.Limiter.Penalty.Threshold,
		StrikeWindow:  c.Limiter.Penalty.StrikeWindow,
		BasePenalty:   c.Limiter.Penalty.BasePenalty,
		MaxPenalty:    c.Limiter.Penalty.MaxPenalty,
		CheckInterval: c.Limiter.Penalty.CheckInterval,
		MaxKeys:       c.Limiter.MaxKeys,
	}
}

// CostMap builds an exact-path → cost lookup.
func (c *Config) CostMap() map[string]int64 {
	m := make(map[string]int64, len(c.Routes))
	for _, r := range c.Routes {
		if r.Cost > 1 {
			m[r.Path] = r.Cost
		}
	}
	return m
}
