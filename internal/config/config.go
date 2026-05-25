// Package config loads and validates the application configuration.
// Supports YAML file + environment variable overrides.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Redis    RedisConfig    `mapstructure:"redis"`
	Limiter  LimiterConfig  `mapstructure:"limiter"`
	Routes   []RouteConfig  `mapstructure:"routes"`
	Metrics  MetricsConfig  `mapstructure:"metrics"`
}

type ServerConfig struct {
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

type RedisConfig struct {
	Addr         string        `mapstructure:"addr"`
	Password     string        `mapstructure:"password"`
	DB           int           `mapstructure:"db"`
	PoolSize     int           `mapstructure:"pool_size"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

type LimiterConfig struct {
	Algorithm        string        `mapstructure:"algorithm"`
	Limit            int64         `mapstructure:"limit"`
	Window           time.Duration `mapstructure:"window"`
	BurstMax         int64         `mapstructure:"burst_max"`
	CacheTTL         time.Duration `mapstructure:"cache_ttl"`
	FallbackStrategy string        `mapstructure:"fallback_strategy"`
	KeyType          string        `mapstructure:"key_type"`

	// Chain enables hierarchical multi-tier enforcement.
	Chain ChainConfig `mapstructure:"chain"`

	// Adaptive enables AIMD load shedding based on observed latency.
	Adaptive AdaptiveConfig `mapstructure:"adaptive"`

	// Penalty enables exponential-backoff penalty box for abusive keys.
	Penalty PenaltyConfig `mapstructure:"penalty"`
}

// ChainConfig adds global and per-tenant tiers on top of the per-key limit.
type ChainConfig struct {
	Enabled     bool  `mapstructure:"enabled"`
	GlobalLimit int64 `mapstructure:"global_limit"`
	TenantLimit int64 `mapstructure:"tenant_limit"`
}

// AdaptiveConfig controls the AIMD control loop parameters.
type AdaptiveConfig struct {
	Enabled         bool    `mapstructure:"enabled"`
	LowWatermarkMs  float64 `mapstructure:"low_watermark_ms"`
	HighWatermarkMs float64 `mapstructure:"high_watermark_ms"`
	DecreaseRatio   float64 `mapstructure:"decrease_ratio"`
	IncreaseStep    float64 `mapstructure:"increase_step"`
	MinMultiplier   float64 `mapstructure:"min_multiplier"`
	EWMAAlpha       float64 `mapstructure:"ewma_alpha"`
}

// PenaltyConfig controls the penalty box behaviour.
type PenaltyConfig struct {
	Enabled      bool          `mapstructure:"enabled"`
	Threshold    int64         `mapstructure:"threshold"`
	StrikeWindow time.Duration `mapstructure:"strike_window"`
	BasePenalty  time.Duration `mapstructure:"base_penalty"`
	MaxPenalty   time.Duration `mapstructure:"max_penalty"`
}

// RouteConfig lets you override limits and cost per path prefix.
type RouteConfig struct {
	Path   string        `mapstructure:"path"`
	Limit  int64         `mapstructure:"limit"`
	Window time.Duration `mapstructure:"window"`
	// Cost is the number of quota units this route consumes per request (default 1).
	Cost int64 `mapstructure:"cost"`
}

type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

func Load(path string) (*Config, error) {
	v := viper.New()

	// server defaults
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", "5s")
	v.SetDefault("server.write_timeout", "10s")

	// redis defaults
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.pool_size", 20)
	v.SetDefault("redis.dial_timeout", "2s")
	v.SetDefault("redis.read_timeout", "1s")
	v.SetDefault("redis.write_timeout", "1s")

	// limiter defaults
	v.SetDefault("limiter.algorithm", "sliding_window_counter")
	v.SetDefault("limiter.limit", 100)
	v.SetDefault("limiter.window", "1s")
	v.SetDefault("limiter.cache_ttl", "5ms")
	v.SetDefault("limiter.fallback_strategy", "fail_open")
	v.SetDefault("limiter.key_type", "ip")

	// chain defaults
	v.SetDefault("limiter.chain.enabled", false)
	v.SetDefault("limiter.chain.global_limit", 10000)
	v.SetDefault("limiter.chain.tenant_limit", 1000)

	// adaptive defaults
	v.SetDefault("limiter.adaptive.enabled", false)
	v.SetDefault("limiter.adaptive.low_watermark_ms", 2.0)
	v.SetDefault("limiter.adaptive.high_watermark_ms", 10.0)
	v.SetDefault("limiter.adaptive.decrease_ratio", 0.75)
	v.SetDefault("limiter.adaptive.increase_step", 0.05)
	v.SetDefault("limiter.adaptive.min_multiplier", 0.1)
	v.SetDefault("limiter.adaptive.ewma_alpha", 0.1)

	// penalty defaults
	v.SetDefault("limiter.penalty.enabled", false)
	v.SetDefault("limiter.penalty.threshold", 10)
	v.SetDefault("limiter.penalty.strike_window", "60s")
	v.SetDefault("limiter.penalty.base_penalty", "30s")
	v.SetDefault("limiter.penalty.max_penalty", "3600s")

	// metrics defaults
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("reading config file %q: %w", path, err)
		}
	}

	v.SetEnvPrefix("RATELIMITER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}
	return &cfg, nil
}
