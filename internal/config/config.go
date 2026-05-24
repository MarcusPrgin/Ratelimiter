// Package config loads and validates the application configuration.
// Supports YAML file + environment variable overrides.
// Per-route rules take precedence over global defaults.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config is the root configuration structure.
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
	Algorithm      string        `mapstructure:"algorithm"`      // sliding_window_counter | token_bucket | fixed_window
	Limit          int64         `mapstructure:"limit"`          // requests per window
	Window         time.Duration `mapstructure:"window"`         // window size
	BurstMax       int64         `mapstructure:"burst_max"`      // token bucket burst
	CacheTTL       time.Duration `mapstructure:"cache_ttl"`      // local cache TTL (0 = no cache)
	FallbackStrategy string      `mapstructure:"fallback_strategy"` // fail_open | fail_closed | local_fallback
	KeyType        string        `mapstructure:"key_type"`       // ip | user | tenant
}

// RouteConfig lets you set per-route limits that override global defaults.
type RouteConfig struct {
	Path   string        `mapstructure:"path"`
	Limit  int64         `mapstructure:"limit"`
	Window time.Duration `mapstructure:"window"`
}

type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

// Load reads configuration from file and environment.
func Load(path string) (*Config, error) {
	v := viper.New()

	// defaults
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", "5s")
	v.SetDefault("server.write_timeout", "10s")
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.pool_size", 20)
	v.SetDefault("redis.dial_timeout", "2s")
	v.SetDefault("redis.read_timeout", "1s")
	v.SetDefault("redis.write_timeout", "1s")
	v.SetDefault("limiter.algorithm", "sliding_window_counter")
	v.SetDefault("limiter.limit", 100)
	v.SetDefault("limiter.window", "1s")
	v.SetDefault("limiter.cache_ttl", "5ms")
	v.SetDefault("limiter.fallback_strategy", "fail_open")
	v.SetDefault("limiter.key_type", "ip")
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")

	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("reading config file %q: %w", path, err)
		}
	}

	// environment overrides: RATELIMITER_REDIS_ADDR, RATELIMITER_LIMITER_LIMIT, etc.
	v.SetEnvPrefix("RATELIMITER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	return &cfg, nil
}
