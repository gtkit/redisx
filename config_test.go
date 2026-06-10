package redisx

import (
	"crypto/tls"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	if cfg.DefaultDB != 0 {
		t.Errorf("DefaultDB = %d, want 0", cfg.DefaultDB)
	}
	if cfg.PoolSize != 10 {
		t.Errorf("PoolSize = %d, want 10", cfg.PoolSize)
	}
	if cfg.MinIdleConns != 3 {
		t.Errorf("MinIdleConns = %d, want 3", cfg.MinIdleConns)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", cfg.MaxRetries)
	}
	if cfg.DialTimeout != 5*time.Second {
		t.Errorf("DialTimeout = %v, want 5s", cfg.DialTimeout)
	}
	if cfg.ReadTimeout != 3*time.Second {
		t.Errorf("ReadTimeout = %v, want 3s", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 3*time.Second {
		t.Errorf("WriteTimeout = %v, want 3s", cfg.WriteTimeout)
	}
	if cfg.IdleTimeout != 5*time.Minute {
		t.Errorf("IdleTimeout = %v, want 5m", cfg.IdleTimeout)
	}
}

func TestOptionsApply(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	tlsCfg := &tls.Config{ServerName: "redis.example.com", MinVersion: tls.VersionTLS12}
	opts := []Option{
		WithAddr("10.0.0.1:6380"),
		WithUsername("admin"),
		WithPassword("secret"),
		WithDB(2),
		WithInitDBs(1, 3),
		WithDBConfig(4, "session"),
		WithPoolSize(20),
		WithMinIdleConns(5),
		WithMaxRetries(0),
		WithDialTimeout(time.Second),
		WithReadTimeout(2 * time.Second),
		WithWriteTimeout(4 * time.Second),
		WithIdleTimeout(time.Minute),
		WithKeyPrefix("app"),
		WithTLSConfig(tlsCfg),
	}
	for _, o := range opts {
		o(cfg)
	}

	if cfg.Addr != "10.0.0.1:6380" {
		t.Errorf("Addr = %q", cfg.Addr)
	}
	if cfg.Username != "admin" {
		t.Errorf("Username = %q", cfg.Username)
	}
	if cfg.Password != "secret" {
		t.Errorf("Password = %q", cfg.Password)
	}
	if cfg.DefaultDB != 2 {
		t.Errorf("DefaultDB = %d", cfg.DefaultDB)
	}
	wantDBs := []DBConfig{{DB: 1}, {DB: 3}, {DB: 4, Prefix: "session"}}
	if len(cfg.InitDBs) != len(wantDBs) {
		t.Fatalf("InitDBs = %v, want %v", cfg.InitDBs, wantDBs)
	}
	for i, dc := range wantDBs {
		if cfg.InitDBs[i] != dc {
			t.Errorf("InitDBs[%d] = %v, want %v", i, cfg.InitDBs[i], dc)
		}
	}
	if cfg.PoolSize != 20 {
		t.Errorf("PoolSize = %d", cfg.PoolSize)
	}
	if cfg.MinIdleConns != 5 {
		t.Errorf("MinIdleConns = %d", cfg.MinIdleConns)
	}
	if cfg.MaxRetries != 0 {
		t.Errorf("MaxRetries = %d", cfg.MaxRetries)
	}
	if cfg.DialTimeout != time.Second {
		t.Errorf("DialTimeout = %v", cfg.DialTimeout)
	}
	if cfg.ReadTimeout != 2*time.Second {
		t.Errorf("ReadTimeout = %v", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 4*time.Second {
		t.Errorf("WriteTimeout = %v", cfg.WriteTimeout)
	}
	if cfg.IdleTimeout != time.Minute {
		t.Errorf("IdleTimeout = %v", cfg.IdleTimeout)
	}
	if cfg.KeyPrefix != "app" {
		t.Errorf("KeyPrefix = %q", cfg.KeyPrefix)
	}
	if cfg.TLSConfig != tlsCfg {
		t.Errorf("TLSConfig = %v, want %v", cfg.TLSConfig, tlsCfg)
	}
}
