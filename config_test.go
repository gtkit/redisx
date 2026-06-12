package redisx

import (
	"crypto/tls"
	"strings"
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
	if cfg.AllowPartialInit {
		t.Error("AllowPartialInit 默认应为 false（全有或全无）")
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // 空表示期望通过
	}{
		{name: "默认配置合法", mutate: func(*Config) {}},
		{name: "MaxRetries为0合法", mutate: func(c *Config) { c.MaxRetries = 0 }},
		{name: "MinIdleConns为0合法", mutate: func(c *Config) { c.MinIdleConns = 0 }},
		{name: "MinIdleConns等于PoolSize合法", mutate: func(c *Config) { c.MinIdleConns = c.PoolSize }},
		{name: "PoolSize为0", mutate: func(c *Config) { c.PoolSize = 0 }, wantErr: "pool size must be > 0"},
		{name: "PoolSize为负", mutate: func(c *Config) { c.PoolSize = -1 }, wantErr: "pool size must be > 0"},
		{name: "MinIdleConns为负", mutate: func(c *Config) { c.MinIdleConns = -1 }, wantErr: "min idle conns must be >= 0"},
		{
			name:    "MinIdleConns超过PoolSize",
			mutate:  func(c *Config) { c.PoolSize, c.MinIdleConns = 5, 6 },
			wantErr: "must not exceed pool size",
		},
		{name: "MaxRetries为负", mutate: func(c *Config) { c.MaxRetries = -1 }, wantErr: "max retries must be >= 0"},
		{name: "全局前缀含星号", mutate: func(c *Config) { c.KeyPrefix = "app*" }, wantErr: "must not contain glob characters"},
		{name: "全局前缀含反斜杠", mutate: func(c *Config) { c.KeyPrefix = `a\b` }, wantErr: "must not contain glob characters"},
		{
			name:    "perDB前缀含方括号",
			mutate:  func(c *Config) { c.InitDBs = []DBConfig{{DB: 1, Prefix: "tenant[3]"}} },
			wantErr: "db=1 prefix",
		},
		{name: "前缀含冒号合法", mutate: func(c *Config) { c.KeyPrefix = "app:svc" }},
		{name: "DialTimeout为0", mutate: func(c *Config) { c.DialTimeout = 0 }, wantErr: "dial timeout must be > 0"},
		{name: "ReadTimeout为负", mutate: func(c *Config) { c.ReadTimeout = -time.Second }, wantErr: "read timeout must be > 0"},
		{name: "WriteTimeout为0", mutate: func(c *Config) { c.WriteTimeout = 0 }, wantErr: "write timeout must be > 0"},
		{name: "IdleTimeout为负", mutate: func(c *Config) { c.IdleTimeout = -time.Minute }, wantErr: "idle timeout must be > 0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaultConfig()
			tt.mutate(cfg)
			err := cfg.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() = %v, 期望通过", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("validate() = %v, 期望包含 %q", err, tt.wantErr)
			}
		})
	}
}

// TestNewClientRejectsInvalidConfig 验证非法配置在拨号前即被 NewClient 拒绝
// （地址指向无监听端口，若发生拨号会得到 ping 错误而非校验错误）。
func TestNewClientRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	c, err := NewClient(WithAddr("127.0.0.1:1"), WithPoolSize(-1))
	if err == nil {
		_ = c.Close()
		t.Fatal("NewClient() 期望返回校验错误，实际为 nil")
	}
	if !strings.Contains(err.Error(), "pool size must be > 0") {
		t.Errorf("NewClient() 错误 = %q, 期望为配置校验错误", err.Error())
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
		WithAllowPartialInit(),
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
	if !cfg.AllowPartialInit {
		t.Error("AllowPartialInit = false, want true")
	}
}
