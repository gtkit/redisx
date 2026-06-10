package redisx

import (
	"strings"
	"testing"
)

func TestPrefixKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{name: "空前缀", prefix: "", key: "user:1", want: "user:1"},
		{name: "普通前缀", prefix: "app", key: "user:1", want: "app:user:1"},
		{name: "多级前缀", prefix: "app:demo", key: "user:1", want: "app:demo:user:1"},
		{name: "空key", prefix: "app", key: "", want: "app:"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := prefixKey(tt.prefix, tt.key); got != tt.want {
				t.Errorf("prefixKey(%q, %q) = %q, want %q", tt.prefix, tt.key, got, tt.want)
			}
		})
	}
}

func TestNewClientValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []Option
		wantErr string
	}{
		{
			name:    "缺少Addr",
			opts:    nil,
			wantErr: "addr is required",
		},
		{
			name:    "默认DB为负数",
			opts:    []Option{WithAddr("127.0.0.1:6379"), WithDB(-1)},
			wantErr: "invalid default db -1",
		},
		{
			name:    "InitDBs含负数",
			opts:    []Option{WithAddr("127.0.0.1:6379"), WithInitDBs(0, -2)},
			wantErr: "invalid db -2 in init list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewClient(tt.opts...)
			if err == nil {
				_ = c.Close()
				t.Fatalf("NewClient() 期望返回错误，实际为 nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("NewClient() 错误 = %q, 期望包含 %q", err.Error(), tt.wantErr)
			}
		})
	}
}
