package redisx

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Codec defines caller-provided value encoding for SetCodec and GetCodec.
//
// It is intentionally small so protobuf, gob, msgpack, encrypted payloads, or
// application-specific codecs can be adapted without redisx depending on them.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// SetBytes writes data to key with automatic key prefixing.
//
// expiration follows Redis SET semantics: 0 means no expiration.
func SetBytes(ctx context.Context, p *Proxy, key string, data []byte, expiration time.Duration) error {
	if p == nil {
		return errors.New("redisx: bytes proxy is nil")
	}
	if err := p.Set(ctx, key, data, expiration).Err(); err != nil {
		return fmt.Errorf("redisx: set bytes key=%q: %w", p.key(key), err)
	}
	return nil
}

// GetBytes reads raw bytes from key with automatic key prefixing.
//
// Missing keys preserve redis.Nil through error wrapping.
func GetBytes(ctx context.Context, p *Proxy, key string) ([]byte, error) {
	if p == nil {
		return nil, errors.New("redisx: bytes proxy is nil")
	}
	data, err := p.Get(ctx, key).Bytes()
	if err != nil {
		return nil, fmt.Errorf("redisx: get bytes key=%q: %w", p.key(key), err)
	}
	return data, nil
}

// SetCodec marshals value with codec and writes the resulting bytes to key.
//
// The codec is caller-provided; redisx does not depend on protobuf, gob,
// msgpack, or other encoding packages.
func SetCodec(ctx context.Context, p *Proxy, key string, codec Codec, value any, expiration time.Duration) error {
	if p == nil {
		return errors.New("redisx: codec proxy is nil")
	}
	if codec == nil {
		return fmt.Errorf("redisx: codec is nil key=%q", p.key(key))
	}
	data, err := codec.Marshal(value)
	if err != nil {
		return fmt.Errorf("redisx: marshal codec key=%q: %w", p.key(key), err)
	}
	if err := p.Set(ctx, key, data, expiration).Err(); err != nil {
		return fmt.Errorf("redisx: set codec key=%q: %w", p.key(key), err)
	}
	return nil
}

// GetCodec reads key and unmarshals the stored bytes into T with codec.
//
// Missing keys preserve redis.Nil through error wrapping.
func GetCodec[T any](ctx context.Context, p *Proxy, key string, codec Codec) (T, error) {
	var v T
	if p == nil {
		return v, errors.New("redisx: codec proxy is nil")
	}
	if codec == nil {
		return v, fmt.Errorf("redisx: codec is nil key=%q", p.key(key))
	}
	data, err := p.Get(ctx, key).Bytes()
	if err != nil {
		return v, fmt.Errorf("redisx: get codec key=%q: %w", p.key(key), err)
	}
	if err := codec.Unmarshal(data, &v); err != nil {
		return v, fmt.Errorf("redisx: unmarshal codec key=%q: %w", p.key(key), err)
	}
	return v, nil
}
