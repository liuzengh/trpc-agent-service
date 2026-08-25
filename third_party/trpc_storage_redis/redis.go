// Package redis provides Redis instance registration for tRPC-Agent-Go.
// It is an API-compatible copy of storage/redis v0.0.3, kept locally only
// because that module declares Go 1.24.1 despite using Go 1.22-compatible APIs.
package redis

import (
	"fmt"

	client "github.com/redis/go-redis/v9"
)

var redisRegistry = make(map[string][]ClientBuilderOpt)

type clientBuilder func(...ClientBuilderOpt) (client.UniversalClient, error)

var globalBuilder clientBuilder = DefaultClientBuilder

func SetClientBuilder(builder clientBuilder) { globalBuilder = builder }
func GetClientBuilder() clientBuilder        { return globalBuilder }

func DefaultClientBuilder(builderOpts ...ClientBuilderOpt) (client.UniversalClient, error) {
	opts := &ClientBuilderOpts{}
	for _, opt := range builderOpts {
		opt(opts)
	}
	if opts.URL == "" {
		return nil, fmt.Errorf("redis: url is empty")
	}
	parsed, err := client.ParseURL(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("redis: parse url %s: %w", opts.URL, err)
	}
	universal := &client.UniversalOptions{
		Addrs: []string{parsed.Addr}, DB: parsed.DB, Username: parsed.Username,
		Password: parsed.Password, Protocol: parsed.Protocol, ClientName: parsed.ClientName,
		TLSConfig: parsed.TLSConfig, MaxRetries: parsed.MaxRetries,
		MinRetryBackoff: parsed.MinRetryBackoff, MaxRetryBackoff: parsed.MaxRetryBackoff,
		DialTimeout: parsed.DialTimeout, ReadTimeout: parsed.ReadTimeout,
		WriteTimeout: parsed.WriteTimeout, ContextTimeoutEnabled: parsed.ContextTimeoutEnabled,
		PoolFIFO: parsed.PoolFIFO, PoolSize: parsed.PoolSize, PoolTimeout: parsed.PoolTimeout,
		MinIdleConns: parsed.MinIdleConns, MaxIdleConns: parsed.MaxIdleConns,
		MaxActiveConns: parsed.MaxActiveConns, ConnMaxIdleTime: parsed.ConnMaxIdleTime,
		ConnMaxLifetime: parsed.ConnMaxLifetime,
	}
	return client.NewUniversalClient(universal), nil
}

type ClientBuilderOpt func(*ClientBuilderOpts)

type ClientBuilderOpts struct {
	URL          string
	ExtraOptions []interface{}
}

func WithClientBuilderURL(url string) ClientBuilderOpt {
	return func(opts *ClientBuilderOpts) { opts.URL = url }
}

func WithExtraOptions(extraOptions ...interface{}) ClientBuilderOpt {
	return func(opts *ClientBuilderOpts) {
		opts.ExtraOptions = append(opts.ExtraOptions, extraOptions...)
	}
}

func RegisterRedisInstance(name string, opts ...ClientBuilderOpt) {
	redisRegistry[name] = append(redisRegistry[name], opts...)
}

func GetRedisInstance(name string) ([]ClientBuilderOpt, bool) {
	opts, ok := redisRegistry[name]
	return opts, ok
}
