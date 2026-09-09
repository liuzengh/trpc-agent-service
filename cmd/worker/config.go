//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

// The configuration seam keeps environment parsing and validation out of the
// process composition root. Invalid required values fail before dependencies
// are opened; optional values use explicit, documented defaults.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("%s is required", key)
	}
	return value
}

func requireSecret(key string, minimumLength int) string {
	value := os.Getenv(key)
	if len(value) < minimumLength {
		log.Fatalf("%s must be configured with at least %d characters", key, minimumLength)
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func parseExecutionTimeout(value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return worker.DefaultExecutionTimeout, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse EXECUTION_TIMEOUT: %w", err)
	}
	if err := worker.ValidateExecutionTimeout(timeout); err != nil {
		return 0, err
	}
	return timeout, nil
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func parseRedisOptions(value string) (*redis.Options, error) {
	if strings.Contains(value, "://") {
		return redis.ParseURL(value)
	}
	if value == "" {
		return nil, errors.New("redis address is empty")
	}
	return &redis.Options{Addr: value}, nil
}
