// Package redistopology validates Redis invariants required by strong
// cross-component fencing.
package redistopology

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// VerifyPrimaryStandalone returns the server run_id only after confirming
// that the endpoint is a standalone primary.
func VerifyPrimaryStandalone(ctx context.Context, client *redis.Client) (string, error) {
	role, err := role(ctx, client)
	if err != nil {
		return "", fmt.Errorf("Redis ROLE check failed: %w", err)
	}
	if role != "master" {
		return "", fmt.Errorf("strong session fencing requires primary Redis, got %s", role)
	}
	if err := rejectCluster(ctx, client); err != nil {
		return "", err
	}
	runID, err := runID(ctx, client)
	if err != nil {
		return "", err
	}
	return runID, nil
}

func role(ctx context.Context, client *redis.Client) (string, error) {
	value, err := client.Do(ctx, "ROLE").Result()
	if err != nil {
		return "", err
	}
	items, ok := value.([]interface{})
	if !ok || len(items) == 0 {
		return "", errors.New("invalid ROLE response")
	}
	role, ok := items[0].(string)
	if !ok || strings.TrimSpace(role) == "" {
		return "", errors.New("invalid ROLE value")
	}
	return strings.ToLower(role), nil
}

func runID(ctx context.Context, client *redis.Client) (string, error) {
	value, err := client.Do(ctx, "INFO", "server").Text()
	if err != nil {
		return "", fmt.Errorf("Redis INFO server check failed: %w", err)
	}
	for _, line := range strings.Split(value, "\n") {
		if strings.HasPrefix(line, "run_id:") {
			id := strings.TrimSpace(strings.TrimPrefix(line, "run_id:"))
			if id != "" {
				return id, nil
			}
		}
	}
	return "", errors.New("Redis INFO server did not contain run_id")
}

func rejectCluster(ctx context.Context, client *redis.Client) error {
	_, err := client.Do(ctx, "CLUSTER", "INFO").Result()
	if err == nil {
		return errors.New("strong session fencing rejects Redis Cluster")
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "cluster support disabled") {
		return nil
	}
	return fmt.Errorf("Redis Cluster topology check failed: %w", err)
}
