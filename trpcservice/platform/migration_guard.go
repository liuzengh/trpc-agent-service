package platform

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

var ErrTenantMigrating = errors.New("tenant_storage_migrating")

type migrationImportKey struct{}

func importingMigration(ctx context.Context) bool {
	value, _ := ctx.Value(migrationImportKey{}).(bool)
	return value
}
func (s *RedisStore) migrationKey(tenant string) string { return s.prefix + "migration:" + tenant }

// A durable source freeze survives a crashed migration process. Recovery requires
// an explicit operator unlock after the destination has been inspected.
func (s *RedisStore) freezeMigration(ctx context.Context, tenant string) (string, error) {
	token := newTraceID()
	ok, err := s.client.SetNX(ctx, s.migrationKey(tenant), token, 0).Result()
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrTenantMigrating
	}
	return token, nil
}
func (s *RedisStore) unfreezeMigration(ctx context.Context, tenant, token string) error {
	return s.client.Eval(ctx, `if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0`, []string{s.migrationKey(tenant)}, token).Err()
}
func (s *RedisStore) checkMigration(ctx context.Context, tx *redis.Tx, tenant string) error {
	count, err := tx.Exists(ctx, s.migrationKey(tenant)).Result()
	if err != nil {
		return err
	}
	if count != 0 {
		return ErrTenantMigrating
	}
	return nil
}

// ResumeTenantWrites explicitly ends a crash freeze or a retired-source read-only
// window. Operators must inspect the migration result and routing before calling.
func (s *RedisStore) ResumeTenantWrites(ctx context.Context, tenant string) error {
	if tenant == "" {
		return errors.New("tenant required")
	}
	return s.client.Del(ctx, s.migrationKey(tenant)).Err()
}
