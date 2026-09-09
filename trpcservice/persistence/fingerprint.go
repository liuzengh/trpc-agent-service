package persistence

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// FingerprintForProfile parses a credential only to derive a stable backend
// identity. Usernames, passwords, TLS parameters, and pool settings are never
// copied into the returned value or parser errors.
func FingerprintForProfile(profile tenant.StorageProfile, credential string) (BackendFingerprint, error) {
	profile, err := tenant.NormalizeStorageProfile(profile)
	if err != nil {
		return BackendFingerprint{}, err
	}
	fingerprint := BackendFingerprint{SchemaVersion: FingerprintSchemaVersion, Kind: profile.Kind, StorageProfileID: profile.ID}
	switch profile.Kind {
	case tenant.StorageKindInMemory:
		fingerprint.Namespace = "process"
	case tenant.StorageKindRedis:
		opts, parseErr := redis.ParseURL(strings.TrimSpace(credential))
		if parseErr != nil {
			return BackendFingerprint{}, fmt.Errorf("invalid Redis storage credential")
		}
		fingerprint.DatabaseIdentity = opts.Addr + "/" + strconv.Itoa(opts.DB)
		fingerprint.Namespace = strings.TrimRight(profile.KeyPrefix, ":")
	case tenant.StorageKindPostgres:
		cfg, parseErr := pgx.ParseConfig(strings.TrimSpace(credential))
		if parseErr != nil || strings.TrimSpace(cfg.Host) == "" || strings.TrimSpace(cfg.Database) == "" {
			return BackendFingerprint{}, fmt.Errorf("invalid PostgreSQL storage credential")
		}
		fingerprint.DatabaseIdentity = net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))) + "/" + cfg.Database
		fingerprint.Namespace = profile.Schema + "." + profile.TablePrefix
	case tenant.StorageKindMySQL:
		dsn := strings.TrimSpace(credential)
		cfg, parseErr := mysqldriver.ParseDSN(dsn)
		var values url.Values
		queryStart := strings.LastIndexByte(dsn, '?')
		if queryStart >= 0 {
			values, err = url.ParseQuery(dsn[queryStart+1:])
		}
		if parseErr != nil || queryStart < 0 || err != nil || strings.TrimSpace(cfg.Addr) == "" || strings.TrimSpace(cfg.DBName) == "" || !cfg.ParseTime || cfg.Loc == nil || cfg.Loc.String() != "UTC" || values.Get("loc") != "UTC" || !strings.EqualFold(values.Get("charset"), "utf8mb4") {
			return BackendFingerprint{}, fmt.Errorf("invalid MySQL storage credential")
		}
		fingerprint.DatabaseIdentity = cfg.Net + "(" + cfg.Addr + ")/" + cfg.DBName
		fingerprint.Namespace = profile.TablePrefix
	default:
		return BackendFingerprint{}, fmt.Errorf("unsupported storage backend")
	}
	if err := fingerprint.Validate(); err != nil {
		return BackendFingerprint{}, err
	}
	return fingerprint, nil
}
