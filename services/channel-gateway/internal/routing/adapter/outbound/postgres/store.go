// Package postgresadapter persists routing receipts and projections atomically.
package postgresadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

var _ application.Store = (*Store)(nil)

// Apply rejects unsequenced writes; live routing always uses ApplyFromStream.
func (store *Store) Apply(context.Context, domain.RouteEvent) error {
	return domain.ErrStreamPositionRequired
}

func applyEvent(ctx context.Context, tx pgx.Tx, event domain.RouteEvent) error {
	if err := lockAccount(ctx, tx, event.Route.Provider, event.Route.AccountID, false); err != nil {
		return err
	}

	// Unique insert also serializes an EventID accidentally reused across accounts.
	// ON CONFLICT waits for the other transaction, then compares its committed digest.
	receiptDigest := event.ReceiptDigest()
	inserted, err := tx.Exec(ctx, `INSERT INTO gateway_route_receipts(event_id, digest) VALUES ($1,$2) ON CONFLICT (event_id) DO NOTHING`, event.EventID, receiptDigest)
	if err != nil {
		return fmt.Errorf("insert route receipt: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		var previous string
		if err = tx.QueryRow(ctx, `SELECT digest FROM gateway_route_receipts WHERE event_id=$1`, event.EventID).Scan(&previous); err != nil {
			return fmt.Errorf("read route receipt: %w", err)
		}
		if previous != receiptDigest {
			return fmt.Errorf("%w: event identity reused", domain.ErrGenerationConflict)
		}
		return nil
	}

	var generation int64
	var digest string
	err = tx.QueryRow(ctx, `SELECT generation,digest FROM gateway_route_projections WHERE provider=$1 AND account_id=$2 FOR UPDATE`, event.Route.Provider, event.Route.AccountID).Scan(&generation, &digest)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock route projection: %w", err)
	}
	change, err := domain.Compare(generation, digest, event)
	if err != nil {
		return err
	}
	if change == domain.Replace {
		snapshot, marshalErr := json.Marshal(event.Route)
		if marshalErr != nil {
			return fmt.Errorf("encode route snapshot: %w", marshalErr)
		}
		_, err = tx.Exec(ctx, `INSERT INTO gateway_route_projections(provider,account_id,generation,enabled,snapshot,digest)
   VALUES ($1,$2,$3,$4,$5,$6)
   ON CONFLICT (provider,account_id) DO UPDATE SET generation=excluded.generation,enabled=excluded.enabled,snapshot=excluded.snapshot,digest=excluded.digest,updated_at=now()`, event.Route.Provider, event.Route.AccountID, event.Route.Generation, event.Enabled, snapshot, event.ProjectionDigest())
		if err != nil {
			return fmt.Errorf("replace route projection: %w", err)
		}
	}
	return nil
}

func (store *Store) Resolve(ctx context.Context, provider, accountID string) (domain.RouteSnapshot, error) {
	if err := domain.ValidateAccount(provider, accountID); err != nil {
		return domain.RouteSnapshot{}, err
	}
	health, err := store.QueryProjectionHealth(ctx)
	if err != nil {
		return domain.RouteSnapshot{}, err
	}
	if err = health.RequireInitialized(); err != nil {
		return domain.RouteSnapshot{}, err
	}
	var generation int64
	var enabled bool
	var snapshot []byte
	var digest string
	err = store.pool.QueryRow(ctx, `SELECT generation,enabled,snapshot,digest FROM gateway_route_projections WHERE provider=$1 AND account_id=$2`, provider, accountID).Scan(&generation, &enabled, &snapshot, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.RouteSnapshot{}, domain.ErrUnavailable
	}
	if err != nil {
		return domain.RouteSnapshot{}, err
	}
	route, err := decodeProjection(provider, accountID, generation, enabled, snapshot, digest)
	if err != nil {
		if qerr := store.Quarantine(ctx, domain.StreamPosition{StreamName: health.StreamName, StreamID: health.StreamID}, domain.QuarantineCorruptProjection, ""); qerr != nil {
			return domain.RouteSnapshot{}, qerr
		}
		return domain.RouteSnapshot{}, domain.ErrProjectionBlocked
	}
	if !enabled {
		return domain.RouteSnapshot{}, domain.ErrUnavailable
	}
	return route, nil
}

func decodeProjection(provider, accountID string, generation int64, enabled bool, snapshot []byte, digest string) (domain.RouteSnapshot, error) {
	var route domain.RouteSnapshot
	decoder := json.NewDecoder(bytes.NewReader(snapshot))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&route); err != nil {
		return route, domain.ErrProjectionBlocked
	}
	if err := route.Validate(enabled); err != nil || route.Provider != provider || route.AccountID != accountID || route.Generation != generation {
		return domain.RouteSnapshot{}, domain.ErrProjectionBlocked
	}
	event := domain.RouteEvent{Route: route, Enabled: enabled}
	if event.ProjectionDigest() != digest {
		return domain.RouteSnapshot{}, domain.ErrProjectionBlocked
	}
	return route, nil
}

// VerifyGeneration participates in the admission transaction. Shared advisory
// and row locks are held until that transaction commits or rolls back, so Apply
// cannot disable/switch the target between this check and durable acceptance.
// The advisory lock also protects the absent-row case; it is not an owner lease.
func (store *Store) VerifyGeneration(ctx context.Context, tx pgx.Tx, provider, accountID string, generation int64) error {
	if err := domain.ValidateAccount(provider, accountID); err != nil {
		return err
	}
	if generation < 1 || generation > domain.MaxGeneration {
		return domain.ErrInvalidEvent
	}
	if tx == nil {
		return errors.New("routing generation verifier requires transaction")
	}
	// A shared replay-state row lock serializes with initialization and
	// checkpoint changes, before taking the shared per-account locks.
	var singleton bool
	if err := tx.QueryRow(ctx, `SELECT singleton FROM gateway_route_replay_state WHERE singleton FOR SHARE`).Scan(&singleton); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotInitialized
		}
		return err
	}
	health, err := readHealth(ctx, tx, false)
	if err != nil {
		return err
	}
	if err = health.RequireInitialized(); err != nil {
		return err
	}
	if err := lockAccount(ctx, tx, provider, accountID, true); err != nil {
		return err
	}
	var current int64
	var enabled bool
	var snapshot []byte
	var digest string
	err = tx.QueryRow(ctx, `SELECT generation,enabled,snapshot,digest FROM gateway_route_projections WHERE provider=$1 AND account_id=$2 FOR SHARE`, provider, accountID).Scan(&current, &enabled, &snapshot, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrUnavailable
	}
	if err != nil {
		return fmt.Errorf("verify route generation: %w", err)
	}
	if _, err := decodeProjection(provider, accountID, current, enabled, snapshot, digest); err != nil {
		return domain.ErrProjectionBlocked
	}
	if !enabled {
		return domain.ErrUnavailable
	}
	if current != generation {
		return domain.ErrGenerationConflict
	}
	return nil
}

func lockAccount(ctx context.Context, tx pgx.Tx, provider, accountID string, shared bool) error {
	// Length-delimited JSON avoids ambiguous key concatenation. Hash collisions
	// only serialize unrelated accounts; they never weaken mutual exclusion.
	key, err := json.Marshal([]string{"gateway.routing.v1", provider, accountID})
	if err != nil {
		return err
	}
	statement := `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`
	if shared {
		statement = `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`
	}
	if _, err = tx.Exec(ctx, statement, string(key)); err != nil {
		return fmt.Errorf("lock route account: %w", err)
	}
	return nil
}
