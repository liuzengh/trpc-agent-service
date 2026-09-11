package node

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type PostgresStore struct {
	database *sql.DB
	now      func() time.Time
}

func NewPostgresStore(database *sql.DB) (*PostgresStore, error) {
	if database == nil {
		return nil, errors.New("node database is required")
	}
	return &PostgresStore{database: database, now: time.Now}, nil
}

func (s *PostgresStore) Register(ctx context.Context, record Record) error {
	if err := record.validate(); err != nil {
		return err
	}
	_, err := s.database.ExecContext(ctx, `
INSERT INTO service_nodes(node_id,boot_id,role,state,build_version,inflight,started_at,last_heartbeat,lease_until)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT(node_id) DO UPDATE SET
  boot_id=EXCLUDED.boot_id,
  role=EXCLUDED.role,
  state=EXCLUDED.state,
  build_version=EXCLUDED.build_version,
  inflight=EXCLUDED.inflight,
  started_at=EXCLUDED.started_at,
  last_heartbeat=EXCLUDED.last_heartbeat,
  lease_until=EXCLUDED.lease_until`,
		record.NodeID, record.BootID, record.Role, record.State, record.BuildVersion, record.Inflight,
		record.StartedAt, record.LastHeartbeat, record.LeaseUntil,
	)
	if err != nil {
		return fmt.Errorf("register node: %w", err)
	}
	return nil
}

func (s *PostgresStore) Heartbeat(ctx context.Context, nodeID, bootID string, state State, inflight int, leaseUntil time.Time) error {
	if nodeID == "" || bootID == "" || inflight < 0 || leaseUntil.IsZero() || (state != StateReady && state != StateDraining) {
		return errors.New("invalid node heartbeat")
	}
	result, err := s.database.ExecContext(ctx, `
UPDATE service_nodes
SET state=$3,inflight=$4,last_heartbeat=NOW(),lease_until=$5
WHERE node_id=$1 AND boot_id=$2`, nodeID, bootID, state, inflight, leaseUntil)
	if err != nil {
		return fmt.Errorf("heartbeat node: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("heartbeat node rows: %w", err)
	}
	if rows != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *PostgresStore) List(ctx context.Context) ([]Record, error) {
	rows, err := s.database.QueryContext(ctx, `
SELECT node_id,boot_id,role,state,build_version,inflight,started_at,last_heartbeat,lease_until
FROM service_nodes
ORDER BY role,node_id`)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	defer rows.Close()
	now := s.now().UTC()
	items := make([]Record, 0)
	for rows.Next() {
		var record Record
		if err := rows.Scan(&record.NodeID, &record.BootID, &record.Role, &record.State, &record.BuildVersion, &record.Inflight, &record.StartedAt, &record.LastHeartbeat, &record.LeaseUntil); err != nil {
			return nil, fmt.Errorf("scan node: %w", err)
		}
		if !record.LeaseUntil.After(now) {
			record.State = StateOffline
		}
		items = append(items, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	return items, nil
}

var _ Store = (*PostgresStore)(nil)
