package console

import (
	"context"
	"encoding/json"
	"time"
)

// Wait longer than the maximum run duration; keep records until native data
// cleanup succeeds, so a backend outage cannot orphan the cleanup target.
func (s *Store) ExpiredSessions(ctx context.Context) ([]Record, error) {
	cutoff := time.Now().UTC().Add(-5 * time.Minute)
	out := []Record{}
	if s.db != nil {
		rows, err := s.db.QueryContext(ctx, "SELECT "+columns+" FROM debug_session WHERE expires_at<$1 ORDER BY expires_at LIMIT 5", cutoff)
		if err != nil {
			return nil, mapError(err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			record, err := scan(rows, "session")
			if err != nil {
				return nil, err
			}
			out = append(out, record)
		}
		return out, mapError(rows.Err())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range s.records {
		if record.Kind == "session" && record.ExpiresAt.Before(cutoff) {
			out = append(out, clone(record))
			if len(out) == 5 {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) PurgeSession(ctx context.Context, record Record, session Session) error {
	if s.db != nil {
		return s.Transaction(ctx, func(ctx context.Context) error {
			for _, name := range []string{"debug_tool_execution", "debug_tool_approval", "debug_event"} {
				if _, err := s.executor(ctx).ExecContext(ctx, "DELETE FROM "+name+" WHERE tenant_id=$1 AND app_id IN (SELECT record_id FROM debug_run WHERE tenant_id=$1 AND owner_id=$2 AND data->>'session_id'=$3)", record.TenantID, record.OwnerID, record.ID); err != nil {
					return mapError(err)
				}
			}
			if _, err := s.executor(ctx).ExecContext(ctx, "DELETE FROM debug_approval_decision WHERE tenant_id=$1 AND owner_id=$2", record.TenantID, session.UserID); err != nil {
				return mapError(err)
			}
			if _, err := s.executor(ctx).ExecContext(ctx, "DELETE FROM debug_run WHERE tenant_id=$1 AND owner_id=$2 AND data->>'session_id'=$3", record.TenantID, record.OwnerID, record.ID); err != nil {
				return mapError(err)
			}
			if _, err := s.executor(ctx).ExecContext(ctx, "DELETE FROM debug_snapshot WHERE tenant_id=$1 AND record_id=$2 AND owner_id=$3", record.TenantID, session.SnapshotID, record.OwnerID); err != nil {
				return mapError(err)
			}
			return s.Delete(ctx, "session", record.TenantID, record.ID)
		})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	runIDs := map[string]bool{}
	for _, r := range s.records {
		if r.Kind == "run" && r.TenantID == record.TenantID && r.OwnerID == record.OwnerID {
			var run Run
			_ = json.Unmarshal(r.Data, &run)
			if run.SessionID == record.ID {
				runIDs[r.ID] = true
			}
		}
	}
	for k, r := range s.records {
		if r.TenantID != record.TenantID {
			continue
		}
		remove := false
		switch r.Kind {
		case "run":
			remove = runIDs[r.ID]
		case "tool", "approval", "event":
			remove = runIDs[r.AppID]
		case "decision":
			remove = r.OwnerID == session.UserID
		case "snapshot":
			remove = r.ID == session.SnapshotID && r.OwnerID == record.OwnerID
		case "session":
			remove = r.ID == record.ID && r.OwnerID == record.OwnerID
		}
		if remove {
			delete(s.records, k)
		}
	}
	return nil
}

func (e *Engine) maintenance(ctx context.Context) {
	_ = e.Store.Cleanup(ctx, "worker")
	if e.Cleanup == nil {
		return
	}
	records, err := e.Store.ExpiredSessions(ctx)
	if err != nil {
		return
	}
	for _, record := range records {
		var session Session
		if json.Unmarshal(record.Data, &session) != nil || session.UserID != "console_"+Hash(record.TenantID+"\x00"+record.OwnerID+"\x00"+record.ID) {
			continue
		}
		limited, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = e.Cleanup(limited, record.TenantID, record.AppID, session.UserID, record.ID)
		if err == nil {
			_ = e.Store.PurgeSession(limited, record, session)
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
	}
}
