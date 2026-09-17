package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

// Source reads platform coordination facts and official framework Session rows.
// The databases may be split throughout a migration: coordination stays in the
// control database, while SDK Session data comes from the profile-selected
// data plane. During observation that data plane is the cut-over target.
type Source struct {
	coordinationDB *sql.DB
	sessionDB      *sql.DB
}

func NewSource(db *sql.DB) *Source { return NewSplitSource(db, db) }

func NewSplitSource(coordinationDB, sessionDB *sql.DB) *Source {
	return &Source{coordinationDB: coordinationDB, sessionDB: sessionDB}
}

type keyCursor struct {
	AgentAppID string `json:"agent_app_id"`
	SessionID  string `json:"session_id"`
	Empty      bool   `json:"empty,omitempty"`
}

func (s *Source) CaptureWatermark(ctx context.Context, tenantID string) (string, error) {
	if s == nil || s.coordinationDB == nil || s.sessionDB == nil {
		return "", runtime.ErrBackendUnavailable
	}
	if tenantID == "" {
		return "", runtime.ErrTenantScope
	}
	var cursor keyCursor
	err := s.coordinationDB.QueryRowContext(ctx, `SELECT agent_app_id,session_id FROM public.session_head
WHERE tenant_id=$1 ORDER BY agent_app_id DESC,session_id DESC LIMIT 1`, tenantID).
		Scan(&cursor.AgentAppID, &cursor.SessionID)
	if errors.Is(err, sql.ErrNoRows) {
		cursor.Empty = true
		return encodeCursor(cursor)
	}
	if err != nil {
		return "", err
	}
	return encodeCursor(cursor)
}

func (s *Source) LoadSessionImage(ctx context.Context, key sessionstore.SessionKey) (sessiondriver.SessionImage, error) {
	if s == nil || s.coordinationDB == nil || s.sessionDB == nil {
		return sessiondriver.SessionImage{}, runtime.ErrBackendUnavailable
	}
	coordinationTx, err := s.coordinationDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	defer coordinationTx.Rollback()
	sessionTx := coordinationTx
	if s.sessionDB != s.coordinationDB {
		sessionTx, err = s.sessionDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			return sessiondriver.SessionImage{}, err
		}
		defer sessionTx.Rollback()
	}
	image, err := loadImage(ctx, coordinationTx, sessionTx, key)
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	if sessionTx != coordinationTx {
		if err := sessionTx.Commit(); err != nil {
			return sessiondriver.SessionImage{}, err
		}
	}
	if err := coordinationTx.Commit(); err != nil {
		return sessiondriver.SessionImage{}, err
	}
	return image, nil
}

func (s *Source) PageSessions(ctx context.Context, in sessiondriver.PageRequest) (sessiondriver.Page, error) {
	if s == nil || s.coordinationDB == nil || s.sessionDB == nil {
		return sessiondriver.Page{}, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.Limit < 1 || in.Limit > 1000 {
		return sessiondriver.Page{}, runtime.ErrInvariantViolation
	}
	upper, err := decodeCursor(in.SnapshotWatermark)
	if err != nil {
		return sessiondriver.Page{}, err
	}
	if upper.Empty {
		return sessiondriver.Page{NextCheckpoint: eofCheckpoint(in.SnapshotWatermark), Complete: true}, nil
	}
	after := keyCursor{}
	if in.After != "" {
		after, err = decodeCursor(in.After)
		if err != nil || after.Empty {
			return sessiondriver.Page{}, runtime.ErrInvariantViolation
		}
	}
	coordinationTx, err := s.coordinationDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return sessiondriver.Page{}, err
	}
	defer coordinationTx.Rollback()
	sessionTx := coordinationTx
	if s.sessionDB != s.coordinationDB {
		sessionTx, err = s.sessionDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			return sessiondriver.Page{}, err
		}
		defer sessionTx.Rollback()
	}
	rows, err := coordinationTx.QueryContext(ctx, `SELECT agent_app_id,session_id FROM public.session_head
WHERE tenant_id=$1 AND (agent_app_id,session_id)>($2,$3) AND (agent_app_id,session_id)<=($4,$5)
ORDER BY agent_app_id,session_id LIMIT $6`, in.TenantID, after.AgentAppID, after.SessionID,
		upper.AgentAppID, upper.SessionID, in.Limit+1)
	if err != nil {
		return sessiondriver.Page{}, err
	}
	var keys []sessionstore.SessionKey
	for rows.Next() {
		key := sessionstore.SessionKey{TenantID: in.TenantID}
		if err := rows.Scan(&key.AgentAppID, &key.SessionID); err != nil {
			rows.Close()
			return sessiondriver.Page{}, err
		}
		keys = append(keys, key)
	}
	if err := rows.Close(); err != nil {
		return sessiondriver.Page{}, err
	}
	complete := len(keys) <= in.Limit
	if len(keys) > in.Limit {
		keys = keys[:in.Limit]
	}
	page := sessiondriver.Page{Complete: complete}
	for _, key := range keys {
		image, err := loadImage(ctx, coordinationTx, sessionTx, key)
		if err != nil {
			return sessiondriver.Page{}, err
		}
		page.Sessions = append(page.Sessions, image)
	}
	if len(keys) == 0 {
		page.NextCheckpoint = eofCheckpoint(in.SnapshotWatermark)
	} else {
		last := keys[len(keys)-1]
		page.NextCheckpoint, err = encodeCursor(keyCursor{AgentAppID: last.AgentAppID, SessionID: last.SessionID})
		if err != nil {
			return sessiondriver.Page{}, err
		}
	}
	if sessionTx != coordinationTx {
		if err := sessionTx.Commit(); err != nil {
			return sessiondriver.Page{}, err
		}
	}
	if err := coordinationTx.Commit(); err != nil {
		return sessiondriver.Page{}, err
	}
	return page, nil
}

func (s *Source) Fingerprint(ctx context.Context, tenantID, watermark string) (sessiondriver.Fingerprint, error) {
	if watermark == "" {
		var err error
		watermark, err = s.CaptureWatermark(ctx, tenantID)
		if err != nil {
			return sessiondriver.Fingerprint{}, err
		}
	}
	upper, err := decodeCursor(watermark)
	if err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	if upper.Empty {
		return sessiondriver.FingerprintFromItems(nil, watermark), nil
	}
	if s == nil || s.coordinationDB == nil || s.sessionDB == nil {
		return sessiondriver.Fingerprint{}, runtime.ErrBackendUnavailable
	}
	coordinationTx, err := s.coordinationDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	defer coordinationTx.Rollback()
	sessionTx := coordinationTx
	if s.sessionDB != s.coordinationDB {
		sessionTx, err = s.sessionDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			return sessiondriver.Fingerprint{}, err
		}
		defer sessionTx.Rollback()
	}
	rows, err := coordinationTx.QueryContext(ctx, `SELECT agent_app_id,session_id FROM public.session_head
WHERE tenant_id=$1 AND (agent_app_id,session_id)<=($2,$3) ORDER BY agent_app_id,session_id`,
		tenantID, upper.AgentAppID, upper.SessionID)
	if err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	var keys []sessionstore.SessionKey
	for rows.Next() {
		key := sessionstore.SessionKey{TenantID: tenantID}
		if err := rows.Scan(&key.AgentAppID, &key.SessionID); err != nil {
			rows.Close()
			return sessiondriver.Fingerprint{}, err
		}
		keys = append(keys, key)
	}
	if err := rows.Close(); err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	images := make([]sessiondriver.SessionImage, 0, len(keys))
	for _, key := range keys {
		image, err := loadImage(ctx, coordinationTx, sessionTx, key)
		if err != nil {
			return sessiondriver.Fingerprint{}, err
		}
		images = append(images, image)
	}
	result, err := FingerprintImages(images, watermark)
	if err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	if sessionTx != coordinationTx {
		if err := sessionTx.Commit(); err != nil {
			return sessiondriver.Fingerprint{}, err
		}
	}
	if err := coordinationTx.Commit(); err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	return result, nil
}

func FingerprintImages(images []sessiondriver.SessionImage, watermark string) (sessiondriver.Fingerprint, error) {
	items := make([]string, 0, len(images))
	for _, image := range images {
		digest, err := sessiondriver.SnapshotDigest(image)
		if err != nil {
			return sessiondriver.Fingerprint{}, err
		}
		items = append(items, image.Head.AgentAppID+"\x00"+image.Head.SessionID+"\x00"+digest)
	}
	return sessiondriver.FingerprintFromItems(items, watermark), nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func loadImage(ctx context.Context, coordination queryer, sessions queryer, key sessionstore.SessionKey) (sessiondriver.SessionImage, error) {
	if key.TenantID == "" || key.AgentAppID == "" || key.SessionID == "" {
		return sessiondriver.SessionImage{}, runtime.ErrTenantScope
	}
	image := sessiondriver.SessionImage{}
	image.Head.SessionKey = key
	err := coordination.QueryRowContext(ctx, `SELECT version,last_fence,last_session_seq,next_input_seq,last_allocated_input_seq
FROM public.session_head WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`,
		key.TenantID, key.AgentAppID, key.SessionID).Scan(&image.Head.Version, &image.Head.LastFence,
		&image.Head.LastSessionSeq, &image.Head.NextInputSeq, &image.LastAllocatedInputSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiondriver.SessionImage{}, runtime.ErrNotFound
	}
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	appName := key.TenantID + "/" + key.AgentAppID
	stateRows, err := sessions.QueryContext(ctx, `SELECT user_id,state,created_at,updated_at,expires_at
FROM public.session_states WHERE app_name=$1 AND session_id=$2 AND deleted_at IS NULL ORDER BY id`, appName, key.SessionID)
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	for stateRows.Next() {
		if image.SDK != nil {
			stateRows.Close()
			return sessiondriver.SessionImage{}, runtime.ErrInvariantViolation
		}
		item := &sessiondriver.SDKSessionImage{AppName: appName, SessionID: key.SessionID}
		var expiresAt sql.NullTime
		if err := stateRows.Scan(&item.UserID, &item.State, &item.CreatedAt, &item.UpdatedAt, &expiresAt); err != nil {
			stateRows.Close()
			return sessiondriver.SessionImage{}, err
		}
		if item.UserID == "" || !json.Valid(item.State) {
			stateRows.Close()
			return sessiondriver.SessionImage{}, runtime.ErrInvariantViolation
		}
		item.CreatedAt, item.UpdatedAt, item.ExpiresAt = item.CreatedAt.UTC(), item.UpdatedAt.UTC(), nullableUTC(expiresAt)
		image.SDK = item
	}
	if err := stateRows.Close(); err != nil {
		return sessiondriver.SessionImage{}, err
	}
	if image.SDK != nil {
		if err := loadSDKRows(ctx, sessions, image.SDK); err != nil {
			return sessiondriver.SessionImage{}, err
		}
	}
	commitRows, err := coordination.QueryContext(ctx, `SELECT commit_id,request_id,request_digest,input_seq,stage,outcome,fence,
session_version,COALESCE(reply_cursor,''),COALESCE(result_ref,''),created_at FROM public.session_commit
WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 ORDER BY session_version,commit_id`, key.TenantID, key.AgentAppID, key.SessionID)
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	for commitRows.Next() {
		var item sessiondriver.CommitRecord
		if err := commitRows.Scan(&item.CommitID, &item.RequestID, &item.RequestDigest, &item.InputSeq,
			&item.Stage, &item.Outcome, &item.Fence, &item.SessionVersion, &item.ReplyCursor,
			&item.ResultRef, &item.CreatedAt); err != nil {
			commitRows.Close()
			return sessiondriver.SessionImage{}, err
		}
		item.CreatedAt = item.CreatedAt.UTC()
		image.Commits = append(image.Commits, item)
	}
	if err := commitRows.Close(); err != nil {
		return sessiondriver.SessionImage{}, err
	}
	return image, nil
}

func loadSDKRows(ctx context.Context, q queryer, image *sessiondriver.SDKSessionImage) error {
	events, err := q.QueryContext(ctx, `SELECT event,created_at,updated_at,expires_at FROM public.session_events
WHERE app_name=$1 AND user_id=$2 AND session_id=$3 AND deleted_at IS NULL ORDER BY created_at,id`, image.AppName, image.UserID, image.SessionID)
	if err != nil {
		return err
	}
	for events.Next() {
		var item sessiondriver.SDKEventRecord
		var expiresAt sql.NullTime
		if err := events.Scan(&item.Event, &item.CreatedAt, &item.UpdatedAt, &expiresAt); err != nil {
			events.Close()
			return err
		}
		if !json.Valid(item.Event) {
			events.Close()
			return runtime.ErrInvariantViolation
		}
		item.CreatedAt, item.UpdatedAt, item.ExpiresAt = item.CreatedAt.UTC(), item.UpdatedAt.UTC(), nullableUTC(expiresAt)
		image.Events = append(image.Events, item)
	}
	if err := events.Close(); err != nil {
		return err
	}
	tracks, err := q.QueryContext(ctx, `SELECT track,event,created_at,updated_at,expires_at FROM public.session_track_events
WHERE app_name=$1 AND user_id=$2 AND session_id=$3 AND deleted_at IS NULL ORDER BY created_at,id`, image.AppName, image.UserID, image.SessionID)
	if err != nil {
		return err
	}
	for tracks.Next() {
		var item sessiondriver.SDKTrackEventRecord
		var expiresAt sql.NullTime
		if err := tracks.Scan(&item.Track, &item.Event, &item.CreatedAt, &item.UpdatedAt, &expiresAt); err != nil {
			tracks.Close()
			return err
		}
		if item.Track == "" || !json.Valid(item.Event) {
			tracks.Close()
			return runtime.ErrInvariantViolation
		}
		item.CreatedAt, item.UpdatedAt, item.ExpiresAt = item.CreatedAt.UTC(), item.UpdatedAt.UTC(), nullableUTC(expiresAt)
		image.TrackEvents = append(image.TrackEvents, item)
	}
	if err := tracks.Close(); err != nil {
		return err
	}
	summaries, err := q.QueryContext(ctx, `SELECT filter_key,summary,updated_at,expires_at FROM public.session_summaries
WHERE app_name=$1 AND user_id=$2 AND session_id=$3 AND deleted_at IS NULL ORDER BY filter_key,id`, image.AppName, image.UserID, image.SessionID)
	if err != nil {
		return err
	}
	for summaries.Next() {
		var item sessiondriver.SDKSummaryRecord
		var expiresAt sql.NullTime
		if err := summaries.Scan(&item.FilterKey, &item.Summary, &item.UpdatedAt, &expiresAt); err != nil {
			summaries.Close()
			return err
		}
		if !json.Valid(item.Summary) {
			summaries.Close()
			return runtime.ErrInvariantViolation
		}
		item.UpdatedAt, item.ExpiresAt = item.UpdatedAt.UTC(), nullableUTC(expiresAt)
		image.Summaries = append(image.Summaries, item)
	}
	return summaries.Close()
}

func nullableUTC(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func encodeCursor(value keyCursor) (string, error) {
	return sessiondriver.EncodeSessionWatermark(value.AgentAppID, value.SessionID, value.Empty)
}

func decodeCursor(value string) (keyCursor, error) {
	agentAppID, sessionID, empty, err := sessiondriver.DecodeSessionWatermark(value)
	if err != nil {
		return keyCursor{}, err
	}
	return keyCursor{AgentAppID: agentAppID, SessionID: sessionID, Empty: empty}, nil
}

func eofCheckpoint(watermark string) string {
	return "pg-session-eof-v1:" + sessiondriver.FingerprintFromItems([]string{watermark}, watermark).Digest
}

var _ sessiondriver.SnapshotReader = (*Source)(nil)
var _ sessiondriver.BackfillSource = (*Source)(nil)
var _ sessiondriver.Inventory = (*Source)(nil)
