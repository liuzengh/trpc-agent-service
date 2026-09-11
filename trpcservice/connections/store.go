// Package connections manages browser-created IM connections, not Agent execution.
package connections

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credentials"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

var ErrUnavailable = errors.New("连接管理未就绪，请联系管理员检查数据库和加密设置")
var ErrConflict = errors.New("连接已变化，请刷新后重试")
var ErrBusy = errors.New("上一次连接操作还在处理中，请稍后检查状态")
var ErrConfirm = errors.New("机器人正在连接其他服务，请确认是否切换")
var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type Connection struct {
	TenantID     string          `json:"tenant_id"`
	ID           string          `json:"connection_id"`
	AppID        string          `json:"app_id"`
	Kind         string          `json:"channel_type"`
	Name         string          `json:"name"`
	BindingID    string          `json:"binding_id"`
	Status       string          `json:"status"`
	Message      string          `json:"message"`
	URL          string          `json:"callback_url,omitempty"`
	Version      int64           `json:"version"`
	LastReceived *time.Time      `json:"last_received_at,omitempty"`
	UpdatedAt    time.Time       `json:"updated_at"`
	Account      string          `json:"-"`
	Credential   string          `json:"-"`
	Webhook      string          `json:"-"`
	RemoteHash   string          `json:"-"`
	Settings     json.RawMessage `json:"-"`
	BusyUntil    *time.Time      `json:"-"`
	Operation    string          `json:"operation,omitempty"`
}
type Store struct {
	db        *sql.DB
	repo      controlplane.MutableRepository
	vault     *credentials.Vault
	audit     audit.Writer
	telegram  *telegram.Connector
	mcpClient *http.Client
	publicURL string
}
type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *Store) sql(ctx context.Context) executor {
	if tx := database.Transaction(ctx, s.db); tx != nil {
		return tx
	}
	return s.db
}
func New(repo controlplane.Repository, vault *credentials.Vault, writer audit.Writer, publicURL string, client *http.Client) *Store {
	p, ok := repo.(interface{ SQLDB() *sql.DB })
	mutable, canWrite := repo.(controlplane.MutableRepository)
	if !ok || !canWrite || vault == nil {
		return nil
	}
	return &Store{db: p.SQLDB(), repo: mutable, vault: vault, audit: writer, telegram: telegram.NewConnector(client), mcpClient: client, publicURL: strings.TrimRight(publicURL, "/")}
}

const columns = `tenant_id,connection_id,app_id,channel_type,display_name,COALESCE(binding_id,''),status,message,callback_url,version,last_received_at,updated_at,account_key,credential_ref,webhook_ref,remote_hash,settings,busy_until,operation`

type scanner interface{ Scan(...any) error }

func scan(row scanner) (Connection, error) {
	var c Connection
	e := row.Scan(&c.TenantID, &c.ID, &c.AppID, &c.Kind, &c.Name, &c.BindingID, &c.Status, &c.Message, &c.URL, &c.Version, &c.LastReceived, &c.UpdatedAt, &c.Account, &c.Credential, &c.Webhook, &c.RemoteHash, &c.Settings, &c.BusyUntil, &c.Operation)
	return c, safe(e)
}
func safe(e error) error {
	if e == nil {
		return nil
	}
	if errors.Is(e, sql.ErrNoRows) {
		return controlplane.ErrNotFound
	}
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return e
	}
	return ErrUnavailable
}
func (s *Store) Get(ctx context.Context, t, id string) (Connection, error) {
	if s == nil {
		return Connection{}, ErrUnavailable
	}
	return scan(s.sql(ctx).QueryRowContext(ctx, `SELECT `+columns+` FROM channel_connection WHERE tenant_id=$1 AND connection_id=$2`, t, id))
}
func (s *Store) List(ctx context.Context, t, after string) ([]Connection, string, error) {
	result := []Connection{}
	if s == nil {
		return result, "", nil
	}
	rows, e := s.db.QueryContext(ctx, `SELECT `+columns+` FROM channel_connection WHERE tenant_id=$1 AND connection_id>$2 AND status<>'removed' ORDER BY connection_id LIMIT 101`, t, after)
	if e != nil {
		return nil, "", safe(e)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		c, e := scan(rows)
		if e != nil {
			return nil, "", e
		}
		if len(result) == 100 {
			return result, result[99].ID, nil
		}
		result = append(result, c)
	}
	return result, "", safe(rows.Err())
}
func (s *Store) record(ctx context.Context, c Connection, actor, decision string) error {
	if s.audit == nil {
		return ErrUnavailable
	}
	return s.audit.Record(ctx, audit.Event{TenantID: c.TenantID, Channel: c.Kind, ChannelBindingID: c.BindingID, UserID: actor, Decision: decision, Details: map[string]any{"connection_id": c.ID, "app_id": c.AppID}})
}
func (s *Store) app(ctx context.Context, t, a string) error {
	if s == nil {
		return ErrUnavailable
	}
	if !identifier.MatchString(t) || !identifier.MatchString(a) {
		return errors.New("请选择 Agent")
	}
	tenant, e := s.repo.GetTenant(ctx, t)
	if e != nil {
		return e
	}
	app, e := s.repo.GetAgentApp(ctx, t, a)
	if e != nil {
		return e
	}
	if tenant.Status != "active" || app.Status != "active" || app.StableRevisionID == "" {
		return errors.New("请先发布这个 Agent，再连接机器人")
	}
	return nil
}
func (s *Store) PublicURL(ctx context.Context) string {
	value, _ := s.ReadPublicURL(ctx)
	return value
}

// ReadPublicURL is the shared source for connection setup and diagnostics.
// Do not fall back to an old environment value when reading the database fails.
func (s *Store) ReadPublicURL(ctx context.Context) (string, error) {
	if s == nil {
		return "", ErrUnavailable
	}
	var value string
	e := s.sql(ctx).QueryRowContext(ctx, `SELECT value FROM channel_connection_setting WHERE name='public_url'`).Scan(&value)
	if errors.Is(e, sql.ErrNoRows) {
		value = s.publicURL
	} else if e != nil {
		return "", safe(e)
	}
	return strings.TrimRight(value, "/"), nil
}

func (s *Store) CallbackURL(ctx context.Context, tenant, binding string) string {
	if s == nil {
		return ""
	}
	var value string
	if s.sql(ctx).QueryRowContext(ctx, `SELECT callback_url FROM channel_connection WHERE tenant_id=$1 AND binding_id=$2`, tenant, binding).Scan(&value) != nil {
		return ""
	}
	return value
}
func (s *Store) SetPublicURL(ctx context.Context, t, actor, raw string) error {
	if s == nil {
		return ErrUnavailable
	}
	value, e := validPublicURL(raw)
	if e != nil {
		return e
	}
	return database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		if _, e := s.sql(ctx).ExecContext(ctx, `INSERT INTO channel_connection_setting(name,value) VALUES('public_url',$1) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value,updated_at=now()`, value); e != nil {
			return safe(e)
		}
		return s.record(ctx, Connection{TenantID: t}, actor, "channel_public_address_updated")
	})
}
func validPublicURL(raw string) (string, error) {
	u, e := url.Parse(strings.TrimSpace(raw))
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || strings.ContainsAny(raw, "\r\n\\") {
		return "", errors.New("请填写服务器的公网 HTTPS 地址，不要包含密码或查询参数")
	}
	return strings.TrimRight(u.String(), "/"), nil
}
func hash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func randomText() string {
	var data [24]byte
	if _, e := rand.Read(data[:]); e != nil {
		return uuid.NewString()
	}
	return hex.EncodeToString(data[:])
}
func encode(value any) json.RawMessage { raw, _ := json.Marshal(value); return raw }
func (s *Store) reserve(ctx context.Context, c Connection, version int64, actor, action string) (Connection, error) {
	if c.Version != version {
		return c, ErrConflict
	}
	if c.Status == "removed" {
		return c, ErrConflict
	}
	if c.BusyUntil != nil && c.BusyUntil.After(time.Now()) {
		return c, ErrBusy
	}
	e := database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		operation := "activate"
		if action == "channel_connection_removal_requested" {
			operation = "remove"
		}
		r, e := s.sql(ctx).ExecContext(ctx, `UPDATE channel_connection SET status='connecting',operation=$4,busy_until=now()+interval '45 seconds',message='',version=version+1,updated_at=now() WHERE tenant_id=$1 AND connection_id=$2 AND version=$3 AND (busy_until IS NULL OR busy_until<now())`, c.TenantID, c.ID, version, operation)
		if e != nil {
			return safe(e)
		}
		n, _ := r.RowsAffected()
		if n != 1 {
			return ErrConflict
		}
		return s.record(ctx, c, actor, action)
	})
	if e != nil {
		return c, e
	}
	return s.Get(ctx, c.TenantID, c.ID)
}
func (s *Store) outcome(ctx context.Context, c Connection, status, message string) error {
	r, e := s.sql(ctx).ExecContext(ctx, `UPDATE channel_connection SET status=$4,message=$5,busy_until=NULL,operation=CASE WHEN $4 IN ('unknown','connecting') THEN operation ELSE '' END,version=version+1,updated_at=now() WHERE tenant_id=$1 AND connection_id=$2 AND version=$3`, c.TenantID, c.ID, c.Version, status, message)
	if e != nil {
		return safe(e)
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return ErrConflict
	}
	return nil
}
func (s *Store) failed(c Connection, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = s.outcome(ctx, c, "unknown", message)
}
func (s *Store) newBinding(ctx context.Context, c Connection, config json.RawMessage, status string) (controlplane.ChannelBinding, error) {
	b := controlplane.ChannelBinding{ID: "binding-" + uuid.NewString(), TenantID: c.TenantID, AppID: c.AppID, ChannelType: c.Kind, AccountID: c.Account, CallbackKey: randomText(), SecretRef: c.Credential, Config: config, Status: status, Version: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	return b, s.repo.CreateChannelBinding(ctx, b)
}
