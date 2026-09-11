package connections

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

type Enrollment struct {
	GroupID string                 `json:"group_id"`
	Marker  string                 `json:"marker"`
	Started time.Time              `json:"started"`
	Member  wecommcp.ConnectMember `json:"member"`
	Pending bool                   `json:"pending"`
}

func (s *Store) PrepareWeCom(ctx context.Context, t, app, id, endpoint, actor string) (Connection, error) {
	if e := s.app(ctx, t, app); e != nil {
		return Connection{}, e
	}
	if !identifier.MatchString(id) {
		return Connection{}, ErrConflict
	}
	if c, e := s.Get(ctx, t, id); e == nil {
		if c.Kind == "wecom_mcp" && c.AppID == app && c.Status != "removed" {
			return c, nil
		}
		return c, ErrConflict
	} else if !errors.Is(e, controlplane.ErrNotFound) {
		return c, e
	}
	endpoint = strings.TrimSpace(endpoint)
	if e := wecommcp.CheckConnection(ctx, endpoint, s.mcpClient); e != nil {
		return Connection{}, e
	}
	c := Connection{TenantID: t, AppID: app, ID: id, Kind: "wecom_mcp", Name: "企业微信机器人", Account: hash(endpoint), Status: "draft"}
	e := database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		var e error
		c.Credential, e = s.vault.Put(ctx, t, []string{secret.WeComMCPRead, secret.WeComMCPSend}, endpoint)
		if e != nil {
			return e
		}
		if _, e = s.sql(ctx).ExecContext(ctx, `INSERT INTO channel_connection(tenant_id,connection_id,app_id,channel_type,account_key,display_name,credential_ref) VALUES($1,$2,$3,$4,$5,$6,$7)`, t, id, app, c.Kind, c.Account, c.Name, c.Credential); e != nil {
			return errors.New("这个连接已经添加，请从连接列表继续设置")
		}
		return s.record(ctx, c, actor, "channel_connection_prepared")
	})
	if e != nil {
		return c, e
	}
	return s.Get(ctx, t, id)
}
func (s *Store) LoadWeComGroups(ctx context.Context, c Connection, actor string) ([]Group, error) {
	if c.Kind != "wecom_mcp" {
		return nil, ErrConflict
	}
	endpoint, e := s.vault.Resolve(ctx, c.TenantID, secret.WeComMCPRead, c.Credential)
	if e != nil {
		return nil, e
	}
	if e = s.record(ctx, c, actor, "channel_group_list_authorized"); e != nil {
		return nil, e
	}
	groups, e := wecommcp.ListConnectGroups(ctx, endpoint, s.mcpClient)
	if e != nil {
		return nil, e
	}
	result := []Group{}
	e = database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		var current int64
		if e := s.sql(ctx).QueryRowContext(ctx, `SELECT version FROM channel_connection WHERE tenant_id=$1 AND connection_id=$2 AND status<>'removed' FOR SHARE`, c.TenantID, c.ID).Scan(&current); e != nil {
			return safe(e)
		}
		if current != c.Version {
			return ErrConflict
		}
		for i, g := range groups {
			g.Name = cleanName(g.Name)
			if g.Name == "" {
				g.Name = "未返回名称的群 " + strconv.Itoa(i+1)
				if stamp, e := time.Parse("2006-01-02 15:04:05", g.LastActive); e == nil {
					g.Name = "最近活跃于 " + stamp.Format("01-02 15:04") + " 的群"
				}
			}
			if _, e := s.sql(ctx).ExecContext(ctx, `INSERT INTO channel_connection_group(tenant_id,connection_id,chat_id,display_name) VALUES($1,$2,$3,$4) ON CONFLICT(tenant_id,connection_id,chat_id) DO UPDATE SET display_name=EXCLUDED.display_name,seen_at=now()`, c.TenantID, c.ID, g.ID, g.Name); e != nil {
				return safe(e)
			}
			result = append(result, Group{ID: g.ID, Name: g.Name})
		}
		return nil
	})
	if e != nil {
		return nil, e
	}
	return s.decorateWeComGroups(ctx, c, result)
}
func (s *Store) BeginEnrollment(ctx context.Context, c Connection, version int64, chat, actor string) (Connection, Enrollment, error) {
	var enroll Enrollment
	if c.Kind != "wecom_mcp" || c.Version != version {
		return c, enroll, ErrConflict
	}
	var exists bool
	if s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channel_connection_group WHERE tenant_id=$1 AND connection_id=$2 AND chat_id=$3 AND seen_at>now()-interval '15 minutes')`, c.TenantID, c.ID, chat).Scan(&exists) != nil || !exists {
		return c, enroll, errors.New("请刷新群列表后重新选择")
	}
	enroll = Enrollment{GroupID: chat, Marker: "CONNECT-" + randomText()[:16], Started: time.Now().UTC().Truncate(time.Second), Pending: true}
	e := s.saveEnrollment(ctx, c, enroll, actor, "channel_group_selected")
	if e != nil {
		return c, enroll, e
	}
	c, e = s.Get(ctx, c.TenantID, c.ID)
	return c, enroll, e
}
func (s *Store) Enrollment(c Connection) Enrollment {
	var e Enrollment
	_ = json.Unmarshal(c.Settings, &e)
	return e
}
func (s *Store) saveEnrollment(ctx context.Context, c Connection, enroll Enrollment, actor, decision string) error {
	return database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		r, e := s.sql(ctx).ExecContext(ctx, `UPDATE channel_connection SET settings=$4,version=version+1,updated_at=now() WHERE tenant_id=$1 AND connection_id=$2 AND version=$3 AND (busy_until IS NULL OR busy_until<now())`, c.TenantID, c.ID, c.Version, encode(enroll))
		if e != nil {
			return safe(e)
		}
		n, _ := r.RowsAffected()
		if n != 1 {
			return ErrConflict
		}
		return s.record(ctx, c, actor, decision)
	})
}
func (s *Store) CheckEnrollment(ctx context.Context, c Connection, version int64, actor string) (Connection, Enrollment, error) {
	en := s.Enrollment(c)
	if c.Kind != "wecom_mcp" || c.Version != version || !en.Pending {
		return c, en, ErrConflict
	}
	endpoint, e := s.vault.Resolve(ctx, c.TenantID, secret.WeComMCPRead, c.Credential)
	if e != nil {
		return c, en, e
	}
	if e = s.record(ctx, c, actor, "channel_enrollment_read_authorized"); e != nil {
		return c, en, e
	}
	member, e := wecommcp.ReadConnectMember(ctx, endpoint, s.mcpClient, en.GroupID, en.Marker, en.Started)
	if e != nil {
		return c, en, e
	}
	member.Name = cleanName(member.Name)
	if member.Name == "" {
		member.Name = "发送确认消息的成员"
	}
	en.Member = member
	if e = s.saveEnrollment(ctx, c, en, actor, "channel_enrollment_member_identified"); e != nil {
		return c, en, e
	}
	c, e = s.Get(ctx, c.TenantID, c.ID)
	return c, en, e
}
func (s *Store) ActivateWeCom(ctx context.Context, c Connection, version int64, actor string) (Connection, error) {
	if c.Kind != "wecom_mcp" || c.Version != version {
		return c, ErrConflict
	}
	if e := s.app(ctx, c.TenantID, c.AppID); e != nil {
		return c, e
	}
	en := s.Enrollment(c)
	if !en.Pending && c.BindingID != "" {
		if c.Status == "connected" {
			return c, nil
		}
		if e := s.finish(ctx, c, actor); e != nil {
			return c, e
		}
		return s.Get(ctx, c.TenantID, c.ID)
	}
	if !en.Pending || en.Member.ID == "" || time.Since(en.Started) > 10*time.Minute {
		return c, errors.New("请先完成群里的确认消息，过期后需要重新确认")
	}
	e := database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		if e := s.outcome(ctx, c, "connected", ""); e != nil {
			return e
		}
		cfg := wecommcp.BindingConfig{AllowedChatIDs: []string{en.GroupID}, AllowedUserIDs: []string{en.Member.ID}, MentionPrefix: en.Member.Prefix, MentionStyle: en.Member.Style, Timezone: "Asia/Shanghai", StartAt: time.Now().UTC().Truncate(time.Second).Format(time.RFC3339), DedupeMode: "fingerprint-v1", SetupMarker: en.Marker, ManagedIdentity: true}
		cfg.GroupGrants = map[string]wecommcp.GroupGrant{en.GroupID: {StartAt: cfg.StartAt, Members: []wecommcp.MemberGrant{{ID: en.Member.ID, Name: en.Member.Name, Since: cfg.StartAt}}}}
		if e := s.checkWeComRoute(ctx, c, en.GroupID, en.Member.Prefix); e != nil {
			return e
		}
		if c.BindingID == "" {
			b, e := s.newBinding(ctx, c, encode(cfg), "active")
			if e != nil {
				return e
			}
			c.BindingID = b.ID
		} else {
			b, e := s.repo.GetChannelBinding(ctx, c.TenantID, c.BindingID)
			if e != nil {
				return e
			}
			cfg, e = wecommcp.ParseBinding(b)
			if e != nil {
				return e
			}
			if cfg.MentionPrefix != en.Member.Prefix {
				return errors.New("这次确认的机器人名称与已有连接不同，请核对后重试")
			}
			if !slices.Contains(cfg.AllowedChatIDs, en.GroupID) {
				cfg.AllowedChatIDs = append(cfg.AllowedChatIDs, en.GroupID)
			}
			grant := cfg.GroupGrants[en.GroupID]
			now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
			if grant.StartAt == "" || len(grant.Members) == 0 {
				grant.StartAt = now
			}
			found := false
			for i, m := range grant.Members {
				if m.ID == en.Member.ID {
					grant.Members[i].Name = en.Member.Name
					found = true
				}
			}
			if !found {
				grant.Members = append(grant.Members, wecommcp.MemberGrant{ID: en.Member.ID, Name: en.Member.Name, Since: now})
			}
			cfg.GroupGrants[en.GroupID] = grant
			cfg.SyncMembers()
			cfg.SetupMarker = en.Marker
			b.Config = encode(cfg)
			if _, e = wecommcp.ParseBinding(b); e != nil {
				return errors.New("群或成员数量已达到上限")
			}
			if _, e = s.repo.UpdateChannelBinding(ctx, b.TenantID, b.ID, b.Config, "active", b.Version); e != nil {
				return e
			}
		}
		en.Pending = false
		if _, e := s.sql(ctx).ExecContext(ctx, `UPDATE channel_connection SET binding_id=$3,display_name=$4,settings=$5 WHERE tenant_id=$1 AND connection_id=$2`, c.TenantID, c.ID, c.BindingID, cleanName(en.Member.Prefix), encode(en)); e != nil {
			return safe(e)
		}
		return s.record(ctx, c, actor, "channel_connection_activated")
	})
	if e != nil {
		return c, e
	}
	return s.Get(ctx, c.TenantID, c.ID)
}

func (s *Store) checkWeComRoute(ctx context.Context, c Connection, chat, prefix string) error {
	// Serialize competing activations. A pre-transaction existence check alone
	// lets two browser connections both pass and reply from the same bot/group.
	if _, e := s.sql(ctx).ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "wecom-group/"+chat+"/"+prefix); e != nil {
		return safe(e)
	}
	var duplicate bool
	e := s.sql(ctx).QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channel_binding WHERE channel_type='wecom_mcp' AND status='active' AND channel_binding_id<>$1 AND config->'allowed_chat_ids' ? $2 AND config->>'mention_prefix'=$3 AND (COALESCE(config->>'managed_identity','false')<>'true' OR config->'group_grants' IS NULL OR jsonb_array_length(COALESCE(config->'group_grants'->$2->'members','[]'::jsonb))>0))`, c.BindingID, chat, prefix).Scan(&duplicate)
	if e != nil {
		return safe(e)
	}
	if duplicate {
		return errors.New("这个群已连接同名机器人，请先暂停旧连接")
	}
	return nil
}
func (s *Store) Targets(ctx context.Context) ([]config.WeComMCPTarget, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result := []config.WeComMCPTarget{}
	if s == nil {
		return result, nil
	}
	rows, e := s.db.QueryContext(ctx, `SELECT tenant_id,binding_id FROM channel_connection WHERE channel_type='wecom_mcp' AND status='connected' AND binding_id IS NOT NULL ORDER BY tenant_id,connection_id LIMIT 1001`)
	if e != nil {
		return nil, safe(e)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var t config.WeComMCPTarget
		if rows.Scan(&t.TenantID, &t.BindingID) != nil {
			return nil, ErrUnavailable
		}
		result = append(result, t)
		if len(result) > 1000 {
			return nil, errors.New("连接数量超出当前节点的接收上限")
		}
	}
	return result, safe(rows.Err())
}
