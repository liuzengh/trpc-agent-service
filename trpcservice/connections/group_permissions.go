package connections

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

func (s *Store) decorateWeComGroups(ctx context.Context, c Connection, groups []Group) ([]Group, error) {
	if c.Kind != "wecom_mcp" || c.BindingID == "" {
		return groups, nil
	}
	b, e := s.repo.GetChannelBinding(ctx, c.TenantID, c.BindingID)
	if e != nil {
		return nil, e
	}
	cfg, e := wecommcp.ParseBinding(b)
	if e != nil {
		return nil, e
	}
	for i := range groups {
		members := slices.Clone(cfg.GroupGrants[groups[i].ID].Members)
		for j := range members {
			if members[j].Name == "" {
				members[j].Name = "已授权成员"
			}
		}
		groups[i].Members = members
		groups[i].Selected = len(members) > 0
	}
	return groups, nil
}

// RevokeWeCom removes one member, or all members when user is empty. Retained
// checkpoints and histories are never reset; a re-enrollment gets a new time bound.
func (s *Store) RevokeWeCom(ctx context.Context, c Connection, version int64, chat, user, actor string) (Connection, error) {
	if c.Kind != "wecom_mcp" || c.BindingID == "" || c.Version != version || c.Status == "removed" {
		return c, ErrConflict
	}
	if c.Operation != "" || (c.BusyUntil != nil && c.BusyUntil.After(time.Now())) {
		return c, ErrBusy
	}
	e := database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		if e := s.outcome(ctx, c, c.Status, ""); e != nil {
			return e
		}
		b, e := s.repo.GetChannelBinding(ctx, c.TenantID, c.BindingID)
		if e != nil {
			return e
		}
		cfg, e := wecommcp.ParseBinding(b)
		if e != nil {
			return e
		}
		g, ok := cfg.GroupGrants[chat]
		if !ok {
			return errors.New("没有找到这个群的授权，请刷新")
		}
		members := []wecommcp.MemberGrant{}
		found := user == ""
		for _, m := range g.Members {
			if user == "" || m.ID == user {
				found = true
				continue
			}
			members = append(members, m)
		}
		if !found {
			return ErrConflict
		}
		g.Members = members
		cfg.GroupGrants[chat] = g
		cfg.SyncMembers()
		if _, e = s.repo.UpdateChannelBinding(ctx, c.TenantID, b.ID, encode(cfg), b.Status, b.Version); e != nil {
			return e
		}
		// A previously identified but unconfirmed enrollment cannot restore the
		// revoked grant. Any later authorization must use a fresh marker.
		if _, e = s.sql(ctx).ExecContext(ctx, `UPDATE channel_connection SET settings='{}' WHERE tenant_id=$1 AND connection_id=$2`, c.TenantID, c.ID); e != nil {
			return safe(e)
		}
		return s.record(ctx, c, actor, "channel_group_permission_revoked")
	})
	if e != nil {
		return c, e
	}
	return s.Get(ctx, c.TenantID, c.ID)
}
