package modelregistry

import (
	"context"
	"errors"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

var ErrAddressKey = errors.New("修改 API 地址时必须重新填写目标服务的 API Key，不会将原密钥自动发送到新地址")
var ErrSuperseded = errors.New("这个配置已有新版本，请打开最新版本修改模型或地址；旧版本仍可更名和更新密钥")

type Edit struct {
	TenantID        string
	ID              string
	ExpectedVersion int64
	Name            string
	Model           string
	BaseURL         string
	APIKey          string
	NewID           string
	Actor           string
}

type EditResult struct {
	Connection Connection `json:"connection"`
	PreviousID string     `json:"previous_connection_id,omitempty"`
	NewConfig  bool       `json:"new_config_version"`
	KeyChanged bool       `json:"key_changed"`
}

// Edit executes under the caller's transaction (including its audit write), or
// opens its own transaction for controlled internal callers. A locked version
// prevents concurrent edits and retry-after-commit from creating duplicate forks.
func (s *Store) Edit(ctx context.Context, in Edit, rotateOnly bool) (EditResult, error) {
	if s == nil {
		return EditResult{}, ErrUnavailable
	}
	if in.ExpectedVersion < 1 {
		return EditResult{}, ErrInvalid
	}
	in.Name = strings.TrimSpace(in.Name)
	in.Model = strings.TrimSpace(in.Model)
	in.BaseURL = strings.TrimRight(strings.TrimSpace(in.BaseURL), "/")
	in.APIKey = strings.TrimSpace(in.APIKey)
	var result EditResult
	err := database.InTransaction(ctx, s.db, func(ctx context.Context) error {
		current, err := scan(s.executor(ctx).QueryRowContext(ctx, `SELECT `+metadata+` FROM model_connection WHERE tenant_id=$1 AND connection_id=$2 FOR UPDATE`, in.TenantID, in.ID))
		if err != nil {
			return err
		}
		if current.Version != in.ExpectedVersion {
			return controlplane.ErrConflict
		}
		if rotateOnly {
			if in.APIKey == "" {
				return ErrInvalid
			}
			in.Name, in.Model, in.BaseURL = current.Name, current.Model, current.BaseURL
		}
		for _, v := range []string{in.Name, in.Model, in.Actor} {
			if v == "" || len(v) > 256 || strings.ContainsAny(v, "\x00\r\n") {
				return ErrInvalid
			}
		}
		configChanged := current.Model != in.Model || current.BaseURL != in.BaseURL
		if configChanged {
			if current.SupersededBy != "" {
				return ErrSuperseded
			}
			if in.NewID == "" || in.NewID == current.ID {
				return ErrInvalid
			}
			if !s.allows(in.BaseURL) {
				return ErrEndpoint
			}
			if current.BaseURL != in.BaseURL && in.APIKey == "" {
				return ErrAddressKey
			}
			key := in.APIKey
			if key == "" {
				key, err = s.credential(ctx, current)
				if err != nil {
					return err
				}
			}
			next := Connection{TenantID: in.TenantID, ID: in.NewID, Name: in.Name, Model: in.Model, BaseURL: in.BaseURL, CreatedBy: in.Actor, RootID: current.RootID, ConfigVersion: current.ConfigVersion + 1}
			created, err := s.insert(ctx, next, key)
			if err != nil {
				return err
			}
			_, err = s.executor(ctx).ExecContext(ctx, `UPDATE model_connection SET superseded_by=$3,version=version+1,updated_by=$4,updated_at=now() WHERE tenant_id=$1 AND connection_id=$2`, in.TenantID, in.ID, created.ID, in.Actor)
			if err != nil {
				return mapError(err)
			}
			result = EditResult{Connection: created, PreviousID: current.ID, NewConfig: true, KeyChanged: in.APIKey != ""}
			return nil
		}
		if in.APIKey != "" {
			if !s.allows(current.BaseURL) {
				return ErrEndpoint
			}
			sealed, err := s.encrypt(current, in.APIKey)
			if err != nil {
				return err
			}
			_, err = s.executor(ctx).ExecContext(ctx, `UPDATE model_connection SET display_name=$3,encrypted_key=$4,credential_version=credential_version+1,version=version+1,updated_by=$5,updated_at=now() WHERE tenant_id=$1 AND connection_id=$2`, in.TenantID, in.ID, in.Name, sealed, in.Actor)
			if err != nil {
				return mapError(err)
			}
			result.KeyChanged = true
		} else if in.Name != current.Name {
			_, err = s.executor(ctx).ExecContext(ctx, `UPDATE model_connection SET display_name=$3,version=version+1,updated_by=$4,updated_at=now() WHERE tenant_id=$1 AND connection_id=$2`, in.TenantID, in.ID, in.Name, in.Actor)
			if err != nil {
				return mapError(err)
			}
		}
		result.Connection, err = s.Get(ctx, in.TenantID, in.ID)
		return err
	})
	if err != nil {
		return EditResult{}, err
	}
	return result, nil
}
