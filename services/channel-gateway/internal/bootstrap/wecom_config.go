package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"unicode/utf8"

	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	connectiondomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

// accountFileSource is a temporary deployment-owned projection source, not the
// Control ChannelAccount management API. Revisions must be monotonic across all replicas.
type accountFileSource struct {
	path    string
	initial []connectiondomain.Account
}

func (s accountFileSource) List(ctx context.Context) ([]connectiondomain.Account, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.path == "" {
		return append([]connectiondomain.Account(nil), s.initial...), nil
	}
	f, err := os.Open(s.path)
	if err != nil {
		return nil, errors.New("open WeCom account projection failed")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(b) > 65536 {
		return nil, errors.New("WeCom account projection exceeds limit")
	}
	return decodeWeComAccounts(b)
}
func decodeWeComAccounts(b []byte) ([]connectiondomain.Account, error) {
	invalid := errors.New("invalid WeCom account projection")
	if !utf8.Valid(b) || len(b) > 65536 {
		return nil, invalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	tok, err := d.Token()
	if err != nil || tok != json.Delim('[') {
		return nil, invalid
	}
	out := make([]connectiondomain.Account, 0)
	ids, bots := map[string]bool{}, map[string]bool{}
	for d.More() {
		if len(out) >= 100 {
			return nil, invalid
		}
		tok, err = d.Token()
		if err != nil || tok != json.Delim('{') {
			return nil, invalid
		}
		var a connectiondomain.Account
		seen := map[string]bool{}
		for d.More() {
			tok, err = d.Token()
			if err != nil {
				return nil, invalid
			}
			key, ok := tok.(string)
			if !ok || seen[key] {
				return nil, invalid
			}
			seen[key] = true
			value, e := d.Token()
			if e != nil {
				return nil, invalid
			}
			switch key {
			case "account_id", "bot_id", "secret_env":
				v, ok := value.(string)
				if !ok {
					return nil, invalid
				}
				switch key {
				case "account_id":
					a.ID = v
				case "bot_id":
					a.BotID = v
				case "secret_env":
					a.CredentialRef = v
				}
			case "revision":
				v, ok := value.(json.Number)
				if !ok {
					return nil, invalid
				}
				a.Revision, e = strconv.ParseInt(v.String(), 10, 64)
				if e != nil {
					return nil, invalid
				}
			case "enabled":
				v, ok := value.(bool)
				if !ok {
					return nil, invalid
				}
				a.Enabled = v
			default:
				return nil, invalid
			}
		}
		tok, err = d.Token()
		if err != nil || tok != json.Delim('}') || len(seen) != 5 {
			return nil, invalid
		}
		if err = a.Validate(); err != nil || !secretEnvPattern.MatchString(a.CredentialRef) || ids[a.ID] || bots[a.BotID] {
			return nil, invalid
		}
		ids[a.ID], bots[a.BotID] = true, true
		out = append(out, a)
	}
	tok, err = d.Token()
	if err != nil || tok != json.Delim(']') {
		return nil, invalid
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, invalid
	}
	return out, nil
}

// Credential material is resolved only after ownership is acquired, never stored
// in account projections or borrowed from the Worker profile credential service.
type envWeComCredentials struct{}

func (envWeComCredentials) Resolve(ctx context.Context, a connectiondomain.Account) (connection.CredentialMaterial, error) {
	if err := ctx.Err(); err != nil {
		return connection.CredentialMaterial{}, err
	}
	if !secretEnvPattern.MatchString(a.CredentialRef) {
		return connection.CredentialMaterial{}, errors.New("invalid WeCom credential reference")
	}
	secret := os.Getenv(a.CredentialRef)
	if secret == "" {
		return connection.CredentialMaterial{}, errors.New("WeCom credential unavailable")
	}
	return connection.CredentialMaterial{Secret: secret}, nil
}
