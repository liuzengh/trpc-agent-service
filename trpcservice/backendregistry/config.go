// Package backendregistry manages named, tenant-scoped storage connections.
package backendregistry

import (
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

var ErrInvalid = errors.New("存储连接配置无效，请检查资源类型、地址、端口和必填字段")

// Settings contains public connection fields only. Passwords never enter it.
type Settings struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Database   string `json:"database"`
	Username   string `json:"username"`
	TLS        bool   `json:"tls"`
	SSLMode    string `json:"ssl_mode"`
	KeyPrefix  string `json:"key_prefix"`
	Schema     string `json:"schema"`
	TableName  string `json:"table_name"`
	Collection string `json:"collection"`
	Dimensions int    `json:"dimensions"`
	Endpoint   string `json:"endpoint"`
	Region     string `json:"region"`
	Bucket     string `json:"bucket"`
	PathStyle  bool   `json:"path_style"`
}

type Secrets struct {
	Password        string `json:"password"`
	APIKey          string `json:"api_key"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func (s Secrets) Empty() bool { return s == (Secrets{}) }

// Build performs local validation only; saving a connection never dials it.
// The returned secret is stored in the existing encrypted credential vault.
func Build(resource, kind string, settings Settings, secrets Secrets, existingRef string) (json.RawMessage, string, error) {
	for _, v := range []string{settings.Host, settings.Database, settings.Username, settings.KeyPrefix, settings.Schema, settings.TableName, settings.Collection, settings.Endpoint, settings.Region, settings.Bucket, secrets.Password, secrets.APIKey, secrets.AccessKeyID, secrets.SecretAccessKey} {
		if len(v) > 16384 || strings.ContainsAny(v, "\r\n\x00") {
			return nil, "", ErrInvalid
		}
	}
	if existingRef != "" && !secrets.Empty() {
		return nil, "", ErrInvalid
	}
	config := map[string]any{}
	value := ""
	if kind == "inmemory" {
		if existingRef != "" || !secrets.Empty() {
			return nil, "", ErrInvalid
		}
	} else {
		switch kind {
		case "redis", "postgres":
			if resource != "session" && resource != "memory" {
				return nil, "", ErrInvalid
			}
			if existingRef == "" {
				if !validHost(settings.Host) || settings.Port < 1 || settings.Port > 65535 || secrets.APIKey != "" || secrets.AccessKeyID != "" || secrets.SecretAccessKey != "" {
					return nil, "", ErrInvalid
				}
				u := url.URL{Host: net.JoinHostPort(settings.Host, strconv.Itoa(settings.Port))}
				if settings.Username != "" || secrets.Password != "" {
					u.User = url.UserPassword(settings.Username, secrets.Password)
				}
				if kind == "redis" {
					db, err := strconv.Atoi(settings.Database)
					if err != nil || db < 0 || db > 2147483647 {
						return nil, "", ErrInvalid
					}
					u.Scheme = "redis"
					if settings.TLS {
						u.Scheme = "rediss"
					}
					u.Path = "/" + strconv.Itoa(db)
				} else {
					if settings.Database == "" || settings.Username == "" {
						return nil, "", ErrInvalid
					}
					switch settings.SSLMode {
					case "disable", "require", "verify-ca", "verify-full":
					default:
						return nil, "", ErrInvalid
					}
					u.Scheme = "postgres"
					u.Path = "/" + settings.Database
					q := url.Values{"sslmode": {settings.SSLMode}}
					u.RawQuery = q.Encode()
				}
				value = u.String()
			}
			if kind == "redis" {
				config["key_prefix"] = settings.KeyPrefix
			} else if resource == "memory" {
				config["schema"] = settings.Schema
				config["table_name"] = settings.TableName
			} else {
				config["table_prefix"] = settings.TableName
			}
		case "qdrant":
			if resource != "knowledge" || !validHost(settings.Host) || settings.Port < 1 || settings.Port > 65535 || settings.Collection == "" || settings.Dimensions < 1 || settings.Dimensions > 65536 || secrets.Password != "" || secrets.AccessKeyID != "" || secrets.SecretAccessKey != "" {
				return nil, "", ErrInvalid
			}
			config = map[string]any{"host": settings.Host, "port": settings.Port, "tls": settings.TLS, "collection_name": settings.Collection, "dimensions": settings.Dimensions}
			value = secrets.APIKey
		case "s3":
			if resource != "artifact" || settings.Bucket == "" || secrets.Password != "" || secrets.APIKey != "" {
				return nil, "", ErrInvalid
			}
			if settings.Endpoint != "" {
				u, err := url.Parse(settings.Endpoint)
				if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
					return nil, "", ErrInvalid
				}
			}
			if existingRef == "" {
				if secrets.AccessKeyID == "" || secrets.SecretAccessKey == "" {
					return nil, "", ErrInvalid
				}
				raw, _ := json.Marshal(map[string]string{"access_key_id": secrets.AccessKeyID, "secret_access_key": secrets.SecretAccessKey})
				value = string(raw)
			}
			config = map[string]any{"bucket": settings.Bucket, "endpoint": settings.Endpoint, "region": settings.Region, "path_style": settings.PathStyle}
		default:
			return nil, "", ErrInvalid
		}
	}
	raw, _ := json.Marshal(config)
	ref := existingRef
	if value != "" {
		ref = "managed://pending"
	}
	if err := platformstorage.ValidateBackendBindingConfig(controlplane.BackendBinding{ResourceType: resource, BackendType: kind, Config: raw, SecretRef: ref}); err != nil {
		return nil, "", ErrInvalid
	}
	return raw, value, nil
}

func validHost(host string) bool {
	return host != "" && strings.TrimSpace(host) == host && len(host) <= 253 && !strings.ContainsAny(host, "/\\?#@% \t") && (net.ParseIP(host) != nil || !strings.Contains(host, ":"))
}
