package redis

import (
	"fmt"
	"strings"
)

func keyPart(name, value string) error {
	if value == "" || len(value) > 256 || strings.ContainsAny(value, ":{}\x00\r\n") {
		return fmt.Errorf("redis: invalid %s", name)
	}
	return nil
}

func key(prefix, kind string, parts ...string) (string, error) {
	if prefix == "" || kind == "" {
		return "", fmt.Errorf("redis: invalid key prefix or kind")
	}
	for i, p := range parts {
		if err := keyPart(fmt.Sprintf("key part %d", i), p); err != nil {
			return "", err
		}
	}
	return prefix + ":" + kind + ":" + strings.Join(parts, ":"), nil
}

func dedupKey(prefix, tenant, channel, binding, message string) (string, error) {
	return key(prefix, "dedup", tenant, channel, binding, message)
}
func sessionLeaseKey(prefix, tenant, session string) (string, error) {
	return key(prefix, "session-lease", tenant, session)
}
func fenceKey(prefix, kind, tenant, resource string) (string, error) {
	return key(prefix, "fence", kind, tenant, resource)
}
