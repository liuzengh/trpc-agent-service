package sessionstore

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

// CredentialDSN combines the immutable published destination with Control's
// password-only credential value. A value that looks like a URI is still only a
// password: it cannot override host, port, database, role, TLS mode or schema.
func CredentialDSN(target Target, password string) (string, error) {
	if target.Host == "" || len(target.Host) > 253 || !utf8.ValidString(target.Host) || strings.ContainsAny(target.Host, ",/\\ \t\r\n\x00@?#%[]") ||
		(strings.Contains(target.Host, ":") && net.ParseIP(target.Host) == nil) || target.Port == 0 ||
		target.Username != "session_runtime" || target.Database == "" || len(target.Database) > 128 || !utf8.ValidString(target.Database) || strings.ContainsAny(target.Database, "/\x00\r\n") ||
		(target.SSLMode != "disable" && target.SSLMode != "require" && target.SSLMode != "verify-full") ||
		!utf8.ValidString(password) || strings.TrimSpace(password) == "" || strings.ContainsAny(password, "\x00\r\n") {
		return "", ErrIdentity
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(target.Username, password),
		Host:     net.JoinHostPort(target.Host, strconv.Itoa(int(target.Port))),
		Path:     "/" + target.Database,
		RawQuery: url.Values{"sslmode": {target.SSLMode}}.Encode(),
	}
	return u.String(), nil
}
