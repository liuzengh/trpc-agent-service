package identity

import (
	"errors"
	"strings"
)

// validateBearerCredential refuses a static key that cannot be presented to
// this platform as the credential it is configured to be.
//
// A key is offered exactly one way: an Authorization header reading
// "Bearer <key>", parsed on arrival by trimming the header and then trimming
// the token. So a key that is only whitespace never survives that parse at all,
// and a key with whitespace at either end arrives as a shorter string than the
// one that was configured. Either way the digest lookup cannot match, and the
// deployment holds a credential that authenticates nobody. Length does not
// catch it: thirty-two spaces is a thirty-two character key.
//
// The rule is deliberately narrow. It is not RFC 6750 token68, which would
// refuse keys that are already in use and work perfectly — ':' and '|' are not
// token68 characters and appear in real API keys, and they travel and compare
// byte for byte. What is refused here is only what provably cannot work, so
// this check can never turn a working deployment away.
//
// Refusing at construction is the whole point. Every one of these keys loads
// today: the process starts, logs a configuration with the expected number of
// credentials, and then answers 401 to the operator holding the admin key it
// just reported. A startup error naming the role is the difference between a
// typo and an outage nobody can read.
func validateBearerCredential(key string) error {
	if key == "" {
		return errors.New("it is empty")
	}
	// Covers the all-whitespace key too, which trims to nothing.
	if strings.TrimSpace(key) != key {
		return errors.New("it begins or ends with whitespace, which is trimmed in transit")
	}
	for index := 0; index < len(key); index++ {
		if !validHeaderValueByte(key[index]) {
			return errors.New("it holds a byte that cannot be sent in an HTTP header")
		}
	}
	return nil
}

// validHeaderValueByte reports whether b may appear in an HTTP header field
// value: a horizontal tab, or any byte from space upward that is not DEL.
//
// That is RFC 9110's field-vchar plus SP and HTAB. Bytes at 0x80 and above are
// obs-text and stay allowed, because they do travel; what is ruled out is the
// set that does not — a CR or an LF would end the header line rather than sit
// in it, and a NUL or a DEL is refused by the client before the request is
// written. A key containing one of those is a key that never reaches this
// process, however carefully it was configured.
func validHeaderValueByte(b byte) bool {
	return b == '\t' || (b >= ' ' && b != 0x7f)
}
