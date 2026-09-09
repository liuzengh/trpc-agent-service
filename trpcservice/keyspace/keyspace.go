// Package keyspace defines the Redis namespaces shared by messaging and
// fenced Session implementations.
package keyspace

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"strings"
)

// CoordinationPrefix returns the namespace used by the reliable messaging
// state machine.
func CoordinationPrefix(prefix string) string {
	return strings.TrimRight(strings.TrimSpace(prefix), ":") + ":reliable-v1"
}

// FencedPrefix returns the platform-owned fenced Session namespace.
func FencedPrefix(prefix string) string {
	return CoordinationPrefix(prefix) + ":fenced-v1"
}

func SessionCoord(parts ...string) string { return DigestCoord(parts...) }
func UserCoord(parts ...string) string    { return DigestCoord(parts...) }

// DigestCoord uses length-prefixing so concatenated external values remain
// unambiguous before hashing.
func DigestCoord(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = h.Write(size[:])
		_, _ = io.WriteString(h, part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func FencedMeta(prefix, coord string) string {
	return FencedPrefix(prefix) + ":session:" + coord + ":meta"
}

func FencedEvents(prefix, coord string) string {
	return FencedPrefix(prefix) + ":session:" + coord + ":events"
}

func FencedUserIndex(prefix, userCoord string) string {
	return FencedPrefix(prefix) + ":user:" + userCoord
}

func SessionSequence(prefix, coord string) string {
	return CoordinationPrefix(prefix) + ":session:seq:" + coord
}

func SessionState(prefix, coord string) string {
	return CoordinationPrefix(prefix) + ":session:state:" + coord
}

func SessionLock(prefix, coord string) string {
	return CoordinationPrefix(prefix) + ":session:lock:" + coord
}

func SessionWait(prefix string) string {
	return CoordinationPrefix(prefix) + ":session:wait"
}

func Outbound(prefix, taskID string) string {
	return CoordinationPrefix(prefix) + ":outbound:" + taskID
}

func BackendFingerprint(prefix, tenantID, agentAppID string) string {
	return CoordinationPrefix(prefix) + ":backend:fingerprint:" + DigestCoord(tenantID, agentAppID)
}

func PersistenceRetry(prefix string) string {
	return CoordinationPrefix(prefix) + ":persistence:retry"
}
