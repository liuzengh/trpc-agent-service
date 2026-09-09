package sessionlease

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
)

// DigestVersion is mixed into every key digest. Bump it when the field set or
// the encoding below changes: an old and a new build would otherwise disagree
// about which lock protects a Session while both are running.
const DigestVersion = "sessionlease/v1"

// KeyDigest returns the stable, opaque name of a lease scope.
//
// Backends that put lease state in a shared store name it by this digest rather
// than by the key fields, so tenant identifiers, principal identifiers and
// session identifiers never appear in a key an operator, a keyspace scan or a
// slow-log line can read. The digest is one-way; it is not an encryption of the
// key and it is not reversible into one.
//
// Fields are length-prefixed before hashing so that no two distinct keys can
// produce the same byte stream by moving a separator across a field boundary.
func KeyDigest(key sessiondir.Key) string {
	sum := sha256.New()
	writeDigestField(sum, DigestVersion)
	writeDigestField(sum, key.TenantID)
	writeDigestField(sum, key.AppID)
	writeDigestField(sum, key.PrincipalID)
	writeDigestField(sum, key.SessionID)
	var epoch [4]byte
	binary.BigEndian.PutUint32(epoch[:], key.Epoch)
	sum.Write(epoch[:])
	return hex.EncodeToString(sum.Sum(nil))
}

func writeDigestField(sum hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	sum.Write(length[:])
	sum.Write([]byte(value))
}
