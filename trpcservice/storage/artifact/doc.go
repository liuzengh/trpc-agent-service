// Package artifact hosts tenant-scoped immutable media artifacts.
package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Record struct {
	TenantID, RequestID, ArtifactID, ArtifactRef string
	Ordinal                                      int
	SourceDigest, ContentDigest, MediaType, Kind string
	Content                                      []byte
	MalwareScanVersion, DLPVersion               string
	CreatedAt                                    time.Time
}

type Store interface {
	PutArtifact(context.Context, Record) (Record, error)
	GetArtifact(context.Context, string, string) (Record, error)
}

func StableIdentity(tenantID, requestID string, ordinal int, sourceDigest string) (string, string, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(requestID) == "" || ordinal < 0 ||
		len(sourceDigest) != sha256.Size*2 {
		return "", "", runtime.ErrInvalidEnvelope
	}
	if _, err := hex.DecodeString(sourceDigest); err != nil {
		return "", "", runtime.ErrInvalidEnvelope
	}
	sum := sha256.Sum256([]byte(tenantID + "\x00" + requestID + "\x00" + strconv.Itoa(ordinal) + "\x00" + sourceDigest))
	id := "a1_" + base64.RawURLEncoding.EncodeToString(sum[:])
	return id, "artifact://" + tenantID + "/" + id, nil
}
