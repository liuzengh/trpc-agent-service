package domain

import (
	"errors"
	"time"
)

var ErrReplyNotFound = errors.New("admission reply snapshot not found")

// ReplySnapshot is an Admission-owned read model. It never resolves current
// routing, changes the original recipient, or authorizes a Worker Attempt.
type ReplySnapshot struct {
	AdmissionID, RunID, TenantID, Provider, AccountID, ManifestDigest           string
	ConversationID, ThreadID, SourceMessageID, SourceEventID, CallbackRequestID string
	ReceivedAt                                                                  time.Time
	Origin                                                                      *ReplyOrigin
}
