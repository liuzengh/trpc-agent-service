package storage

import (
	"context"
	"io"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
)

// AttachmentStore persists tenant-scoped attachment data and lifecycle metadata.
// An attachment is first stored, then bound to a durable message event by the
// Gateway before it can be read by a Runner.
type AttachmentStore interface {
	PutAttachment(context.Context, string, attachment.Upload, io.Reader) (attachment.Reference, error)
	Load(context.Context, string, string, attachment.Reference) (attachment.Content, error)
	BindAttachments(context.Context, string, string, []attachment.Reference) error
	CleanupAttachments(context.Context, string, time.Time) (int, error)
	Close() error
}
