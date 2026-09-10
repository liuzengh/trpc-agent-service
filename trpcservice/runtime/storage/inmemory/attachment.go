package inmemory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

type storedAttachment struct {
	reference attachment.Reference
	eventID   string
	expiresAt time.Time
}

// PutAttachment stores exactly one bounded attachment and its unbound lifecycle record.
func (s *Store) PutAttachment(ctx context.Context, tenantID string, upload attachment.Upload, content io.Reader) (attachment.Reference, error) {
	if err := check(ctx); err != nil {
		return attachment.Reference{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || content == nil {
		return attachment.Reference{}, runtimestorage.ErrInvalid
	}
	normalized, err := upload.Normalize(time.Now().UTC())
	if err != nil {
		return attachment.Reference{}, err
	}
	data, err := io.ReadAll(io.LimitReader(content, normalized.Size+1))
	if err != nil {
		return attachment.Reference{}, runtimestorage.ErrStorage
	}
	if int64(len(data)) != normalized.Size {
		return attachment.Reference{}, attachment.ErrInvalid
	}
	digest := sha256.Sum256(data)
	reference := attachment.Reference{ID: normalized.ID, Kind: normalized.Kind, MIMEType: normalized.MIMEType, Name: normalized.Name, Size: normalized.Size, SHA256: hex.EncodeToString(digest[:]), Provider: normalized.Provider, ProviderID: normalized.ProviderID}
	if _, err := reference.Normalize(); err != nil {
		return attachment.Reference{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := key(tenantID, reference.ID)
	if existing, ok := s.attachments[key]; ok {
		if existing.reference != reference {
			return attachment.Reference{}, runtimestorage.ErrConflict
		}
		return existing.reference, nil
	}
	if object, ok := s.objects[key]; ok && (object.ContentType != reference.MIMEType || object.Size != reference.Size || object.ETag != reference.SHA256) {
		return attachment.Reference{}, runtimestorage.ErrConflict
	}
	s.objectData[key] = append([]byte(nil), data...)
	s.objects[key] = runtimestorage.ObjectInfo{TenantID: tenantID, ObjectKey: reference.ID, ContentType: reference.MIMEType, Size: reference.Size, ETag: reference.SHA256, CreatedAt: time.Now().UTC()}
	s.attachments[key] = storedAttachment{reference: reference, expiresAt: normalized.ExpiresAt}
	return reference, nil
}

// BindAttachments associates unexpired attachment references with an existing event.
func (s *Store) BindAttachments(ctx context.Context, tenantID, eventID string, references []attachment.Reference) error {
	if err := check(ctx); err != nil {
		return err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || eventID == "" {
		return runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.events[key(tenantID, eventID)]; !ok {
		return runtimestorage.ErrNotFound
	}
	now := time.Now().UTC()
	for _, reference := range references {
		normalized, err := reference.Normalize()
		if err != nil {
			return err
		}
		stored, ok := s.attachments[key(tenantID, normalized.ID)]
		if !ok {
			return runtimestorage.ErrNotFound
		}
		if stored.reference != normalized || !stored.expiresAt.After(now) || stored.eventID != "" && stored.eventID != eventID {
			return runtimestorage.ErrConflict
		}
	}
	for _, reference := range references {
		stored := s.attachments[key(tenantID, reference.ID)]
		stored.eventID = eventID
		s.attachments[key(tenantID, reference.ID)] = stored
	}
	return nil
}

// Load returns attachment data only when its reference belongs to eventID.
func (s *Store) Load(ctx context.Context, tenantID, eventID string, reference attachment.Reference) (attachment.Content, error) {
	if err := check(ctx); err != nil {
		return attachment.Content{}, err
	}
	normalized, err := reference.Normalize()
	if err != nil {
		return attachment.Content{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || eventID == "" {
		return attachment.Content{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	stored, ok := s.attachments[key(tenantID, normalized.ID)]
	data := append([]byte(nil), s.objectData[key(tenantID, normalized.ID)]...)
	s.mu.RUnlock()
	if !ok || stored.reference != normalized || stored.eventID != eventID || !stored.expiresAt.After(time.Now().UTC()) {
		return attachment.Content{}, runtimestorage.ErrNotFound
	}
	result := attachment.Content{Data: data}
	if err := result.Validate(normalized); err != nil {
		return attachment.Content{}, err
	}
	return result, nil
}

// CleanupAttachments removes expired unbound attachments and attachments whose
// bound message has reached a terminal state.
func (s *Store) CleanupAttachments(ctx context.Context, tenantID string, before time.Time) (int, error) {
	if err := check(ctx); err != nil {
		return 0, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || before.IsZero() {
		return 0, runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for storageKey, value := range s.attachments {
		event, found := s.events[key(tenantID, value.eventID)]
		terminal := value.eventID == "" || found && (event.Status == runtimestorage.EventCompleted || event.Status == runtimestorage.EventFailed)
		if terminal && !value.expiresAt.After(before) && strings.HasPrefix(storageKey, key(tenantID)) {
			delete(s.attachments, storageKey)
			delete(s.objects, storageKey)
			delete(s.objectData, storageKey)
			removed++
		}
	}
	return removed, nil
}

var _ runtimestorage.AttachmentStore = (*Store)(nil)
