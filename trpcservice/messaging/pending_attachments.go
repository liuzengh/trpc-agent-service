package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

const (
	PendingAttachmentTTL      = 10 * time.Minute
	MaxPendingAttachmentFiles = 8
	MaxPendingAttachmentBytes = int64(16 << 20)
)

var ErrPendingAttachmentLimit = errors.New("pending attachment limit exceeded")

type PendingAttachmentKey struct {
	TenantID   string
	AppCode    string
	Channel    channels.Channel
	BindingID  string
	SessionKey string
	SenderID   string
}

func (k PendingAttachmentKey) Validate() error {
	if strings.TrimSpace(k.TenantID) == "" || strings.TrimSpace(k.AppCode) == "" ||
		!k.Channel.Supported() || strings.TrimSpace(k.BindingID) == "" ||
		strings.TrimSpace(k.SessionKey) == "" || strings.TrimSpace(k.SenderID) == "" {
		return errors.New("pending attachment key is incomplete")
	}
	return nil
}

type PendingAttachmentBatch struct {
	MessageID  string                 `json:"message_id"`
	ReceivedAt time.Time              `json:"received_at"`
	Files      []channels.InboundFile `json:"files"`
}

type PendingAttachmentStore interface {
	Append(context.Context, PendingAttachmentKey, PendingAttachmentBatch) error
	Drain(context.Context, PendingAttachmentKey) ([]PendingAttachmentBatch, error)
	Restore(context.Context, PendingAttachmentKey, []PendingAttachmentBatch) error
}

type RedisPendingAttachmentStore struct {
	client redis.UniversalClient
}

func NewRedisPendingAttachmentStore(client redis.UniversalClient) (*RedisPendingAttachmentStore, error) {
	if client == nil {
		return nil, errors.New("pending attachment Redis client is required")
	}
	return &RedisPendingAttachmentStore{client: client}, nil
}

var appendPendingAttachmentScript = redis.NewScript(`
local field = 'b:' .. ARGV[1]
local count_field = 'c:' .. ARGV[1]
local bytes_field = 's:' .. ARGV[1]
local old_count = tonumber(redis.call('HGET', KEYS[1], count_field) or '0')
local old_bytes = tonumber(redis.call('HGET', KEYS[1], bytes_field) or '0')
local total_count = tonumber(redis.call('HGET', KEYS[1], '_count') or '0') - old_count + tonumber(ARGV[3])
local total_bytes = tonumber(redis.call('HGET', KEYS[1], '_bytes') or '0') - old_bytes + tonumber(ARGV[4])
if total_count > tonumber(ARGV[6]) or total_bytes > tonumber(ARGV[7]) then
  return 0
end
redis.call('HSET', KEYS[1], field, ARGV[2], count_field, ARGV[3], bytes_field, ARGV[4], '_count', total_count, '_bytes', total_bytes)
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1
`)

func (s *RedisPendingAttachmentStore) Append(ctx context.Context, key PendingAttachmentKey, batch PendingAttachmentBatch) error {
	if err := key.Validate(); err != nil {
		return err
	}
	batch.MessageID = strings.TrimSpace(batch.MessageID)
	if batch.MessageID == "" || len(batch.Files) == 0 {
		return errors.New("pending attachment batch is empty")
	}
	if batch.ReceivedAt.IsZero() {
		batch.ReceivedAt = time.Now().UTC()
	} else {
		batch.ReceivedAt = batch.ReceivedAt.UTC()
	}
	var totalBytes int64
	for _, file := range batch.Files {
		if strings.TrimSpace(file.Name) == "" || strings.TrimSpace(file.ArtifactName) == "" || file.Version < 0 || file.SizeBytes < 0 {
			return errors.New("pending attachment batch contains an invalid file")
		}
		totalBytes += file.SizeBytes
	}
	encoded, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("encode pending attachment batch: %w", err)
	}
	result, err := appendPendingAttachmentScript.Run(ctx, s.client, []string{pendingAttachmentRedisKey(key)},
		batch.MessageID, string(encoded), len(batch.Files), totalBytes, PendingAttachmentTTL.Milliseconds(), MaxPendingAttachmentFiles, MaxPendingAttachmentBytes,
	).Int64()
	if err != nil {
		return fmt.Errorf("append pending attachments: %w", err)
	}
	if result != 1 {
		return ErrPendingAttachmentLimit
	}
	return nil
}

var drainPendingAttachmentsScript = redis.NewScript(`
local values = redis.call('HGETALL', KEYS[1])
if #values > 0 then redis.call('DEL', KEYS[1]) end
return values
`)

func (s *RedisPendingAttachmentStore) Drain(ctx context.Context, key PendingAttachmentKey) ([]PendingAttachmentBatch, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	values, err := drainPendingAttachmentsScript.Run(ctx, s.client, []string{pendingAttachmentRedisKey(key)}).StringSlice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("drain pending attachments: %w", err)
	}
	batches := make([]PendingAttachmentBatch, 0, len(values)/6)
	for index := 0; index+1 < len(values); index += 2 {
		if !strings.HasPrefix(values[index], "b:") {
			continue
		}
		var batch PendingAttachmentBatch
		if err := json.Unmarshal([]byte(values[index+1]), &batch); err != nil {
			return nil, fmt.Errorf("decode pending attachment batch: %w", err)
		}
		batches = append(batches, batch)
	}
	sort.Slice(batches, func(a, b int) bool {
		if batches[a].ReceivedAt.Equal(batches[b].ReceivedAt) {
			return batches[a].MessageID < batches[b].MessageID
		}
		return batches[a].ReceivedAt.Before(batches[b].ReceivedAt)
	})
	return batches, nil
}

func (s *RedisPendingAttachmentStore) Restore(ctx context.Context, key PendingAttachmentKey, batches []PendingAttachmentBatch) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if len(batches) == 0 {
		return nil
	}
	args := make([]any, 0, 1+len(batches)*4)
	args = append(args, PendingAttachmentTTL.Milliseconds())
	for _, batch := range batches {
		batch.MessageID = strings.TrimSpace(batch.MessageID)
		if batch.MessageID == "" || len(batch.Files) == 0 {
			return errors.New("pending attachment batch is empty")
		}
		encoded, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("encode restored pending attachment batch: %w", err)
		}
		var totalBytes int64
		for _, file := range batch.Files {
			totalBytes += file.SizeBytes
		}
		args = append(args, batch.MessageID, string(encoded), len(batch.Files), totalBytes)
	}
	if _, err := restorePendingAttachmentsScript.Run(ctx, s.client, []string{pendingAttachmentRedisKey(key)}, args...).Result(); err != nil {
		return fmt.Errorf("restore pending attachments: %w", err)
	}
	return nil
}

var restorePendingAttachmentsScript = redis.NewScript(`
for i = 2, #ARGV, 4 do
  local id = ARGV[i]
  redis.call('HSET', KEYS[1], 'b:' .. id, ARGV[i + 1], 'c:' .. id, ARGV[i + 2], 's:' .. id, ARGV[i + 3])
end
local values = redis.call('HGETALL', KEYS[1])
local total_count = 0
local total_bytes = 0
for i = 1, #values, 2 do
  local field = values[i]
  if string.sub(field, 1, 2) == 'c:' then total_count = total_count + tonumber(values[i + 1]) end
  if string.sub(field, 1, 2) == 's:' then total_bytes = total_bytes + tonumber(values[i + 1]) end
end
redis.call('HSET', KEYS[1], '_count', total_count, '_bytes', total_bytes)
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
`)

func PendingAttachmentFiles(batches []PendingAttachmentBatch) []channels.InboundFile {
	count := 0
	for _, batch := range batches {
		count += len(batch.Files)
	}
	files := make([]channels.InboundFile, 0, count)
	for _, batch := range batches {
		files = append(files, batch.Files...)
	}
	return files
}

func ValidatePendingAttachmentFiles(files []channels.InboundFile) error {
	if len(files) > MaxPendingAttachmentFiles {
		return ErrPendingAttachmentLimit
	}
	var totalBytes int64
	for _, file := range files {
		totalBytes += file.SizeBytes
	}
	if totalBytes > MaxPendingAttachmentBytes {
		return ErrPendingAttachmentLimit
	}
	return nil
}

func pendingAttachmentRedisKey(key PendingAttachmentKey) string {
	raw := strings.Join([]string{
		strings.TrimSpace(key.TenantID), strings.TrimSpace(key.AppCode), string(key.Channel), strings.TrimSpace(key.BindingID),
		strings.TrimSpace(key.SessionKey), strings.TrimSpace(key.SenderID),
	}, "\x00")
	digest := sha256.Sum256([]byte(raw))
	return "trpc:pending-attachments:" + base64.RawURLEncoding.EncodeToString(digest[:])
}
