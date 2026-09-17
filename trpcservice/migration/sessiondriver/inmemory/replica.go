package inmemory

import (
	"context"
	"sort"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

type Replica struct {
	mu        sync.Mutex
	images    map[sessionstore.SessionKey]sessiondriver.SessionImage
	mutations map[string]sessiondriver.ApplyRequest
}

func NewReplica() *Replica {
	return &Replica{images: make(map[sessionstore.SessionKey]sessiondriver.SessionImage),
		mutations: make(map[string]sessiondriver.ApplyRequest)}
}

func (r *Replica) ApplySessionSnapshot(ctx context.Context, in sessiondriver.ApplyRequest) (sessiondriver.ApplyResult, error) {
	if err := ctx.Err(); err != nil {
		return sessiondriver.ApplyResult{}, err
	}
	if in.TenantID == "" || in.MigrationID == "" || in.MutationID == "" || in.Epoch < 1 ||
		in.Image.Head.TenantID != in.TenantID {
		return sessiondriver.ApplyResult{}, runtime.ErrTenantScope
	}
	digest, err := sessiondriver.SnapshotDigest(in.Image)
	if err != nil || digest != in.SnapshotDigest {
		return sessiondriver.ApplyResult{}, runtime.ErrInvariantViolation
	}
	key := applyKey(in)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.mutations[key]; ok {
		if !sameApply(existing, in) {
			return sessiondriver.ApplyResult{}, runtime.ErrIdempotencyCollision
		}
		return imageResult(r.images[in.Image.Head.SessionKey])
	}
	current, exists := r.images[in.Image.Head.SessionKey]
	if exists && current.Head.Version == in.Image.Head.Version {
		currentDigest, digestErr := sessiondriver.SnapshotDigest(current)
		if digestErr != nil || currentDigest != in.SnapshotDigest {
			return sessiondriver.ApplyResult{}, runtime.ErrVersionConflict
		}
	}
	if !exists || current.Head.Version < in.Image.Head.Version {
		r.images[in.Image.Head.SessionKey] = cloneImage(in.Image)
	}
	r.mutations[key] = cloneApply(in)
	return imageResult(r.images[in.Image.Head.SessionKey])
}

func (r *Replica) LoadSessionImage(ctx context.Context, key sessionstore.SessionKey) (sessiondriver.SessionImage, error) {
	if err := ctx.Err(); err != nil {
		return sessiondriver.SessionImage{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	image, ok := r.images[key]
	if !ok {
		return sessiondriver.SessionImage{}, runtime.ErrNotFound
	}
	return cloneImage(image), nil
}

func (r *Replica) Fingerprint(ctx context.Context, tenantID, watermark string) (sessiondriver.Fingerprint, error) {
	if err := ctx.Err(); err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	if tenantID == "" {
		return sessiondriver.Fingerprint{}, runtime.ErrTenantScope
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if watermark == "" {
		var maximum sessionstore.SessionKey
		for key := range r.images {
			if key.TenantID == tenantID && (key.AgentAppID > maximum.AgentAppID ||
				(key.AgentAppID == maximum.AgentAppID && key.SessionID > maximum.SessionID)) {
				maximum = key
			}
		}
		var err error
		watermark, err = sessiondriver.EncodeSessionWatermark(maximum.AgentAppID, maximum.SessionID, maximum.AgentAppID == "")
		if err != nil {
			return sessiondriver.Fingerprint{}, err
		}
	}
	upperAgent, upperSession, empty, err := sessiondriver.DecodeSessionWatermark(watermark)
	if err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	images := make([]sessiondriver.SessionImage, 0)
	for key, image := range r.images {
		if key.TenantID == tenantID && !empty &&
			(key.AgentAppID < upperAgent || (key.AgentAppID == upperAgent && key.SessionID <= upperSession)) {
			images = append(images, cloneImage(image))
		}
	}
	sort.Slice(images, func(i, j int) bool {
		if images[i].Head.AgentAppID == images[j].Head.AgentAppID {
			return images[i].Head.SessionID < images[j].Head.SessionID
		}
		return images[i].Head.AgentAppID < images[j].Head.AgentAppID
	})
	return fingerprintImages(images, watermark)
}

func fingerprintImages(images []sessiondriver.SessionImage, watermark string) (sessiondriver.Fingerprint, error) {
	items := make([]string, 0, len(images))
	for _, image := range images {
		digest, err := sessiondriver.SnapshotDigest(image)
		if err != nil {
			return sessiondriver.Fingerprint{}, err
		}
		items = append(items, image.Head.AgentAppID+"\x00"+image.Head.SessionID+"\x00"+digest)
	}
	return sessiondriver.FingerprintFromItems(items, watermark), nil
}

func applyKey(in sessiondriver.ApplyRequest) string {
	return in.TenantID + "\x00" + in.MigrationID + "\x00" + in.Image.Head.AgentAppID + "\x00" +
		in.Image.Head.SessionID + "\x00" + in.MutationID
}

func sameApply(left, right sessiondriver.ApplyRequest) bool {
	return left.TenantID == right.TenantID && left.MigrationID == right.MigrationID &&
		left.MutationID == right.MutationID && left.Epoch == right.Epoch &&
		left.Image.Head.SessionKey == right.Image.Head.SessionKey &&
		left.Image.Head.Version == right.Image.Head.Version && left.SnapshotDigest == right.SnapshotDigest
}

func imageResult(image sessiondriver.SessionImage) (sessiondriver.ApplyResult, error) {
	digest, err := sessiondriver.SnapshotDigest(image)
	return sessiondriver.ApplyResult{SessionVersion: image.Head.Version, SnapshotDigest: digest}, err
}

func cloneApply(in sessiondriver.ApplyRequest) sessiondriver.ApplyRequest {
	out := in
	out.Image = cloneImage(in.Image)
	return out
}

func cloneImage(in sessiondriver.SessionImage) sessiondriver.SessionImage {
	out := in
	out.Head.State = make(map[string]any, len(in.Head.State))
	for key, value := range in.Head.State {
		out.Head.State[key] = value
	}
	if in.SDK != nil {
		sdk := *in.SDK
		sdk.State = append([]byte(nil), in.SDK.State...)
		sdk.Events = append([]sessiondriver.SDKEventRecord(nil), in.SDK.Events...)
		for index := range sdk.Events {
			sdk.Events[index].Event = append([]byte(nil), in.SDK.Events[index].Event...)
		}
		sdk.TrackEvents = append([]sessiondriver.SDKTrackEventRecord(nil), in.SDK.TrackEvents...)
		for index := range sdk.TrackEvents {
			sdk.TrackEvents[index].Event = append([]byte(nil), in.SDK.TrackEvents[index].Event...)
		}
		sdk.Summaries = append([]sessiondriver.SDKSummaryRecord(nil), in.SDK.Summaries...)
		for index := range sdk.Summaries {
			sdk.Summaries[index].Summary = append([]byte(nil), in.SDK.Summaries[index].Summary...)
		}
		out.SDK = &sdk
	}
	out.Commits = append([]sessiondriver.CommitRecord(nil), in.Commits...)
	return out
}

var _ sessiondriver.ReplicaWriter = (*Replica)(nil)
var _ sessiondriver.SnapshotReader = (*Replica)(nil)
var _ sessiondriver.Inventory = (*Replica)(nil)
