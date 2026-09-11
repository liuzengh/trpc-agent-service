package modelusage

import (
	"context"
	"sort"
	"strings"
	"sync"
)

type Segment struct {
	ProviderID         string `json:"provider_id"`
	ModelName          string `json:"model_name"`
	ReportedModel      string `json:"reported_model,omitempty"`
	PromptTokens       int    `json:"prompt_tokens"`
	CachedPromptTokens int    `json:"cached_prompt_tokens"`
	CompletionTokens   int    `json:"completion_tokens"`
	TotalTokens        int    `json:"total_tokens"`
}

type Recorder struct {
	mu       sync.Mutex
	segments map[string]Segment
}

func NewRecorder() *Recorder { return &Recorder{segments: make(map[string]Segment)} }

func (r *Recorder) Record(segment Segment) {
	if r == nil {
		return
	}
	segment.ProviderID = strings.TrimSpace(segment.ProviderID)
	segment.ModelName = strings.TrimSpace(segment.ModelName)
	segment.ReportedModel = strings.TrimSpace(segment.ReportedModel)
	if segment.ProviderID == "" || segment.ModelName == "" || segment.PromptTokens < 0 || segment.CachedPromptTokens < 0 ||
		segment.CompletionTokens < 0 || segment.TotalTokens < 0 {
		return
	}
	if segment.CachedPromptTokens > segment.PromptTokens {
		segment.CachedPromptTokens = segment.PromptTokens
	}
	key := segment.ProviderID + "\x00" + segment.ModelName + "\x00" + segment.ReportedModel
	r.mu.Lock()
	current := r.segments[key]
	current.ProviderID, current.ModelName, current.ReportedModel = segment.ProviderID, segment.ModelName, segment.ReportedModel
	current.PromptTokens += segment.PromptTokens
	current.CachedPromptTokens += segment.CachedPromptTokens
	current.CompletionTokens += segment.CompletionTokens
	current.TotalTokens += segment.TotalTokens
	r.segments[key] = current
	r.mu.Unlock()
}

func (r *Recorder) Snapshot() []Segment {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	result := make([]Segment, 0, len(r.segments))
	for _, segment := range r.segments {
		result = append(result, segment)
	}
	r.mu.Unlock()
	sort.Slice(result, func(i, j int) bool {
		if result[i].ProviderID != result[j].ProviderID {
			return result[i].ProviderID < result[j].ProviderID
		}
		if result[i].ModelName != result[j].ModelName {
			return result[i].ModelName < result[j].ModelName
		}
		return result[i].ReportedModel < result[j].ReportedModel
	})
	return result
}

type recorderContextKey struct{}

func WithRecorder(ctx context.Context, recorder *Recorder) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if recorder == nil {
		return ctx
	}
	return context.WithValue(ctx, recorderContextKey{}, recorder)
}

func FromContext(ctx context.Context) (*Recorder, bool) {
	if ctx == nil {
		return nil, false
	}
	recorder, ok := ctx.Value(recorderContextKey{}).(*Recorder)
	return recorder, ok && recorder != nil
}
