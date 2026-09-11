// Deterministic embeddings for the fake model.
//
// The algorithm is a bag-of-token hashing vectorizer: each token maps to one
// dimension (FNV-1a), counts accumulate, the vector is L2-normalised. That
// makes it deterministic and — unlike a pure per-text hash — *lexically
// meaningful*: a query sharing a token with a document scores a high cosine,
// so the knowledge pipeline's citations can be asserted on real retrieval
// rather than on a fixture that ignores the query.
//
// CJK text has no spaces, so runs of CJK runes are emitted one rune at a
// time; overlap between a Chinese query and a Chinese document therefore
// shows up at the character level.
package main

import (
	"encoding/json"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"strings"
	"unicode"
)

const defaultEmbeddingDim = 64

type embeddingsRequest struct {
	Model      string   `json:"model"`
	Input      []string `json:"input"`
	Dimensions int      `json:"dimensions"`
}

// handleEmbeddings is registered explicitly before the completion
// catch-all: without a route, /v1/embeddings would be answered as a chat
// completion (the same trap the KF and tool stubs each hit once).
func (s *server) handleEmbeddings(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST an embeddings request here")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	var req embeddingsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "decode embeddings request: "+err.Error())
		return
	}
	if len(req.Input) == 0 {
		writeErr(w, http.StatusBadRequest, "embeddings request carries no input")
		return
	}
	dim := req.Dimensions
	if dim <= 0 {
		dim = defaultEmbeddingDim
	}
	if dim > 4096 {
		writeErr(w, http.StatusBadRequest, "dimensions above 4096 are not supported")
		return
	}
	data := make([]map[string]any, 0, len(req.Input))
	for i, text := range req.Input {
		data = append(data, map[string]any{
			"object":    "embedding",
			"index":     i,
			"embedding": deterministicEmbedding(text, dim),
		})
	}
	s.embeds.Add(1)
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"model":  req.Model,
		"data":   data,
		"usage":  map[string]int{"prompt_tokens": 0, "total_tokens": 0},
	})
}

func deterministicEmbedding(text string, dim int) []float32 {
	v := make([]float32, dim)
	for _, token := range tokenize(text) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(token))
		v[h.Sum32()%uint32(dim)]++
	}
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm > 0 {
		n := float32(math.Sqrt(norm))
		for i := range v {
			v[i] /= n
		}
	}
	return v
}

// tokenize splits ASCII words and emits CJK runes individually, lowering
// everything first.
func tokenize(text string) []string {
	text = strings.ToLower(text)
	var (
		out     []string
		current strings.Builder
	)
	flush := func() {
		if current.Len() > 0 {
			out = append(out, current.String())
			current.Reset()
		}
	}
	for _, r := range text {
		switch {
		case r >= 0x2E80 && r <= 0x9FFF || r >= 0xF900 && r <= 0xFAFF: // CJK blocks
			flush()
			out = append(out, string(r))
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			current.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	return out
}
