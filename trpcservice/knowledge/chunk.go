package knowledge

import (
	"fmt"
	"strings"
)

// Chunking is deterministic by construction: fixed window, fixed overlap,
// derived pages. The approved plan's numbers (800 runes, 120 overlap) live
// here as the single source; the chunker version is stored on every document
// so a future change is a re-index, not a silent mix of two chunk shapes.
const (
	WindowRunes   = 800
	OverlapRunes  = 120
	ChunksPerPage = 8 // text formats have no pages; this derives one, documented and stable
)

// Chunk is one window of a document with its locator.
type Chunk struct {
	Ord  int
	Page int
	Text string
}

// ChunkText window-slides over runes (not bytes) so a CJK document is cut on
// character boundaries, and emits pages as ord/ChunksPerPage+1 — a derived
// locator for formats that have none, and the same field a future PDF parser
// will fill with real page numbers.
func ChunkText(text string, window, overlap int) ([]Chunk, error) {
	if window <= 0 || overlap < 0 || overlap >= window {
		return nil, fmt.Errorf("knowledge: window %d and overlap %d are not a usable sliding window", window, overlap)
	}
	runes := []rune(strings.TrimSpace(text))
	if len(runes) == 0 {
		return nil, fmt.Errorf("knowledge: refusing to index an empty document")
	}
	var out []Chunk
	step := window - overlap
	for start, ord := 0, 0; start < len(runes); start, ord = start+step, ord+1 {
		end := start + window
		if end > len(runes) {
			end = len(runes)
		}
		out = append(out, Chunk{
			Ord:  ord,
			Page: ord/ChunksPerPage + 1,
			Text: string(runes[start:end]),
		})
		if end == len(runes) {
			break
		}
	}
	return out, nil
}

// PointID is the deterministic vector id: tenant, document, generation,
// ordinal and embedding version fully identify one point, so a retried index
// job upserts over itself instead of creating twins. The digest is rendered
// as a UUID because that is the id form Qdrant accepts everywhere.
func PointID(tenantID string, docID int64, generation, ord, embeddingVersion int) string {
	return hashedUUID(fmt.Sprintf("doc|%s|%d|%d|%d|%d", tenantID, docID, generation, ord, embeddingVersion))
}

// MemoryPointID is PointID's sibling for the memory index.
func MemoryPointID(tenantID string, memoryID int64, embeddingVersion int) string {
	return hashedUUID(fmt.Sprintf("mem|%s|%d|%d", tenantID, memoryID, embeddingVersion))
}
