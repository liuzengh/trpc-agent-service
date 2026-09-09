package retrieval

import "errors"

// Safe error categories. Raw PostgreSQL, Redis, Milvus or gRPC error text,
// credentials, DSNs, query text and vectors never pass through these errors.
var (
	// ErrInvalidConfig fails closed for missing or out-of-bounds server
	// configuration. Disabled or incomplete composition must use this path.
	ErrInvalidConfig = errors.New("vector retrieval: invalid configuration")
	// ErrInvalidRequest rejects caller input outside the server-owned bounds.
	ErrInvalidRequest = errors.New("vector retrieval: invalid request")
	// ErrUnavailable reports that the vector backend or PostgreSQL could not
	// complete the retrieval safely. It never carries raw backend errors.
	ErrUnavailable = errors.New("vector retrieval: unavailable")
	// ErrPartial reports that hydration could not prove every kept candidate
	// against PostgreSQL, so the result is not returned as a success.
	ErrPartial = errors.New("vector retrieval: hydration incomplete")
)
