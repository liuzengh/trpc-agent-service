package channel

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

const maxAdapterInboundBodyBytes int64 = 1 << 20

func readAdapterInboundBody(req *http.Request, restore bool) ([]byte, error) {
	if req == nil || req.Body == nil {
		return nil, fmt.Errorf("channel request body is required")
	}
	original := req.Body
	defer original.Close()
	if req.ContentLength > maxAdapterInboundBodyBytes {
		return nil, fmt.Errorf("channel request body exceeds %d bytes", maxAdapterInboundBodyBytes)
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, maxAdapterInboundBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read channel request body: %w", err)
	}
	if int64(len(body)) > maxAdapterInboundBodyBytes {
		return nil, fmt.Errorf("channel request body exceeds %d bytes", maxAdapterInboundBodyBytes)
	}
	if restore {
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
	}
	return body, nil
}
