package mysql

import "github.com/XnLemon/trpc-agent-service/trpcservice/storage/internal/jsoncodec"

// EncodeJSON encodes a secret-free control-plane payload.
func EncodeJSON(value any) ([]byte, error) {
	return jsoncodec.Encode(value)
}

// DecodeJSON decodes a complete JSON payload and fails closed on malformed or
// trailing data.
func DecodeJSON(data []byte, destination any) error {
	if err := jsoncodec.Decode(data, destination); err != nil {
		return ErrStorage
	}
	return nil
}
