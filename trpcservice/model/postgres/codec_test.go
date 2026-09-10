package postgres

import (
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func TestModelPostgresConfigurationCodec(t *testing.T) {
	configuration := model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}}
	options, generation, err := encodeModelJSON(configuration)
	if err != nil {
		t.Fatal(err)
	}
	var decoded model.Configuration
	if err := decodeModelJSON(options, generation, &decoded); err != nil || decoded.Options["mode"] != "safe" {
		t.Fatalf("configuration decode = %+v, err=%v", decoded, err)
	}
	if err := decodeModelJSON([]byte("not-json"), []byte("{}"), &model.Configuration{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("malformed configuration error = %v", err)
	}
	if err := decodeModelJSON([]byte("{}"), []byte("not-json"), &model.Configuration{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("malformed generation error = %v", err)
	}
}
