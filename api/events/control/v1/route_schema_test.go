package controleventsv1_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	events "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	controlv1 "github.com/liuzengh/trpc-agent-service/gen/events/control/v1"
)

func TestFixtures(t *testing.T) {
	for _, kind := range []string{"valid", "invalid"} {
		files, err := filepath.Glob(filepath.Join("fixtures", kind, "*.json"))
		if err != nil || len(files) == 0 {
			t.Fatalf("missing %s fixtures: %v", kind, err)
		}
		for _, path := range files {
			t.Run(path, func(t *testing.T) {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				event, err := events.DecodeRouteProjectionEvent(data)
				if kind == "invalid" {
					if !errors.Is(err, events.ErrInvalidEvent) {
						t.Fatalf("expected invalid event, got %#v %v", event, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := events.DecodeRouteProjectionEvent(encoded)
				if err != nil || decoded != event {
					t.Fatalf("round-trip: %#v %v", decoded, err)
				}
			})
		}
	}
}

func TestGeneratedDTOTracksSchema(t *testing.T) {
	actual := fmt.Sprintf("%x", sha256.Sum256(events.RouteProjectionSchema))
	if actual != controlv1.RouteProjectionSchemaSHA256 {
		t.Fatal("generated DTO is stale: run go generate ./api/events/control/v1")
	}
}

func TestDecoderBoundsAndTrailingInput(t *testing.T) {
	data, err := os.ReadFile("fixtures/valid/enabled.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range [][]byte{nil, []byte("null"), []byte("{"), append(append([]byte{}, data...), []byte(` {}`)...), []byte(strings.Repeat(" ", events.MaxEventBytes+1))} {
		if _, err = events.DecodeRouteProjectionEvent(input); !errors.Is(err, events.ErrInvalidEvent) {
			t.Fatalf("expected invalid event, got %v", err)
		}
	}
}

func TestIntegralNumbersAndDuplicateKeys(t *testing.T) {
	data, err := os.ReadFile("fixtures/valid/enabled.json")
	if err != nil {
		t.Fatal(err)
	}
	integral := strings.ReplaceAll(string(data), `"generation": 1`, `"generation": 1.0`)
	integral = strings.ReplaceAll(integral, `"schema_version": 1`, `"schema_version": 1e0`)
	event, err := events.DecodeRouteProjectionEvent([]byte(integral))
	if err != nil || event.Route.Generation != 1 || event.SchemaVersion != 1 {
		t.Fatalf("integral encoding: %#v %v", event, err)
	}
	duplicate := strings.ReplaceAll(string(data), `"generation": 1`, `"generation": 99, "generation": 1`)
	if _, err := events.DecodeRouteProjectionEvent([]byte(duplicate)); !errors.Is(err, events.ErrInvalidEvent) {
		t.Fatalf("duplicate keys accepted: %v", err)
	}
}
