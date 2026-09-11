package domain

import (
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestProviderCallableNamesMatchFrozenConsumerContract(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "..", "api", "schemas", "deployment", "v1", "callable-name-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		ContractVersion     string `json:"contract_version"`
		Kind                string `json:"kind"`
		Scope               string `json:"scope"`
		LogicalEntryPattern string `json:"logical_entry_pattern"`
		Mapping             struct {
			Input                  string `json:"input"`
			Encoding               string `json:"encoding"`
			Hash                   string `json:"hash"`
			DigestEncoding         string `json:"digest_encoding"`
			DigestPrefixCharacters int    `json:"digest_prefix_characters"`
			NamePrefix             string `json:"name_prefix"`
			OutputCharacters       int    `json:"output_characters"`
			OutputPattern          string `json:"output_pattern"`
		} `json:"mapping"`
		Rules  map[string]string `json:"rules"`
		Golden []struct {
			EntryID      string `json:"entry_id"`
			ProviderName string `json:"provider_name"`
		} `json:"golden"`
		InvalidEntries []string `json:"invalid_entries"`
	}
	if err := strictDecodeJSON(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.ContractVersion != ProviderCallableNameVersionV1 || fixture.Kind != "static_consumer_contract" ||
		fixture.Scope != "one_manifest_node" || fixture.LogicalEntryPattern != callableEntryPattern.String() ||
		fixture.Mapping.Input != "exact_complete_logical_entry_id" || fixture.Mapping.Encoding != "UTF-8" ||
		fixture.Mapping.Hash != "SHA-256" || fixture.Mapping.DigestEncoding != "lowercase_hex" ||
		fixture.Mapping.DigestPrefixCharacters != 60 || fixture.Mapping.NamePrefix != "fn_" ||
		fixture.Mapping.OutputCharacters != ProviderCallableNameLengthV1 || fixture.Mapping.OutputPattern != `^fn_[0-9a-f]{60}$` {
		t.Fatal("consumer fixture does not describe the frozen V1 callable mapping")
	}
	wantRules := map[string]string{
		"empty_set": "empty_mapping", "input_order": "irrelevant", "invalid_entry": "reject_complete_set",
		"duplicate_entry": "reject_complete_set", "same_node_name_collision": "reject_complete_set",
		"collision_fallback": "none", "input_normalization": "none",
		"remote_tool_name": "not_used_for_mapping", "node_authority": "manifest_callable_entries_only",
	}
	if !maps.Equal(fixture.Rules, wantRules) || len(fixture.Golden) < 6 || len(fixture.InvalidEntries) < 17 {
		t.Fatal("consumer fixture lost a required semantic rule or boundary example")
	}
	outputPattern := regexp.MustCompile(fixture.Mapping.OutputPattern)
	for _, golden := range fixture.Golden {
		t.Run(golden.EntryID, func(t *testing.T) {
			got, err := ProviderCallableNames([]string{golden.EntryID})
			if err != nil || len(got) != 1 || got[golden.EntryID] != golden.ProviderName ||
				len(got[golden.EntryID]) != ProviderCallableNameLengthV1 || !outputPattern.MatchString(got[golden.EntryID]) {
				t.Fatalf("ProviderCallableNames(%q) = %v, %v; want %q", golden.EntryID, got, err, golden.ProviderName)
			}
		})
	}
	for _, entry := range append(fixture.InvalidEntries, "tools/\x00search", "tools/\xff") {
		got, err := ProviderCallableNames([]string{"tools/valid", entry})
		if !errors.Is(err, ErrInvalidCallableEntry) || got != nil {
			t.Fatalf("invalid entry %q returned partial/successful mapping %v, %v", entry, got, err)
		}
	}
}

func TestProviderCallableNamesPreserveCategoryAndIgnoreEnumerationOrder(t *testing.T) {
	entries := []string{"tools/search", "knowledge/search", "knowledge/docs"}
	original := slices.Clone(entries)
	first, err := ProviderCallableNames(entries)
	if err != nil || !slices.Equal(entries, original) {
		t.Fatalf("mapping changed input or failed: %v", err)
	}
	slices.Reverse(entries)
	second, err := ProviderCallableNames(entries)
	if err != nil || !maps.Equal(first, second) || first["tools/search"] == first["knowledge/search"] {
		t.Fatalf("category or order semantics changed: %v / %v, %v", first, second, err)
	}
	// A callable's name is independent of what else a node is permitted to use.
	single, err := ProviderCallableNames([]string{"tools/search"})
	if err != nil || single["tools/search"] != first["tools/search"] {
		t.Fatalf("node-set membership changed a stable name: %v, %v", single, err)
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if string(firstJSON) != string(secondJSON) {
		t.Fatal("mapping encoding depends on enumeration order")
	}
}

func TestProviderCallableNamesRejectDuplicatesAndTruncatedHashCollision(t *testing.T) {
	if got, err := ProviderCallableNames([]string{"tools/search", "tools/search"}); !errors.Is(err, ErrDuplicateCallableEntry) || got != nil {
		t.Fatalf("duplicate accepted or returned partial map: %v, %v", got, err)
	}
	// Different complete SHA-256 results whose last two bytes differ still
	// collide after the contract's fixed first-60-hex projection.
	digest := func(entry []byte) [32]byte {
		var sum [32]byte
		sum[31] = entry[0]
		return sum
	}
	entries := []string{"tools/search", "knowledge/search"}
	got, err := providerCallableNamesWithDigest(entries, digest)
	if !errors.Is(err, ErrCallableNameCollision) || got != nil {
		t.Fatalf("collision accepted or returned partial map: %v, %v", got, err)
	}
	slices.Reverse(entries)
	_, reversedErr := providerCallableNamesWithDigest(entries, digest)
	if reversedErr == nil || reversedErr.Error() != err.Error() {
		t.Fatalf("collision result depends on enumeration order: %v / %v", err, reversedErr)
	}
}

func TestProviderCallableNamesEmptySetAndResourceKeyLengthBoundary(t *testing.T) {
	for _, empty := range [][]string{nil, {}} {
		got, err := ProviderCallableNames(empty)
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("empty set = %v, %v", got, err)
		}
	}
	for _, length := range []int{1, 63, 64, 65} {
		entry := "tools/" + strings.Repeat("a", length)
		got, err := ProviderCallableNames([]string{entry})
		if length <= 64 {
			if err != nil || len(got[entry]) != ProviderCallableNameLengthV1 {
				t.Fatalf("key length %d = %v, %v", length, got, err)
			}
		} else if !errors.Is(err, ErrInvalidCallableEntry) || got != nil {
			t.Fatalf("oversized key accepted: %v, %v", got, err)
		}
	}
}
