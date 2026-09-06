// trpc-tracecheck verifies metadata-only callback/tool traces retrieved from Tempo.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

type span struct {
	name, id, parent, traceID string
	links                     []string
}

func main() {
	initial := flag.String("initial", "", "initial callback trace JSON file")
	decision := flag.String("decision", "", "approval decision trace JSON file")
	file := flag.String("file", "", "show span names from one Tempo trace JSON file")
	flag.Parse()
	if *file != "" {
		spans, err := readSpans(*file)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		names := make([]string, 0, len(spans))
		for _, item := range spans {
			names = append(names, item.name)
		}
		sort.Strings(names)
		fmt.Printf("spans=%d\n", len(spans))
		for _, name := range names {
			fmt.Println(name)
		}
		return
	}
	if err := verify(*initial, *decision); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func readSpans(path string) ([]span, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	for _, canary := range []string{"fake-secret", "fake-token", "memory-secret-canary", "argument-secret-canary", "模型伪造"} {
		if strings.Contains(string(data), canary) {
			return nil, fmt.Errorf("trace privacy canary found; payload not printed")
		}
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, fmt.Errorf("invalid trace JSON")
	}
	var spans []span
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			id, _ := typed["spanId"].(string)
			name, _ := typed["name"].(string)
			traceID, _ := typed["traceId"].(string)
			if id != "" && name != "" && traceID != "" {
				parent, _ := typed["parentSpanId"].(string)
				item := span{name: name, id: id, traceID: traceID, parent: parent}
				links, _ := typed["links"].([]any)
				for _, raw := range links {
					if link, ok := raw.(map[string]any); ok {
						if linked, ok := link["traceId"].(string); ok {
							item.links = append(item.links, linked)
						}
					}
				}
				spans = append(spans, item)
			}
			for _, child := range typed {
				walk(child)
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(value)
	if len(spans) == 0 {
		return nil, fmt.Errorf("trace contains no spans")
	}
	return spans, nil
}

func requireNames(spans []span, prefixes ...string) error {
	for _, prefix := range prefixes {
		found := false
		for _, item := range spans {
			if strings.HasPrefix(item.name, prefix) {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("stored trace missing span %q", prefix)
		}
	}
	return nil
}

func connected(spans []span) error {
	if len(spans) == 0 {
		return fmt.Errorf("trace contains no spans")
	}
	byID := make(map[string]span)
	for _, item := range spans {
		if item.traceID != spans[0].traceID {
			return fmt.Errorf("trace file mixes trace IDs")
		}
		byID[item.id] = item
	}
	root := func(id string) bool { return id == "" || strings.Trim(id, "0") == "" || id == "AAAAAAAAAAA=" }
	roots := 0
	for _, item := range byID {
		if root(item.parent) {
			roots++
			continue
		}
		seen := map[string]bool{item.id: true}
		for parent := item.parent; !root(parent); {
			ancestor, ok := byID[parent]
			if !ok {
				return fmt.Errorf("preflight span %q has an unrecorded parent", item.name)
			}
			if seen[parent] {
				return fmt.Errorf("trace parent cycle")
			}
			seen[parent] = true
			parent = ancestor.parent
		}
	}
	if roots != 1 {
		return fmt.Errorf("preflight trace has %d roots, expected one", roots)
	}
	return nil
}

func verify(initialFile, decisionFile string) error {
	if initialFile == "" || decisionFile == "" {
		return fmt.Errorf("-initial and -decision are required")
	}
	initial, err := readSpans(initialFile)
	if err != nil {
		return err
	}
	decision, err := readSpans(decisionFile)
	if err != nil {
		return err
	}
	if err := connected(initial); err != nil {
		return err
	}
	if err := connected(decision); err != nil {
		return err
	}
	if err := requireNames(initial, "POST /callbacks/telegram/{callback_key}", "channel.callback", "gateway.accept", "queue.publish",
		"worker.agent.run", "invoke_agent ", "chat ", "execute_tool ", "storage.session.get", "storage.session.event.append", "reply.send"); err != nil {
		return err
	}
	if err := requireNames(decision, "approval.decide", "execute_tool dangerous_demo", "storage.memory.add", "storage.memory.read", "reply.send"); err != nil {
		return err
	}
	linked := false
	for _, item := range decision {
		if item.name == "approval.decide" {
			for _, origin := range item.links {
				if origin == initial[0].traceID {
					linked = true
				}
			}
		}
	}
	if !linked {
		return fmt.Errorf("stored decision trace has no link to the initial request")
	}
	fmt.Printf("stored traces verified: initial_spans=%d decision_spans=%d origin_link=true\n", len(initial), len(decision))
	return nil
}
