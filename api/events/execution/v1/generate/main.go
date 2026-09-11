// Command generate produces transport DTOs from this directory's JSON Schema.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type schema struct {
	Type        string            `json:"type"`
	Title       string            `json:"title"`
	Ref         string            `json:"$ref"`
	GoType      string            `json:"x-go-type"`
	Pointer     bool              `json:"x-go-pointer"`
	Properties  map[string]schema `json:"properties"`
	Required    []string          `json:"required"`
	Definitions map[string]schema `json:"$defs"`
}

func main() {
	schemaPath := flag.String("schema", "run-requested.schema.json", "source JSON Schema")
	output := flag.String("out", "../../../../gen/events/execution/v1/run_requested.go", "generated Go file")
	check := flag.Bool("check", false, "verify generated file is current without writing")
	flag.Parse()
	data, err := os.ReadFile(*schemaPath)
	must(err)
	var root schema
	must(json.Unmarshal(data, &root))
	var code bytes.Buffer
	fmt.Fprintf(&code, "// Code generated from api/events/execution/v1/%s; DO NOT EDIT.\n\n// Package executionv1 contains transport DTOs, not Gateway or Worker domain entities.\npackage executionv1\n\n", filepath.Base(*schemaPath))
	emit(&code, root.Title, root)
	names := make([]string, 0, len(root.Definitions))
	for name, s := range root.Definitions {
		if s.Type == "object" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		emit(&code, name, root.Definitions[name])
	}
	generated, err := format.Source(code.Bytes())
	must(err)
	if *check {
		existing, err := os.ReadFile(*output)
		must(err)
		if !bytes.Equal(existing, generated) {
			panic("generated execution DTOs are stale")
		}
		fmt.Println("EXECUTION_GENERATION_CURRENT")
		return
	}
	must(os.MkdirAll(filepath.Dir(*output), 0755))
	must(os.WriteFile(*output, generated, 0644))
}
func emit(out *bytes.Buffer, name string, s schema) {
	fmt.Fprintf(out, "// %s is defined by the versioned execution JSON Schema.\ntype %s struct {\n", name, name)
	required := map[string]bool{}
	for _, name := range s.Required {
		required[name] = true
	}
	names := make([]string, 0, len(s.Properties))
	for name := range s.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		field := s.Properties[name]
		typ := field.GoType
		if typ == "" {
			if field.Ref != "" {
				typ = strings.TrimPrefix(field.Ref, "#/$defs/")
			} else {
				switch field.Type {
				case "string":
					typ = "string"
				case "integer":
					typ = "int64"
				case "boolean":
					typ = "bool"
				default:
					panic("unsupported schema type for " + name)
				}
			}
		}
		if field.Pointer {
			typ = "*" + typ
		}
		tag := name
		if !required[name] {
			tag += ",omitempty"
		}
		fmt.Fprintf(out, "%s %s `json:%q`\n", goName(name), typ, tag)
	}
	out.WriteString("}\n\n")
}
func goName(name string) string {
	parts := strings.Split(name, "_")
	for i, p := range parts {
		switch p {
		case "id":
			parts[i] = "ID"
		case "chatid":
			parts[i] = "ChatID"
		case "userid":
			parts[i] = "UserID"
		case "url":
			parts[i] = "URL"
		default:
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
