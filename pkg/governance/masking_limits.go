// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package governance

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxMaskingBytes         = 1 << 20
	maxMaskingMatchBytes    = 4 << 20
	maxMaskingTemplateBytes = 4096
	maxMaskingDepth         = 64
)

type maskingBudget struct {
	limit int
	used  int
}

// Bound ordinary tool-owned values before encoding/json allocates its buffer.
// Custom MarshalJSON/MarshalText implementations own their allocations; their
// encoded output is checked separately before decoding or applying any rules.
func checkMaskingInput(ctx context.Context, value interface{}, limit int) error {
	remaining := limit
	var visit func(reflect.Value, int) error
	visit = func(value reflect.Value, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Interfaces add a reflect layer without adding a JSON nesting level.
		if depth > 2*maxMaskingDepth {
			return fmt.Errorf("masking nesting exceeds %d levels", maxMaskingDepth)
		}
		remaining--
		if remaining < 0 {
			return fmt.Errorf("masking input exceeds %d bytes", limit)
		}
		if !value.IsValid() {
			return nil
		}
		switch value.Kind() {
		case reflect.Interface, reflect.Pointer:
			if !value.IsNil() {
				return visit(value.Elem(), depth+1)
			}
		case reflect.String:
			remaining -= value.Len()
		case reflect.Array, reflect.Slice:
			if value.Type().Elem().Kind() == reflect.Uint8 {
				remaining -= value.Len()
				break
			}
			if value.Len() > remaining {
				return fmt.Errorf("masking input exceeds %d bytes", limit)
			}
			for index := 0; index < value.Len(); index++ {
				if err := visit(value.Index(index), depth+1); err != nil {
					return err
				}
			}
		case reflect.Map:
			if value.Len() > remaining/2 {
				return fmt.Errorf("masking input exceeds %d bytes", limit)
			}
			iter := value.MapRange()
			for iter.Next() {
				if err := visit(iter.Key(), depth+1); err != nil {
					return err
				}
				if err := visit(iter.Value(), depth+1); err != nil {
					return err
				}
			}
		case reflect.Struct:
			for index := 0; index < value.NumField(); index++ {
				field := value.Type().Field(index)
				if field.Tag.Get("json") == "-" || (!field.IsExported() && !field.Anonymous) {
					continue
				}
				if err := visit(value.Field(index), depth+1); err != nil {
					return err
				}
			}
		}
		if remaining < 0 {
			return fmt.Errorf("masking input exceeds %d bytes", limit)
		}
		return nil
	}
	return visit(reflect.ValueOf(value), 0)
}

func replaceMaskingString(ctx context.Context, re *regexp.Regexp, source, replacement string, limit, matchBudget int) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(source) > limit || len(replacement) > maxMaskingTemplateBytes {
		return "", fmt.Errorf("masking string exceeds its byte budget")
	}
	// Keep both the capture indices and the outer slice bounded. Searching the
	// original string preserves anchors, word boundaries, and empty matches.
	matchBytes := (2*(re.NumSubexp()+1) + 3) * (strconv.IntSize / 8)
	maxMatches := matchBudget / matchBytes
	if maxMatches < 1 {
		return "", fmt.Errorf("masking match indices exceed their byte budget")
	}
	matches := re.FindAllStringSubmatchIndex(source, maxMatches+1)
	if len(matches) > maxMatches {
		return "", fmt.Errorf("masking match indices exceed their byte budget")
	}
	if len(matches) == 0 {
		return source, nil
	}
	var result strings.Builder
	appendText := func(text string) error {
		if len(text) > limit-result.Len() {
			return fmt.Errorf("masking replacement exceeds %d bytes", limit)
		}
		result.WriteString(text)
		return nil
	}
	end := 0
	for _, match := range matches {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if err := appendText(source[end:match[0]]); err != nil {
			return "", err
		}
		template := replacement
		for len(template) > 0 {
			dollar := strings.IndexByte(template, '$')
			if dollar < 0 {
				if err := appendText(template); err != nil {
					return "", err
				}
				break
			}
			if err := appendText(template[:dollar]); err != nil {
				return "", err
			}
			template = template[dollar:]
			partEnd := len(template)
			if len(template) > 1 && template[1] == '$' {
				partEnd = 2
			} else if next := strings.IndexByte(template[1:], '$'); next >= 0 {
				partEnd = next + 1
			}
			// Each part has at most one capture expansion, so even a rejected
			// part is bounded by one input string plus one small template.
			part := re.ExpandString(nil, template[:partEnd], source, match)
			if len(part) > limit-result.Len() {
				return "", fmt.Errorf("masking replacement exceeds %d bytes", limit)
			}
			result.Write(part)
			template = template[partEnd:]
		}
		end = match[1]
	}
	if err := appendText(source[end:]); err != nil {
		return "", err
	}
	return result.String(), nil
}

// Count encoding/json's default string representation without constructing it.
// This includes HTML escapes, invalid UTF-8, and the two JSONP separators.
func maskingJSONStringSize(value string, limit int) (int, error) {
	size := 2
	for index := 0; index < len(value); {
		char := value[index]
		if char < utf8.RuneSelf {
			switch char {
			case '\\', '"', '\b', '\f', '\n', '\r', '\t':
				size += 2
			case '<', '>', '&':
				size += 6
			default:
				if char < 0x20 {
					size += 6
				} else {
					size++
				}
			}
			index++
		} else {
			char, width := utf8.DecodeRuneInString(value[index:])
			if (char == utf8.RuneError && width == 1) || char == '\u2028' || char == '\u2029' {
				size += 6
			} else {
				size += width
			}
			index += width
		}
		if size > limit {
			return 0, fmt.Errorf("masked JSON string exceeds its byte budget")
		}
	}
	if size > limit {
		return 0, fmt.Errorf("masked JSON string exceeds its byte budget")
	}
	return size, nil
}

func maskingJSONSize(value interface{}, limit, depth int) (int, error) {
	if depth > maxMaskingDepth {
		return 0, fmt.Errorf("masking nesting exceeds %d levels", maxMaskingDepth)
	}
	size := 0
	addValue := func(child interface{}) error {
		childSize, err := maskingJSONSize(child, limit-size, depth+1)
		if err != nil {
			return err
		}
		size += childSize
		return nil
	}
	switch typed := value.(type) {
	case string:
		return maskingJSONStringSize(typed, limit)
	case json.Number:
		size = len(typed)
	case bool:
		size = 4
		if !typed {
			size = 5
		}
	case nil:
		size = 4
	case []interface{}:
		size = 2
		for index, child := range typed {
			if index > 0 {
				size++
			}
			if err := addValue(child); err != nil {
				return 0, err
			}
		}
	case map[string]interface{}:
		size = 2
		for key, child := range typed {
			if size > 2 {
				size++
			}
			keySize, err := maskingJSONStringSize(key, limit-size)
			if err != nil {
				return 0, err
			}
			size += keySize + 1
			if err := addValue(child); err != nil {
				return 0, err
			}
		}
	default:
		return 0, fmt.Errorf("masking received a non-JSON value")
	}
	if size > limit {
		return 0, fmt.Errorf("masked JSON exceeds its byte budget")
	}
	return size, nil
}
