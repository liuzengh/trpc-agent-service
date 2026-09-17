// Package redaction compiles the service's immutable redaction rules before a
// record can reach a log, telemetry, audit, or error-report sink.
package redaction

import (
	"context"
	"errors"
	"regexp"
	"strings"
)

const Replacement = "[REDACTED]"

type Level string

const (
	LevelNone   Level = "none"
	LevelBasic  Level = "basic"
	LevelStrict Level = "strict"
)

// Rule is a declarative, additive redaction rule. It can redact a structured
// key, matching text, or both. Rules can never weaken mandatory protections.
type Rule struct {
	ID           string
	KeyFragments []string
	TextPattern  string
}

type Config struct {
	Level Level
	Rules []Rule
}

// Program is immutable and safe for concurrent use after Compile returns.
type Program struct {
	keyFragments []string
	textRules    []textRule
}

type contextKey struct{}

// ContextWithProgram binds an already compiled, trusted policy to a request.
// It never accepts raw rules, so sinks cannot compile attacker-controlled data.
func ContextWithProgram(ctx context.Context, program *Program) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, contextKey{}, program)
}

// ProgramFromContext returns a policy only when a trusted caller has bound one.
func ProgramFromContext(ctx context.Context) (*Program, bool) {
	if ctx == nil {
		return nil, false
	}
	program, ok := ctx.Value(contextKey{}).(*Program)
	return program, ok && program != nil
}

type textRule struct {
	pattern *regexp.Regexp
	replace func(string) string
}

var urlCredentialPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^\s:/@]+:)[^\s@/]+@`)

// Generic credential matching belongs to this service package while the
// service consumes the upstream agent framework strictly at its official API
// surface. Keep these patterns deliberately narrow: secret-looking fields are
// redacted, but arbitrary application text is not classified as a credential.
var sensitiveKeyFragments = []string{
	"secret", "token", "api_key", "apikey", "password", "authorization",
	"credential", "dsn", "cookie", "private_key", "access_key",
}

var credentialAssignmentPattern = regexp.MustCompile(`(?i)\b((?:api[_-]?key|secret|token|password|authorization|credential|dsn|cookie|private[_-]?key|access[_-]?key)\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+`)
var bearerPattern = regexp.MustCompile(`(?i)\bbearer\s+[a-z0-9._~+/-]+`)

var mandatoryKeyFragments = []string{
	"payload", "prompt", "content", "body",
}

var strictKeyFragments = []string{
	"user", "email", "phone", "message", "external", "session",
}

func Compile(config Config) (*Program, error) {
	if config.Level == "" {
		config.Level = LevelBasic
	}
	if config.Level != LevelNone && config.Level != LevelBasic && config.Level != LevelStrict {
		return nil, errors.New("invalid redaction level")
	}

	program := &Program{
		keyFragments: append([]string(nil), mandatoryKeyFragments...),
		textRules: []textRule{
			{pattern: urlCredentialPattern, replace: redactURLCredential},
		},
	}
	if config.Level == LevelStrict {
		program.keyFragments = append(program.keyFragments, strictKeyFragments...)
	}

	seen := make(map[string]struct{}, len(config.Rules))
	for _, rule := range config.Rules {
		if err := program.addRule(rule, seen); err != nil {
			return nil, err
		}
	}
	return program, nil
}

func (p *Program) addRule(rule Rule, seen map[string]struct{}) error {
	rule.ID = strings.TrimSpace(rule.ID)
	if !validIdentifier(rule.ID) {
		return errors.New("invalid redaction rule identifier")
	}
	if _, exists := seen[rule.ID]; exists {
		return errors.New("duplicate redaction rule identifier")
	}
	seen[rule.ID] = struct{}{}
	if len(rule.KeyFragments) == 0 && strings.TrimSpace(rule.TextPattern) == "" {
		return errors.New("redaction rule has no matcher")
	}
	if len(rule.KeyFragments) > 16 || len(rule.TextPattern) > 256 {
		return errors.New("redaction rule exceeds limits")
	}
	for _, fragment := range rule.KeyFragments {
		fragment = canonicalKey(fragment)
		if fragment == "" || len(fragment) > 64 {
			return errors.New("invalid redaction key matcher")
		}
		p.keyFragments = append(p.keyFragments, fragment)
	}
	if pattern := strings.TrimSpace(rule.TextPattern); pattern != "" {
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return errors.New("invalid redaction text matcher")
		}
		p.textRules = append(p.textRules, textRule{pattern: compiled, replace: func(string) string { return Replacement }})
	}
	return nil
}

func (p *Program) RedactKey(key string) bool {
	if p == nil || sensitiveName(key) {
		return true
	}
	canonical := canonicalKey(key)
	for _, fragment := range p.keyFragments {
		if strings.Contains(canonical, fragment) {
			return true
		}
	}
	return false
}

func (p *Program) RedactText(value string) string {
	if p == nil {
		return Replacement
	}
	value = credentialAssignmentPattern.ReplaceAllString(value, "${1}"+Replacement)
	value = bearerPattern.ReplaceAllString(value, "Bearer "+Replacement)
	value = redactURLCredential(value)
	for _, rule := range p.textRules {
		value = rule.pattern.ReplaceAllStringFunc(value, rule.replace)
	}
	return value
}

func sensitiveName(value string) bool {
	canonical := canonicalKey(value)
	for _, fragment := range sensitiveKeyFragments {
		if strings.Contains(canonical, fragment) {
			return true
		}
	}
	return false
}

func redactURLCredential(value string) string {
	return urlCredentialPattern.ReplaceAllString(value, "${1}"+Replacement+"@")
}

func canonicalKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "-", "_")
	return strings.ReplaceAll(value, ".", "_")
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, runeValue := range value {
		if !(runeValue >= 'a' && runeValue <= 'z' || runeValue >= 'A' && runeValue <= 'Z' || runeValue >= '0' && runeValue <= '9' || runeValue == '_' || runeValue == '-') {
			return false
		}
	}
	return true
}
