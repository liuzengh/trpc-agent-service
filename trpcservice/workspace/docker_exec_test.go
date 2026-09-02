package workspace

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"
)

func TestCommandForLanguageMapping(t *testing.T) {
	e := NewDockerExecutor()
	cases := []struct {
		lang      string
		wantImage string
		wantLast  string
	}{
		{"python", "python:3.12-alpine", "-"},
		{"Python", "python:3.12-alpine", "-"},
		{"py", "python:3.12-alpine", "-"},
		{"bash", "alpine:3", "-s"},
		{"sh", "alpine:3", "-s"},
		{"shell", "alpine:3", "-s"},
	}
	for _, c := range cases {
		img, args := e.commandFor(c.lang)
		if img != c.wantImage {
			t.Errorf("commandFor(%q) image = %q, want %q", c.lang, img, c.wantImage)
		}
		if len(args) == 0 || args[len(args)-1] != c.wantLast {
			t.Errorf("commandFor(%q) args = %v, want last %q", c.lang, args, c.wantLast)
		}
	}
	if img, _ := e.commandFor("ruby"); img != "" {
		t.Errorf("commandFor(ruby) image = %q, want empty", img)
	}
	if img, _ := e.commandFor(""); img != "" {
		t.Errorf("commandFor(empty) image = %q, want empty", img)
	}
}

func TestExecuteCodeUnsupportedLanguage(t *testing.T) {
	e := NewDockerExecutor()
	_, err := e.ExecuteCode(context.Background(), codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{Language: "ruby", Code: "puts 1"}},
	})
	if !errors.Is(err, ErrUnsupportedLanguage) {
		t.Errorf("err = %v, want ErrUnsupportedLanguage", err)
	}
}

func TestExecuteCodeNoDockerOnEmptyInput(t *testing.T) {
	// Empty or blank code blocks never touch the docker CLI.
	e := NewDockerExecutor()
	res, err := e.ExecuteCode(context.Background(), codeexecutor.CodeExecutionInput{})
	if err != nil {
		t.Fatalf("empty input: %v", err)
	}
	if res.Output != "" {
		t.Errorf("empty input output = %q, want empty", res.Output)
	}
	res, err = e.ExecuteCode(context.Background(), codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{Language: "python", Code: "   "}},
	})
	if err != nil {
		t.Fatalf("blank block: %v", err)
	}
	if res.Output != "" {
		t.Errorf("blank block output = %q, want empty", res.Output)
	}
}
