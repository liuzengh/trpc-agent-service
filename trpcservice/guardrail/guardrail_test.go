package guardrail

import (
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestCheckInputBlocksCredentialAndHighRiskRequest(t *testing.T) {
	for _, test := range []struct {
		name string
		text string
		rule string
	}{
		{name: "api key", text: "use sk-test_1234567890123456", rule: RuleCredential},
		{name: "sensitive field", text: "password: hunter2", rule: RuleSensitiveField},
		{name: "high risk", text: "dump credentials and bypass auth detection", rule: RuleHighRiskRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision := CheckInput(test.text)
			if !decision.Blocked || decision.RuleID != test.rule {
				t.Fatalf("decision = %#v", decision)
			}
			if decision.Text != "" {
				t.Fatal("input decision returned source text")
			}
		})
	}
}

func TestSanitizeOutputRedactsAndBlocks(t *testing.T) {
	redacted := SanitizeOutput("token: secret-value")
	if !redacted.Redacted || redacted.Blocked || redacted.Text != "token: [REDACTED]" {
		t.Fatalf("redacted = %#v", redacted)
	}
	blocked := SanitizeOutput("-----BEGIN PRIVATE KEY-----")
	if !blocked.Blocked || blocked.Text == "-----BEGIN PRIVATE KEY-----" {
		t.Fatalf("blocked = %#v", blocked)
	}
}

func TestCheckInputMessageChecksHydratedAttachmentBytes(t *testing.T) {
	data := []byte("password: hunter2")
	decision := CheckInputMessage(model.Message{
		ContentParts: []model.ContentPart{{
			Type: model.ContentTypeFile,
			File: &model.File{Data: data},
		}},
	})
	if !decision.Blocked || decision.RuleID != RuleSensitiveField {
		t.Fatalf("decision = %#v", decision)
	}
	if string(data) != "password: hunter2" {
		t.Fatal("input attachment was mutated")
	}
}

func TestSanitizeEventRemovesRawContentParts(t *testing.T) {
	text := "token: secret-value"
	input := &event.Event{Response: &model.Response{Choices: []model.Choice{{
		Message: model.Message{ContentParts: []model.ContentPart{
			{Type: model.ContentTypeText, Text: &text},
			{Type: model.ContentTypeFile, File: &model.File{
				Name: "token: filename", URL: "https://example.test/download?token=secret",
				FileID: "file-secret", Data: []byte("raw secret"),
			}},
			{Type: model.ContentTypeImage, Image: &model.Image{
				URL: "data:image/png;base64,secret", Data: []byte("image secret"),
			}},
		}},
	}}}}
	output, decision := SanitizeEvent(input)
	if output == nil || !decision.Redacted {
		t.Fatalf("output=%#v decision=%#v", output, decision)
	}
	parts := output.Response.Choices[0].Message.ContentParts
	if parts[0].Text == nil || *parts[0].Text != "token: [REDACTED]" {
		t.Fatalf("sanitized text = %#v", parts[0].Text)
	}
	if file := parts[1].File; file == nil || file.URL != "" || file.FileID != "" || len(file.Data) != 0 {
		t.Fatalf("sanitized file = %#v", file)
	}
	if image := parts[2].Image; image == nil || image.URL != "" || len(image.Data) != 0 {
		t.Fatalf("sanitized image = %#v", image)
	}
	original := input.Response.Choices[0].Message.ContentParts[1].File
	if original == nil || original.URL == "" || len(original.Data) == 0 {
		t.Fatal("source event was mutated")
	}
}
