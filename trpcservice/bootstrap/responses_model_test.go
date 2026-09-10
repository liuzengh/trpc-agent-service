package bootstrap

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestResponsesModelInfoAndInputDefaults(t *testing.T) {
	model := &responsesModel{model: "gpt-5.6-sol"}
	if got := model.Info(); got.Name != "gpt-5.6-sol" {
		t.Fatalf("Info name = %q", got.Name)
	}
	input := responsesInput(&trpcmodel.Request{Messages: []trpcmodel.Message{
		{ContentParts: []trpcmodel.ContentPart{{Text: stringPtr("part-a")}, {Text: nil}, {Text: stringPtr("part-b")}}},
	}})
	if len(input) != 1 || input[0].Role != string(trpcmodel.RoleUser) || input[0].Content[0].Text != "part-apart-b" {
		t.Fatalf("input defaults = %#v", input)
	}
}

func TestResponsesModelMapsContentParts(t *testing.T) {
	input := responsesInput(&trpcmodel.Request{Messages: []trpcmodel.Message{{
		Role:    trpcmodel.RoleUser,
		Content: "describe",
		ContentParts: []trpcmodel.ContentPart{
			{Type: trpcmodel.ContentTypeImage, Image: &trpcmodel.Image{Data: []byte{1, 2}, Detail: "auto", Format: "png"}},
			{Type: trpcmodel.ContentTypeFile, File: &trpcmodel.File{Name: "brief.pdf", Data: []byte("pdf"), MimeType: "application/pdf"}},
			{Type: trpcmodel.ContentTypeAudio, Audio: &trpcmodel.Audio{Data: []byte("mp3"), Format: "audio/mpeg"}},
			{Type: trpcmodel.ContentTypeVideo, Video: &trpcmodel.Video{Data: []byte("mp4"), Format: "mp4"}},
		},
	}}})
	if len(input) != 1 || len(input[0].Content) != 5 {
		t.Fatalf("input = %#v", input)
	}
	parts := input[0].Content
	if parts[0].Type != "input_text" || parts[0].Text != "describe" {
		t.Fatalf("text part = %#v", parts[0])
	}
	if parts[1].Type != "input_image" || parts[1].ImageURL != "data:image/png;base64,AQI=" || parts[1].Detail != "auto" {
		t.Fatalf("image part = %#v", parts[1])
	}
	if parts[2].Type != "input_file" || parts[2].Filename != "brief.pdf" || parts[2].FileData != "data:application/pdf;base64,cGRm" {
		t.Fatalf("file part = %#v", parts[2])
	}
	if parts[3].Type != "input_audio" || parts[3].InputAudio == nil || parts[3].InputAudio.Data != "bXAz" || parts[3].InputAudio.Format != "mp3" {
		t.Fatalf("audio part = %#v", parts[3])
	}
	if parts[4].Type != "input_text" || parts[4].Text != "[video attachment omitted: model video input is not enabled]" {
		t.Fatalf("video fallback part = %#v", parts[4])
	}
}

func TestResponsesModelMapsContentPartFallbacksAndReferences(t *testing.T) {
	parts := responsesContent(trpcmodel.Message{ContentParts: []trpcmodel.ContentPart{
		{Type: trpcmodel.ContentTypeImage, Image: &trpcmodel.Image{URL: " https://files.example/image.png ", Detail: "low"}},
		{Type: trpcmodel.ContentTypeImage},
		{Type: trpcmodel.ContentTypeFile, File: &trpcmodel.File{FileID: " file-123 "}},
		{Type: trpcmodel.ContentTypeFile, File: &trpcmodel.File{URL: " https://files.example/brief.pdf "}},
		{Type: trpcmodel.ContentTypeFile, File: &trpcmodel.File{Data: []byte("raw")}},
		{Type: trpcmodel.ContentTypeFile},
		{Type: trpcmodel.ContentTypeAudio, Audio: &trpcmodel.Audio{Data: []byte("wav"), Format: "audio/x-wav"}},
		{Type: trpcmodel.ContentTypeAudio, Audio: &trpcmodel.Audio{Data: []byte("ogg"), Format: "audio/ogg"}},
		{Type: "unsupported"},
	}})
	if len(parts) != 8 {
		t.Fatalf("parts = %#v", parts)
	}
	assertResponsesPart(t, parts[0], responsesPartWant{typ: "input_image", imageURL: "https://files.example/image.png", detail: "low"})
	assertResponsesPart(t, parts[1], responsesPartWant{typ: "input_text", text: "[image attachment omitted: image data is missing]"})
	assertResponsesPart(t, parts[2], responsesPartWant{typ: "input_file", fileID: "file-123"})
	assertResponsesPart(t, parts[3], responsesPartWant{typ: "input_file", fileURL: "https://files.example/brief.pdf"})
	assertResponsesPart(t, parts[4], responsesPartWant{typ: "input_file", fileData: "data:application/octet-stream;base64,cmF3", filename: "attachment"})
	assertResponsesPart(t, parts[5], responsesPartWant{typ: "input_text", text: "[file attachment omitted: file data is missing]"})
	assertResponsesPart(t, parts[6], responsesPartWant{typ: "input_audio", audioFormat: "wav"})
	assertResponsesPart(t, parts[7], responsesPartWant{typ: "input_text", text: "[audio attachment omitted: unsupported audio format]"})

	empty := responsesContent(trpcmodel.Message{ContentParts: []trpcmodel.ContentPart{{Type: "unsupported"}}})
	if len(empty) != 1 || empty[0].Type != "input_text" || empty[0].Text != "" {
		t.Fatalf("empty content fallback = %#v", empty)
	}
}

type responsesPartWant struct {
	typ, text, imageURL, detail, fileID, fileURL, fileData, filename, audioFormat string
}

func assertResponsesPart(t *testing.T, got responsesContentPart, want responsesPartWant) {
	t.Helper()
	if got.Type != want.typ || got.Text != want.text || got.ImageURL != want.imageURL || got.Detail != want.detail || got.FileID != want.fileID || got.FileURL != want.fileURL || got.FileData != want.fileData || got.Filename != want.filename {
		t.Fatalf("content part = %#v, want %#v", got, want)
	}
	if want.audioFormat == "" && got.InputAudio != nil {
		t.Fatalf("unexpected audio payload = %#v", got.InputAudio)
	}
	if want.audioFormat != "" && (got.InputAudio == nil || got.InputAudio.Format != want.audioFormat) {
		t.Fatalf("audio part = %#v, want format %q", got, want.audioFormat)
	}
}

func TestResponsesAudioFormatVariants(t *testing.T) {
	for _, test := range []struct {
		input string
		want  string
	}{
		{input: "mpeg", want: "mp3"},
		{input: "audio/mpga", want: "mp3"},
		{input: "wave", want: "wav"},
		{input: "wav", want: "wav"},
		{input: "flac", want: ""},
		{input: "", want: ""},
	} {
		if got := responsesAudioFormat(test.input); got != test.want {
			t.Fatalf("responsesAudioFormat(%q) = %q, want %q", test.input, got, test.want)
		}
	}
}

func TestResponsesModelGenerateContentRejectsInvalidArguments(t *testing.T) {
	model := &responsesModel{}
	if responses, err := model.GenerateContent(nil, &trpcmodel.Request{}); responses != nil || err == nil {
		t.Fatalf("nil context = responses %#v err %v", responses, err)
	}
	if responses, err := model.GenerateContent(context.Background(), nil); responses != nil || err == nil {
		t.Fatalf("nil request = responses %#v err %v", responses, err)
	}
}

func TestResponsesModelStreamsOutputTextAndUsage(t *testing.T) {
	server := httptest.NewServer(responsesTextAndUsageHandler(t))
	defer server.Close()

	responses, err := (&responsesModel{apiKey: "test-key", endpoint: server.URL + "/v1", model: "gpt-5.6-sol"}).GenerateContent(context.Background(), &trpcmodel.Request{Messages: []trpcmodel.Message{{Role: trpcmodel.RoleUser, Content: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	assertResponsesTextAndUsage(t, responses)
}

func responsesTextAndUsageHandler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, request *http.Request) {
		assertResponsesTextRequest(t, request)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"北\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"京\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}}\n\n"))
	}
}

func assertResponsesTextRequest(t *testing.T, request *http.Request) {
	t.Helper()
	if request.URL.Path != "/v1/responses" || request.Header.Get("Authorization") != "Bearer test-key" || request.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected request: %s %s", request.Method, request.URL.String())
	}
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(request.Body).Decode(&raw); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	if _, ok := raw["include"]; ok {
		t.Fatalf("request body unexpectedly included encrypted reasoning: %#v", raw)
	}
	var body struct {
		Model  string               `json:"model"`
		Input  []responsesInputItem `json:"input"`
		Store  bool                 `json:"store"`
		Stream bool                 `json:"stream"`
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("remarshal request body: %v", err)
	}
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatalf("decode normalized request body: %v", err)
	}
	if body.Model != "gpt-5.6-sol" || len(body.Input) != 1 || len(body.Input[0].Content) != 1 || body.Input[0].Content[0].Text != "hello" || body.Store || !body.Stream {
		t.Fatalf("request body = %#v err=%v", body, err)
	}
}

func assertResponsesTextAndUsage(t *testing.T, responses <-chan *trpcmodel.Response) {
	t.Helper()
	var got string
	var usage *trpcmodel.Usage
	count := 0
	for response := range responses {
		count++
		if len(response.Choices) > 0 {
			got += response.Choices[0].Delta.Content
		}
		usage = response.Usage
		if response.Error != nil {
			t.Fatalf("unexpected error: %v", response.Error)
		}
	}
	if got != "北京" || count != 3 {
		t.Fatalf("got text %q across %d responses", got, count)
	}
	if usage == nil || usage.TotalTokens != 5 {
		t.Fatalf("unexpected usage: %#v", usage)
	}
}

func TestResponsesModelStreamsCompletedOutputFallbackAndUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"成功\"}]}],\"usage\":{\"input_tokens\":4,\"output_tokens\":2,\"total_tokens\":6}}}\n\n"))
	}))
	defer server.Close()

	responses, err := (&responsesModel{endpoint: server.URL + "/v1", model: "test"}).GenerateContent(context.Background(), &trpcmodel.Request{Messages: []trpcmodel.Message{{Content: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	var got string
	var usage *trpcmodel.Usage
	count := 0
	for response := range responses {
		count++
		if len(response.Choices) > 0 {
			got += response.Choices[0].Delta.Content
		}
		if response.Usage != nil {
			usage = response.Usage
		}
		if response.Error != nil {
			t.Fatalf("unexpected error: %v", response.Error)
		}
	}
	if got != "成功" || count != 2 {
		t.Fatalf("got text %q across %d responses", got, count)
	}
	if usage == nil || usage.TotalTokens != 6 {
		t.Fatalf("unexpected usage: %#v", usage)
	}
}

func TestResponsesModelStreamsOutputItemFallback(t *testing.T) {
	tests := []struct {
		name  string
		event string
	}{
		{
			name:  "content part done",
			event: "data: {\"type\":\"response.content_part.done\",\"part\":{\"type\":\"output_text\",\"text\":\"成功\"}}\n\n",
		},
		{
			name:  "output item done",
			event: "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"成功\"}]}}\n\n",
		},
		{
			name:  "refusal content",
			event: "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"refusal\",\"refusal\":\"拒绝\"}]}}\n\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(tt.event))
				_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":4,\"output_tokens\":2,\"total_tokens\":6}}}\n\n"))
			}))
			defer server.Close()

			responses, err := (&responsesModel{endpoint: server.URL + "/v1", model: "test"}).GenerateContent(context.Background(), &trpcmodel.Request{Messages: []trpcmodel.Message{{Content: "hello"}}})
			if err != nil {
				t.Fatal(err)
			}
			var got string
			var terminal *trpcmodel.Response
			for response := range responses {
				terminal = response
				if len(response.Choices) > 0 {
					got += response.Choices[0].Delta.Content
				}
				if response.Error != nil {
					t.Fatalf("unexpected error: %v", response.Error)
				}
			}
			if got == "" || terminal == nil || terminal.Usage == nil || terminal.Usage.TotalTokens != 6 {
				t.Fatalf("got text %q terminal %#v", got, terminal)
			}
		})
	}
}

func TestResponsesModelReportsFailedAndIncompleteEvents(t *testing.T) {
	tests := []struct {
		name     string
		event    string
		wantText string
	}{
		{
			name:     "failed",
			event:    "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\",\"message\":\"upstream unavailable\"}}}\n\n",
			wantText: "responses API failed: upstream unavailable",
		},
		{
			name:     "incomplete",
			event:    "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n",
			wantText: "responses API incomplete: max_output_tokens",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(tt.event))
			}))
			defer server.Close()
			responses, err := (&responsesModel{endpoint: server.URL + "/v1", model: "test"}).GenerateContent(context.Background(), &trpcmodel.Request{Messages: []trpcmodel.Message{{Content: "hello"}}})
			if err != nil {
				t.Fatal(err)
			}
			got := <-responses
			message := ""
			if got != nil && got.Error != nil {
				message = got.Error.Message
			}
			if got == nil || got.Error == nil || !strings.Contains(message, tt.wantText) || !got.Done {
				t.Fatalf("error response = %#v message=%q", got, message)
			}
			if _, ok := <-responses; ok {
				t.Fatal("response channel did not close")
			}
		})
	}
}

func TestResponsesModelReturnsHTTPAndTransportErrors(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		status   int
		wantText string
	}{
		{name: "http status", status: http.StatusBadGateway, wantText: "responses API returned status 502"},
		{name: "transport", endpoint: "://bad endpoint", wantText: "missing protocol scheme"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := tt.endpoint
			if endpoint == "" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "upstream", tt.status) }))
				defer server.Close()
				endpoint = server.URL
			}
			responses, err := (&responsesModel{endpoint: endpoint}).GenerateContent(context.Background(), &trpcmodel.Request{})
			if err != nil {
				t.Fatal(err)
			}
			got := <-responses
			message := ""
			if got != nil && got.Error != nil {
				message = got.Error.Message
			}
			if got == nil || got.Error == nil || !strings.Contains(message, tt.wantText) || !got.Done {
				t.Fatalf("error response = %#v message=%q", got, message)
			}
			if _, ok := <-responses; ok {
				t.Fatal("response channel did not close")
			}
		})
	}
}

func TestResponsesModelSkipsMalformedEventsAndReportsScannerErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "comment\n")
		_, _ = io.WriteString(w, "data: not-json\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n")
	}))
	defer server.Close()
	responses, err := (&responsesModel{endpoint: server.URL}).GenerateContent(context.Background(), &trpcmodel.Request{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for response := range responses {
		if len(response.Choices) > 0 {
			got = append(got, response.Choices[0].Delta.Content)
		}
	}
	if !bytes.Equal([]byte(strings.Join(got, "")), []byte("ok")) {
		t.Fatalf("text = %q", got)
	}

	reader := &errorReader{}
	_, err = consumeResponsesStream(context.Background(), bufio.NewScanner(reader), make(chan *trpcmodel.Response, 1))
	if !errors.Is(err, errReaderFailure) {
		t.Fatalf("scanner error = %v", err)
	}
}

func TestSendResponseHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sendResponse(ctx, make(chan *trpcmodel.Response), &trpcmodel.Response{}) {
		t.Fatal("sendResponse succeeded with canceled context")
	}
}

var errReaderFailure = errors.New("reader failure")

type errorReader struct{}

func (*errorReader) Read([]byte) (int, error) { return 0, errReaderFailure }

func stringPtr(value string) *string { return &value }

func TestResponsesModelRejectsEmptyCompletedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{}}\n\n"))
	}))
	defer server.Close()
	responses, err := (&responsesModel{endpoint: server.URL, model: "test"}).GenerateContent(context.Background(), &trpcmodel.Request{})
	if err != nil {
		t.Fatal(err)
	}
	var terminal *trpcmodel.Response
	for response := range responses {
		terminal = response
	}
	if terminal == nil || terminal.Error == nil {
		t.Fatalf("expected empty response error, got %#v", terminal)
	}
}

func TestResponsesModelSerializesFunctionTools(t *testing.T) {
	request := &trpcmodel.Request{
		Messages: []trpcmodel.Message{{Role: trpcmodel.RoleUser, Content: "send a test image"}},
		Tools: map[string]trpctool.Tool{
			"send_test_image": responsesTestTool{declaration: trpctool.Declaration{
				Name: "send_test_image", Description: "Queue a controlled test image.",
				InputSchema: &trpctool.Schema{Type: "object", AdditionalProperties: false},
			}},
		},
	}
	body, err := (&responsesModel{model: "test"}).requestBody(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Tools []responsesTool
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Tools) != 1 || decoded.Tools[0].Type != "function" || decoded.Tools[0].Name != "send_test_image" || decoded.Tools[0].Parameters == nil || decoded.Tools[0].Parameters.Type != "object" || decoded.Tools[0].Parameters.AdditionalProperties != false {
		t.Fatalf("serialized tools = %#v", decoded.Tools)
	}
}

func TestResponsesModelMapsFunctionCallHistory(t *testing.T) {
	input := responsesInput(&trpcmodel.Request{Messages: []trpcmodel.Message{
		{Role: trpcmodel.RoleAssistant, ToolCalls: []trpcmodel.ToolCall{{Type: "function", ID: "call-1", Function: trpcmodel.FunctionDefinitionParam{Name: "send_test_image", Arguments: []byte("{}")}}}},
		{Role: trpcmodel.RoleTool, ToolID: "call-1", ToolName: "send_test_image", Content: "{\"status\":\"queued\"}"},
	}})
	if len(input) != 2 || input[0].Type != "function_call" || input[0].CallID != "call-1" || input[0].Name != "send_test_image" || input[0].Arguments != "{}" || input[1].Type != "function_call_output" || input[1].CallID != "call-1" || input[1].Output != "{\"status\":\"queued\"}" {
		t.Fatalf("function history = %#v", input)
	}
}

func TestResponsesModelStreamsFunctionCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call-1\",\"name\":\"send_test_image\",\"arguments\":\"{}\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"function_call\",\"call_id\":\"call-1\",\"name\":\"send_test_image\",\"arguments\":\"{}\"}],\"usage\":{\"input_tokens\":4,\"output_tokens\":1,\"total_tokens\":5}}}\n\n"))
	}))
	defer server.Close()
	responses, err := (&responsesModel{endpoint: server.URL, model: "test"}).GenerateContent(context.Background(), &trpcmodel.Request{Messages: []trpcmodel.Message{{Content: "send a test image"}}})
	if err != nil {
		t.Fatal(err)
	}
	var terminal *trpcmodel.Response
	for response := range responses {
		terminal = response
	}
	if terminal == nil || !terminal.Done || terminal.Error != nil || terminal.Usage == nil || terminal.Usage.TotalTokens != 5 || len(terminal.Choices) != 1 || len(terminal.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("function call terminal = %#v", terminal)
	}
	call := terminal.Choices[0].Message.ToolCalls[0]
	if call.Type != "function" || call.ID != "call-1" || call.Function.Name != "send_test_image" || string(call.Function.Arguments) != "{}" {
		t.Fatalf("function call = %#v", call)
	}
}

type responsesTestTool struct {
	declaration trpctool.Declaration
}

func (tool responsesTestTool) Declaration() *trpctool.Declaration {
	return &tool.declaration
}
