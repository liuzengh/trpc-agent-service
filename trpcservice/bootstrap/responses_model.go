package bootstrap

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type responsesModel struct {
	apiKey   string
	endpoint string
	model    string
	client   *http.Client
}

func (m *responsesModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: m.model} }
func (m *responsesModel) GenerateContent(ctx context.Context, request *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	if ctx == nil {
		return nil, fmt.Errorf("responses model context is required")
	}
	if request == nil {
		return nil, fmt.Errorf("responses model request is required")
	}
	out := make(chan *trpcmodel.Response, 8)
	go m.stream(ctx, request, out)
	return out, nil
}

type responsesInputItem struct {
	Type      string                 `json:"type,omitempty"`
	Role      string                 `json:"role,omitempty"`
	Content   []responsesContentPart `json:"content,omitempty"`
	CallID    string                 `json:"call_id,omitempty"`
	Name      string                 `json:"name,omitempty"`
	Arguments string                 `json:"arguments,omitempty"`
	Output    string                 `json:"output,omitempty"`
}
type responsesContentPart struct {
	Type       string               `json:"type"`
	Text       string               `json:"text,omitempty"`
	ImageURL   string               `json:"image_url,omitempty"`
	Detail     string               `json:"detail,omitempty"`
	FileID     string               `json:"file_id,omitempty"`
	FileURL    string               `json:"file_url,omitempty"`
	FileData   string               `json:"file_data,omitempty"`
	Filename   string               `json:"filename,omitempty"`
	InputAudio *responsesInputAudio `json:"input_audio,omitempty"`
}
type responsesInputAudio struct {
	Data   string `json:"data"`
	Format string `json:"format"`
}

type responsesTool struct {
	Type        string           `json:"type"`
	Name        string           `json:"name"`
	Description string           `json:"description,omitempty"`
	Parameters  *trpctool.Schema `json:"parameters"`
}

func (m *responsesModel) stream(ctx context.Context, request *trpcmodel.Request, out chan<- *trpcmodel.Response) {
	defer close(out)
	body, err := m.requestBody(request)
	if err != nil {
		m.emitError(out, err)
		return
	}
	resp, err := m.doRequest(ctx, body)
	if err != nil {
		m.emitError(out, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		m.emitError(out, fmt.Errorf("responses API returned status %d", resp.StatusCode))
		return
	}
	state, err := consumeResponsesStream(ctx, bufio.NewScanner(resp.Body), out)
	if err != nil {
		m.emitError(out, err)
		return
	}
	terminal := &trpcmodel.Response{Object: "response", Done: true, Usage: state.usage}
	if len(state.toolCalls) > 0 {
		terminal.Choices = []trpcmodel.Choice{{Message: trpcmodel.Message{Role: trpcmodel.RoleAssistant, ToolCalls: state.toolCalls}}}
	}
	if !state.emittedText && len(state.toolCalls) == 0 {
		terminal.Error = &trpcmodel.ResponseError{Message: "responses API completed without output text", Type: trpcmodel.ErrorTypeAPIError}
	}
	sendResponse(ctx, out, terminal)
}

func (m *responsesModel) requestBody(request *trpcmodel.Request) ([]byte, error) {
	body := map[string]any{
		"model":  m.model,
		"input":  responsesInput(request),
		"store":  false,
		"stream": true,
	}
	if tools := responsesTools(request); len(tools) > 0 {
		body["tools"] = tools
	}
	return json.Marshal(body)
}

func responsesInput(request *trpcmodel.Request) []responsesInputItem {
	if request == nil {
		return nil
	}
	input := make([]responsesInputItem, 0, len(request.Messages))
	for _, message := range request.Messages {
		input = append(input, responsesMessageInput(message)...)
	}
	return input
}

func responsesMessageInput(message trpcmodel.Message) []responsesInputItem {
	if message.Role == trpcmodel.RoleTool && strings.TrimSpace(message.ToolID) != "" {
		return []responsesInputItem{{Type: "function_call_output", CallID: strings.TrimSpace(message.ToolID), Output: message.Content}}
	}
	if len(message.ToolCalls) > 0 {
		input := make([]responsesInputItem, 0, len(message.ToolCalls)+1)
		if message.Content != "" {
			input = append(input, responsesTextMessage(message))
		}
		for _, call := range message.ToolCalls {
			if strings.TrimSpace(call.ID) == "" || strings.TrimSpace(call.Function.Name) == "" {
				continue
			}
			input = append(input, responsesInputItem{Type: "function_call", CallID: strings.TrimSpace(call.ID), Name: strings.TrimSpace(call.Function.Name), Arguments: string(call.Function.Arguments)})
		}
		return input
	}
	return []responsesInputItem{responsesTextMessage(message)}
}

func responsesTextMessage(message trpcmodel.Message) responsesInputItem {
	role := string(message.Role)
	if role == "" {
		role = string(trpcmodel.RoleUser)
	}
	return responsesInputItem{Role: role, Content: responsesContent(message)}
}

func responsesTools(request *trpcmodel.Request) []responsesTool {
	if request == nil || len(request.Tools) == 0 {
		return nil
	}
	names := make([]string, 0, len(request.Tools))
	for name := range request.Tools {
		names = append(names, name)
	}
	sort.Strings(names)
	tools := make([]responsesTool, 0, len(names))
	for _, name := range names {
		tool := request.Tools[name]
		if tool == nil || tool.Declaration() == nil {
			continue
		}
		declaration := tool.Declaration()
		if strings.TrimSpace(declaration.Name) == "" || declaration.InputSchema == nil {
			continue
		}
		tools = append(tools, responsesTool{Type: "function", Name: declaration.Name, Description: declaration.Description, Parameters: declaration.InputSchema})
	}
	return tools
}

func responsesContent(message trpcmodel.Message) []responsesContentPart {
	parts := make([]responsesContentPart, 0, 1+len(message.ContentParts))
	text := message.Content
	if text == "" {
		for _, part := range message.ContentParts {
			if part.Text != nil {
				text += *part.Text
			}
		}
	}
	if text != "" || len(message.ContentParts) == 0 {
		parts = append(parts, responsesTextPart(text))
	}
	for _, part := range message.ContentParts {
		if part.Type == trpcmodel.ContentTypeText || part.Text != nil {
			continue
		}
		if converted, ok := responsesContentFromPart(part); ok {
			parts = append(parts, converted)
		}
	}
	if len(parts) == 0 {
		parts = append(parts, responsesTextPart(""))
	}
	return parts
}

func responsesContentFromPart(part trpcmodel.ContentPart) (responsesContentPart, bool) {
	switch part.Type {
	case trpcmodel.ContentTypeImage:
		if part.Image == nil {
			return responsesTextPart("[image attachment omitted: image data is missing]"), true
		}
		if strings.TrimSpace(part.Image.URL) != "" {
			return responsesContentPart{Type: "input_image", ImageURL: strings.TrimSpace(part.Image.URL), Detail: part.Image.Detail}, true
		}
		if len(part.Image.Data) == 0 {
			return responsesTextPart("[image attachment omitted: image data is missing]"), true
		}
		return responsesContentPart{Type: "input_image", ImageURL: dataURL("image", part.Image.Format, part.Image.Data), Detail: part.Image.Detail}, true
	case trpcmodel.ContentTypeFile:
		return responsesFilePart(part.File)
	case trpcmodel.ContentTypeAudio:
		return responsesAudioPart(part.Audio)
	case trpcmodel.ContentTypeVideo:
		return responsesTextPart("[video attachment omitted: model video input is not enabled]"), true
	default:
		return responsesContentPart{}, false
	}
}

func responsesFilePart(file *trpcmodel.File) (responsesContentPart, bool) {
	if file == nil {
		return responsesTextPart("[file attachment omitted: file data is missing]"), true
	}
	if id := strings.TrimSpace(file.FileID); id != "" {
		return responsesContentPart{Type: "input_file", FileID: id}, true
	}
	if fileURL := strings.TrimSpace(file.URL); fileURL != "" {
		return responsesContentPart{Type: "input_file", FileURL: fileURL}, true
	}
	if len(file.Data) == 0 {
		return responsesTextPart("[file attachment omitted: file data is missing]"), true
	}
	return responsesContentPart{Type: "input_file", FileData: dataURL("application", file.MimeType, file.Data), Filename: responsesFilename(file.Name)}, true
}

func responsesAudioPart(audio *trpcmodel.Audio) (responsesContentPart, bool) {
	if audio == nil || len(audio.Data) == 0 {
		return responsesTextPart("[audio attachment omitted: audio data is missing]"), true
	}
	format := responsesAudioFormat(audio.Format)
	if format == "" {
		return responsesTextPart("[audio attachment omitted: unsupported audio format]"), true
	}
	return responsesContentPart{Type: "input_audio", InputAudio: &responsesInputAudio{Data: base64.StdEncoding.EncodeToString(audio.Data), Format: format}}, true
}

func responsesTextPart(text string) responsesContentPart {
	return responsesContentPart{Type: "input_text", Text: text}
}

func dataURL(defaultFamily, format string, data []byte) string {
	mediaType := strings.ToLower(strings.TrimSpace(format))
	if mediaType == "" {
		mediaType = defaultFamily + "/octet-stream"
	} else if !strings.Contains(mediaType, "/") {
		mediaType = defaultFamily + "/" + strings.TrimPrefix(mediaType, ".")
	}
	return "data:" + mediaType + ";base64," + base64.StdEncoding.EncodeToString(data)
}

func responsesFilename(name string) string {
	if trimmed := strings.TrimSpace(name); trimmed != "" {
		return trimmed
	}
	return "attachment"
}

func responsesAudioFormat(format string) string {
	value := strings.ToLower(strings.TrimSpace(format))
	if _, subtype, ok := strings.Cut(value, "/"); ok {
		value = subtype
	}
	switch value {
	case "mpeg", "mpga":
		return "mp3"
	case "x-wav", "wave":
		return "wav"
	case "mp3", "wav":
		return value
	default:
		return ""
	}
}

func (m *responsesModel) doRequest(ctx context.Context, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(m.endpoint, "/")+"/responses", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+m.apiKey)
	req.Header.Set("Content-Type", "application/json")
	client := m.client
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req)
}

type responsesEvent struct {
	Type     string                     `json:"type"`
	Delta    string                     `json:"delta"`
	Text     string                     `json:"text"`
	Response responsesCompletedResponse `json:"response"`
	Item     responsesOutputItem        `json:"item"`
	Part     responsesOutputContent     `json:"part"`
}
type responsesCompletedResponse struct {
	Status string          `json:"status"`
	Error  *responsesError `json:"error"`
	Usage  *struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
		TotalTokens  int64 `json:"total_tokens"`
	} `json:"usage"`
	IncompleteDetails *responsesIncompleteDetails `json:"incomplete_details"`
	Output            []responsesOutputItem       `json:"output"`
	OutputText        string                      `json:"output_text"`
}
type responsesOutputItem struct {
	Type      string                   `json:"type"`
	Status    string                   `json:"status"`
	CallID    string                   `json:"call_id"`
	Name      string                   `json:"name"`
	Arguments string                   `json:"arguments"`
	Content   []responsesOutputContent `json:"content"`
}
type responsesOutputContent struct {
	Text    string `json:"text"`
	Refusal string `json:"refusal"`
}
type responsesError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}
type responsesIncompleteDetails struct {
	Reason string `json:"reason"`
}

type responsesStreamState struct {
	usage       *trpcmodel.Usage
	emittedText bool
	toolCalls   []trpcmodel.ToolCall
	callIDs     map[string]struct{}
}

func consumeResponsesStream(ctx context.Context, scanner *bufio.Scanner, out chan<- *trpcmodel.Response) (responsesStreamState, error) {
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	state := responsesStreamState{callIDs: make(map[string]struct{})}
	for scanner.Scan() {
		event, ok := parseResponsesEvent(scanner.Text())
		if !ok {
			continue
		}
		continueReading, err := consumeResponsesEvent(ctx, out, event, &state)
		if err != nil {
			return state, err
		}
		if !continueReading {
			return state, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return state, err
	}
	return state, nil
}

func consumeResponsesEvent(ctx context.Context, out chan<- *trpcmodel.Response, event responsesEvent, state *responsesStreamState) (bool, error) {
	if event.Type == "response.failed" {
		return false, fmt.Errorf("responses API failed: %s", event.Response.errorText())
	}
	if event.Type == "response.incomplete" {
		return false, fmt.Errorf("responses API incomplete: %s", event.Response.incompleteReason())
	}
	state.addToolCalls(event)
	state.setUsage(event)
	if event.Type == "response.output_text.delta" && event.Delta != "" {
		state.emittedText = true
		return sendResponse(ctx, out, responsesTextDelta(event.Type, event.Delta)), nil
	}
	if state.emittedText {
		return true, nil
	}
	object, text := responsesEventText(event)
	if text == "" {
		return true, nil
	}
	state.emittedText = true
	return sendResponse(ctx, out, responsesTextDelta(object, text)), nil
}

func (state *responsesStreamState) addToolCalls(event responsesEvent) {
	if event.Type == "response.output_item.done" {
		state.addToolCall(event.Item.toolCall())
	}
	if event.Type == "response.completed" {
		for _, item := range event.Response.Output {
			state.addToolCall(item.toolCall())
		}
	}
}

func (state *responsesStreamState) addToolCall(call trpcmodel.ToolCall, ok bool) {
	if !ok {
		return
	}
	key := call.ID
	if _, found := state.callIDs[key]; found {
		return
	}
	state.callIDs[key] = struct{}{}
	state.toolCalls = append(state.toolCalls, call)
}

func (state *responsesStreamState) setUsage(event responsesEvent) {
	if event.Type != "response.completed" || event.Response.Usage == nil {
		return
	}
	usage := event.Response.Usage
	state.usage = &trpcmodel.Usage{PromptTokens: int(usage.InputTokens), CompletionTokens: int(usage.OutputTokens), TotalTokens: int(usage.TotalTokens)}
}

func responsesEventText(event responsesEvent) (string, string) {
	switch event.Type {
	case "response.output_text.delta":
		return event.Type, event.Delta
	case "response.output_text.done":
		return event.Type, event.Text
	case "response.content_part.done":
		return event.Type, event.Part.outputText()
	case "response.output_item.done":
		return event.Type, event.Item.outputText()
	case "response.completed":
		return event.Type, event.Response.outputText()
	default:
		return "", ""
	}
}

func (response responsesCompletedResponse) outputText() string {
	if response.OutputText != "" {
		return response.OutputText
	}
	var builder strings.Builder
	for _, item := range response.Output {
		builder.WriteString(item.outputText())
	}
	return builder.String()
}
func (item responsesOutputItem) outputText() string {
	var builder strings.Builder
	for _, content := range item.Content {
		builder.WriteString(content.outputText())
	}
	return builder.String()
}

func (item responsesOutputItem) toolCall() (trpcmodel.ToolCall, bool) {
	if item.Type != "function_call" || strings.TrimSpace(item.CallID) == "" || strings.TrimSpace(item.Name) == "" {
		return trpcmodel.ToolCall{}, false
	}
	return trpcmodel.ToolCall{Type: "function", ID: strings.TrimSpace(item.CallID), Function: trpcmodel.FunctionDefinitionParam{Name: strings.TrimSpace(item.Name), Arguments: []byte(item.Arguments)}}, true
}
func (content responsesOutputContent) outputText() string {
	if content.Text != "" {
		return content.Text
	}
	return content.Refusal
}
func (response responsesCompletedResponse) errorText() string {
	if response.Error == nil {
		return "unknown error"
	}
	if response.Error.Message != "" {
		return response.Error.Message
	}
	if response.Error.Code != "" {
		return response.Error.Code
	}
	if response.Error.Type != "" {
		return response.Error.Type
	}
	return "unknown error"
}
func (response responsesCompletedResponse) incompleteReason() string {
	if response.IncompleteDetails != nil && response.IncompleteDetails.Reason != "" {
		return response.IncompleteDetails.Reason
	}
	if response.Status != "" {
		return response.Status
	}
	return "unknown reason"
}

func responsesTextDelta(object, text string) *trpcmodel.Response {
	return &trpcmodel.Response{Object: object, IsPartial: true, Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Role: trpcmodel.RoleAssistant, Content: text}}}}
}

func parseResponsesEvent(line string) (responsesEvent, bool) {
	if !strings.HasPrefix(line, "data: ") {
		return responsesEvent{}, false
	}
	data := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
	if data == "" || data == "[DONE]" {
		return responsesEvent{}, false
	}
	var event responsesEvent
	if json.Unmarshal([]byte(data), &event) != nil {
		return responsesEvent{}, false
	}
	return event, true
}
func (m *responsesModel) emitError(out chan<- *trpcmodel.Response, err error) {
	out <- &trpcmodel.Response{Done: true, Error: &trpcmodel.ResponseError{Message: err.Error(), Type: trpcmodel.ErrorTypeAPIError}}
}
func sendResponse(ctx context.Context, out chan<- *trpcmodel.Response, response *trpcmodel.Response) bool {
	select {
	case out <- response:
		return true
	case <-ctx.Done():
		return false
	}
}
