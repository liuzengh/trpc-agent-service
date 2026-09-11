package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/netpolicy"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	PresentCardToolName = "platform.present_card"
	maxCardTitleRunes   = 120
	maxCardBodyRunes    = 4000
	maxCardActions      = 3
	maxCardLabelRunes   = 80
)

type PresentCardTool struct{}

type presentCardArguments struct {
	Title   string              `json:"title"`
	Body    string              `json:"body"`
	Actions []presentCardAction `json:"actions"`
}

type presentCardAction struct {
	Label string `json:"label"`
	URL   string `json:"url"`
	Style string `json:"style,omitempty"`
}

func NewPresentCardTool() *PresentCardTool { return &PresentCardTool{} }

func (*PresentCardTool) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{
		Name:        PresentCardToolName,
		Description: "Present a compact result card to the user. Use HTTPS links only; this tool cannot create approval or callback actions.",
		InputSchema: &agenttool.Schema{
			Type:                 "object",
			Required:             []string{"body"},
			AdditionalProperties: false,
			Properties: map[string]*agenttool.Schema{
				"title": {Type: "string", Description: "Short optional card title."},
				"body":  {Type: "string", Description: "Card body shown to the user."},
				"actions": {
					Type: "array", Description: "Up to three HTTPS link actions.",
					Items: &agenttool.Schema{Type: "object", Required: []string{"label", "url"}, AdditionalProperties: false, Properties: map[string]*agenttool.Schema{
						"label": {Type: "string"},
						"url":   {Type: "string"},
						"style": {Type: "string", Enum: []any{"default", "primary"}},
					}},
				},
			},
		},
	}
}

func (*PresentCardTool) Call(ctx context.Context, raw []byte) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	card, err := parsePresentedCard(raw)
	if err != nil {
		return nil, err
	}
	if err := recordPresentedCard(ctx, card); err != nil {
		return nil, err
	}
	return map[string]any{"presented": true}, nil
}

func parsePresentedCard(raw []byte) (channels.InteractiveCard, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var arguments presentCardArguments
	if err := decoder.Decode(&arguments); err != nil {
		return channels.InteractiveCard{}, fmt.Errorf("decode present card arguments: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return channels.InteractiveCard{}, err
	}
	return validatePresentedCard(arguments)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return fmt.Errorf("decode present card arguments: %w", err)
	}
	return errors.New("decode present card arguments: multiple JSON values are not allowed")
}

func validatePresentedCard(arguments presentCardArguments) (channels.InteractiveCard, error) {
	title := strings.TrimSpace(arguments.Title)
	body := strings.TrimSpace(arguments.Body)
	if body == "" {
		return channels.InteractiveCard{}, errors.New("present card body is required")
	}
	if utf8.RuneCountInString(title) > maxCardTitleRunes || utf8.RuneCountInString(body) > maxCardBodyRunes {
		return channels.InteractiveCard{}, errors.New("present card content exceeds display limits")
	}
	if len(arguments.Actions) > maxCardActions {
		return channels.InteractiveCard{}, fmt.Errorf("present card supports at most %d actions", maxCardActions)
	}
	actions := make([]channels.CardAction, 0, len(arguments.Actions))
	for _, input := range arguments.Actions {
		label := strings.TrimSpace(input.Label)
		url := strings.TrimSpace(input.URL)
		style := strings.TrimSpace(input.Style)
		if label == "" || utf8.RuneCountInString(label) > maxCardLabelRunes {
			return channels.InteractiveCard{}, errors.New("present card action label is invalid")
		}
		if err := netpolicy.ValidatePublicHTTPSURL(url); err != nil {
			return channels.InteractiveCard{}, fmt.Errorf("present card action URL: %w", err)
		}
		if style == "" {
			style = "default"
		}
		if style != "default" && style != "primary" {
			return channels.InteractiveCard{}, errors.New("present card action style must be default or primary")
		}
		actions = append(actions, channels.CardAction{Label: label, URL: url, Style: style})
	}
	return channels.InteractiveCard{Title: title, Body: body, Actions: actions}, nil
}

var _ agenttool.CallableTool = (*PresentCardTool)(nil)
