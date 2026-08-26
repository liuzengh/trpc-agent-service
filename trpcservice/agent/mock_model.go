package agent

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

const tutorialModelName = "tutorial-mock-model"

var introductionPattern = regexp.MustCompile(
	`(?:我叫|我的名字是)\s*([^\s，。！？,.!?：:；;]{1,32})`,
)

// TutorialModel is a deterministic model used by the getting-started server.
// It needs no API key and deliberately reads request.Messages so the demo can
// show that Runner restores conversation history from Session.
type TutorialModel struct {
	responseSequence atomic.Uint64
}

// NewTutorialModel creates the local model used by the runnable tutorial.
func NewTutorialModel() *TutorialModel {
	return &TutorialModel{}
}

// Info implements model.Model.
func (m *TutorialModel) Info() model.Info {
	return model.Info{
		Name:          tutorialModelName,
		ContextWindow: 4096,
	}
}

// GenerateContent implements model.Model.
func (m *TutorialModel) GenerateContent(
	ctx context.Context,
	request *model.Request,
) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, fmt.Errorf("tutorial model: request is nil")
	}

	reply := tutorialReply(request.Messages)
	stop := "stop"
	response := &model.Response{
		ID:        fmt.Sprintf("tutorial-response-%d", m.responseSequence.Add(1)),
		Object:    model.ObjectTypeChatCompletion,
		Created:   time.Now().Unix(),
		Model:     tutorialModelName,
		Done:      true,
		IsPartial: false,
		Choices: []model.Choice{{
			Index:        0,
			Message:      model.NewAssistantMessage(reply),
			FinishReason: &stop,
		}},
	}

	responses := make(chan *model.Response, 1)
	responses <- response
	close(responses)
	return responses, nil
}

func tutorialReply(messages []model.Message) string {
	latestIndex, latest := latestUserMessage(messages)
	if latestIndex < 0 {
		return "请先发送一条消息。"
	}

	if asksForName(latest) {
		if name := rememberedName(messages[:latestIndex]); name != "" {
			return fmt.Sprintf("你叫%s。这个名字来自当前 Session 的历史消息。", name)
		}
		return "我还不知道你的名字。你可以告诉我：我叫小明。"
	}

	if name := extractName(latest); name != "" {
		return fmt.Sprintf("你好，%s。我已经把这句话保存在当前 Session 中。", name)
	}

	return fmt.Sprintf("我收到了：%s", latest)
}

func latestUserMessage(messages []model.Message) (int, string) {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != model.RoleUser {
			continue
		}
		if text := strings.TrimSpace(messages[i].Content); text != "" {
			return i, text
		}
	}
	return -1, ""
}

func asksForName(message string) bool {
	for _, phrase := range []string{
		"我叫什么",
		"我的名字是什么",
		"你记得我的名字",
		"你知道我是谁",
	} {
		if strings.Contains(message, phrase) {
			return true
		}
	}
	return false
}

func rememberedName(messages []model.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != model.RoleUser {
			continue
		}
		if name := extractName(messages[i].Content); name != "" {
			return name
		}
	}
	return ""
}

func extractName(message string) string {
	matches := introductionPattern.FindStringSubmatch(strings.TrimSpace(message))
	if len(matches) != 2 {
		return ""
	}
	name := matches[1]
	for _, invalidPrefix := range []string{"什么", "啥", "谁"} {
		if strings.HasPrefix(name, invalidPrefix) {
			return ""
		}
	}
	return name
}
