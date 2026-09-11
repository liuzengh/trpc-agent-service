package wecommcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var connectMarkerPattern = regexp.MustCompile(`^CONNECT-[a-f0-9]{16}$`)

type ConnectGroup struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	LastActive string `json:"last_active"`
}
type ConnectMember struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Prefix string `json:"prefix"`
	Style  string `json:"style"`
}

func CheckConnection(ctx context.Context, endpoint string, client *http.Client) error {
	if ValidateEndpoint(endpoint) != nil {
		return errors.New("请复制企业微信消息 MCP 的完整连接地址")
	}
	var d Discovery
	var e error
	if client == nil {
		d, e = Discover(ctx, endpoint)
	} else {
		d, e = discover(ctx, endpoint, client)
	}
	if e != nil {
		return errors.New("无法连接企业微信，请检查连接地址是否有效")
	}
	has := map[string]bool{}
	for _, tool := range d.Tools {
		has[tool.Name] = true
	}
	if !has[sessionsTool] || !has[messagesTool] || !has[replyTool] {
		return errors.New("此连接没有提供机器人收发消息所需的权限")
	}
	return nil
}

// ListConnectGroups is invoked only after explicit browser consent. It returns
// group metadata, never message text, identity context or private chats.
func ListConnectGroups(ctx context.Context, endpoint string, client *http.Client) ([]ConnectGroup, error) {
	raw, bad, e := callLimited(ctx, endpoint, client, sessionsTool, nil)
	if e != nil || bad {
		return nil, errors.New("未能获取群列表，请稍后重试")
	}
	var data struct {
		Code     *int `json:"errcode"`
		More     bool `json:"has_more"`
		Sessions []struct {
			ID         string `json:"chat_id"`
			Name       string `json:"chat_name"`
			Type       string `json:"chat_type"`
			LastActive string `json:"last_msg_time"`
		} `json:"sessions"`
	}
	if json.Unmarshal(raw, &data) != nil || data.Code == nil || *data.Code != 0 || data.More || len(data.Sessions) > 200 {
		return nil, errors.New("群列表不完整，请先在企业微信收窄授权范围")
	}
	result := []ConnectGroup{}
	seen := map[string]bool{}
	for _, g := range data.Sessions {
		if g.Type == "group" && validIDs([]string{g.ID}, 1) && !seen[g.ID] {
			result = append(result, ConnectGroup{g.ID, g.Name, g.LastActive})
			seen[g.ID] = true
		}
	}
	return result, nil
}

// ReadConnectMember reads only the chosen group's short enrollment window.
// Nonmatching messages are discarded; no sample files or chat bodies are returned.
func ReadConnectMember(ctx context.Context, endpoint string, client *http.Client, chat, marker string, since time.Time) (ConnectMember, error) {
	var result ConnectMember
	if !validIDs([]string{chat}, 1) || len(marker) < 12 || time.Since(since) > 10*time.Minute {
		return result, errors.New("连接确认已过期，请重新选择群")
	}
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	args := map[string]any{"chat_id": chat, "begin_time": since.In(loc).Format("2006-01-02 15:04:05"), "end_time": time.Now().In(loc).Format("2006-01-02 15:04:05")}
	raw, bad, e := callLimited(ctx, endpoint, client, messagesTool, args)
	if e != nil || bad {
		return result, errors.New("未能检查确认消息，请稍后重试")
	}
	var data struct {
		Code     *int `json:"errcode"`
		More     bool `json:"has_more"`
		Messages []struct {
			ID   string `json:"userid"`
			Name string `json:"user_name"`
			Type string `json:"msg_type"`
			Text struct {
				Content string `json:"content"`
			} `json:"text"`
		} `json:"messages"`
	}
	if json.Unmarshal(raw, &data) != nil || data.Code == nil || *data.Code != 0 || data.More || len(data.Messages) > 1000 {
		return result, errors.New("这段时间消息过多，请重新生成确认消息后再试")
	}
	for _, m := range data.Messages {
		text := strings.TrimSpace(m.Text.Content)
		if m.Type != "text" || !strings.HasSuffix(text, marker) {
			continue
		}
		rawPrefix := strings.TrimSuffix(text, marker)
		prefix := strings.TrimSpace(rawPrefix)
		if !validIDs([]string{m.ID}, 1) || !strings.HasPrefix(prefix, "@") || len(prefix) < 2 || len(prefix) > 128 {
			continue
		}
		if result.ID != "" && (result.ID != m.ID || result.Prefix != prefix) {
			return ConnectMember{}, errors.New("有多个人发送了这条确认消息，请重新生成后由一人发送")
		}
		result = ConnectMember{ID: m.ID, Name: m.Name, Prefix: prefix, Style: "whitespace"}
		if prefix == rawPrefix {
			result.Style = "prefix"
		}
	}
	if result.ID == "" {
		return result, errors.New("还没找到确认消息，请在所选群里 @ 机器人并发送这段文字")
	}
	return result, nil
}
