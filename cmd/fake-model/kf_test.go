package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// These tests pin the wire shapes the reliable path depends on: the jobs
// role's sync_msg client and the delivery role's send_msg client both talk to
// this stub through the same request/response shapes as the official API, and
// a drift here would only show up as a confusing E2E failure much later.

func postJSON(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func decodeKFBody[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out
}

func TestKfStubServesScriptedPages(t *testing.T) {
	srv, _ := startFake(t, modeOK, 0)
	script := `{"pages":[
		{"next_cursor":"C1","has_more":1,"messages":[{"msgid":"m1","content":"hello"}]},
		{"next_cursor":"C2","has_more":0,"messages":[{"msgid":"m2","content":"world"}]}
	],"reset":true}`
	if resp := postJSON(t, srv.URL+kfScriptPath, script); resp.StatusCode != http.StatusOK {
		t.Fatalf("set script = %d", resp.StatusCode)
	}

	// gettoken first, the way both roles do.
	tok := decodeKFBody[struct {
		AccessToken string `json:"access_token"`
		ErrCode     int    `json:"errcode"`
	}](t, postJSON(t, srv.URL+kfTokenPath, `{}`))
	if tok.ErrCode != 0 || tok.AccessToken != kfAccessToken {
		t.Fatalf("gettoken = %+v", tok)
	}

	page1 := decodeKFBody[struct {
		NextCursor string `json:"next_cursor"`
		HasMore    int    `json:"has_more"`
		MsgList    []struct {
			Msgid          string `json:"msgid"`
			ExternalUserID string `json:"external_userid"`
			Origin         int    `json:"origin"`
			MsgType        string `json:"msgtype"`
			Text           struct {
				Content string `json:"content"`
			} `json:"text"`
		} `json:"msg_list"`
	}](t, postJSON(t, srv.URL+kfSyncPath, `{"cursor":"","token":"T","limit":1000,"open_kfid":"wk1"}`))
	if page1.NextCursor != "C1" || page1.HasMore != 1 || len(page1.MsgList) != 1 {
		t.Fatalf("page1 = %+v", page1)
	}
	if page1.MsgList[0].Msgid != "m1" || page1.MsgList[0].Text.Content != "hello" ||
		page1.MsgList[0].MsgType != "text" || page1.MsgList[0].Origin != 3 {
		t.Fatalf("page1 message = %+v", page1.MsgList[0])
	}
	if page1.MsgList[0].ExternalUserID == "" {
		t.Fatal("a scripted message without an explicit user must get the default one")
	}

	// The queue is consumed in order: the second call gets page two.
	page2 := decodeKFBody[struct {
		NextCursor string `json:"next_cursor"`
		HasMore    int    `json:"has_more"`
		MsgList    []any  `json:"msg_list"`
	}](t, postJSON(t, srv.URL+kfSyncPath, `{"cursor":"C1","open_kfid":"wk1"}`))
	if page2.NextCursor != "C2" || page2.HasMore != 0 || len(page2.MsgList) != 1 {
		t.Fatalf("page2 = %+v", page2)
	}

	// Exhausted queue: an empty page, not an error.
	page3 := decodeKFBody[struct {
		MsgList []any `json:"msg_list"`
		ErrCode int   `json:"errcode"`
	}](t, postJSON(t, srv.URL+kfSyncPath, `{"cursor":"C2","open_kfid":"wk1"}`))
	if page3.ErrCode != 0 || len(page3.MsgList) != 0 {
		t.Fatalf("exhausted queue = %+v, want an empty ok page", page3)
	}
}

func TestKfStubSyncFaultInjection(t *testing.T) {
	srv, _ := startFake(t, modeOK, 0)
	script := `{"pages":[{"errcode":40001,"errmsg":"invalid credential"}],"reset":true}`
	postJSON(t, srv.URL+kfScriptPath, script).Body.Close()

	resp := decodeKFBody[struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}](t, postJSON(t, srv.URL+kfSyncPath, `{"open_kfid":"wk1"}`))
	if resp.ErrCode != 40001 || resp.ErrMsg != "invalid credential" {
		t.Fatalf("fault page = %+v", resp)
	}
}

func TestKfStubRecordsSendMsg(t *testing.T) {
	srv, _ := startFake(t, modeOK, 0)
	postJSON(t, srv.URL+kfScriptPath, `{"pages":[],"reset":true}`).Body.Close()

	for _, part := range []string{"你好", "世界"} {
		resp := decodeKFBody[struct {
			ErrCode int    `json:"errcode"`
			MsgID   string `json:"msgid"`
		}](t, postJSON(t, srv.URL+kfSendPath,
			`{"touser":"ext1","open_kfid":"wk1","msgtype":"text","text":{"content":"`+part+`"}}`))
		if resp.ErrCode != 0 || resp.MsgID != kfSentMsgID {
			t.Fatalf("send_msg = %+v", resp)
		}
	}

	var sent struct {
		Sent []kfSentMsg `json:"sent"`
	}
	resp, err := http.Get(srv.URL + kfSentPath)
	if err != nil {
		t.Fatalf("GET sent: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &sent); err != nil {
		t.Fatalf("decode sent: %v", err)
	}
	if len(sent.Sent) != 2 {
		t.Fatalf("sent = %+v, want 2 messages", sent.Sent)
	}
	if sent.Sent[0].ToUser != "ext1" || sent.Sent[0].OpenKfID != "wk1" || sent.Sent[0].Content != "你好" {
		t.Fatalf("sent[0] = %+v", sent.Sent[0])
	}

	// Reset clears the send log along with the queue, so a script case does
	// not inherit the previous case's deliveries.
	postJSON(t, srv.URL+kfScriptPath, `{"pages":[],"reset":true}`).Body.Close()
	resp2, err := http.Get(srv.URL + kfSentPath)
	if err != nil {
		t.Fatalf("GET sent after reset: %v", err)
	}
	defer resp2.Body.Close()
	body2, _ := io.ReadAll(resp2.Body)
	var after struct {
		Sent []kfSentMsg `json:"sent"`
	}
	_ = json.Unmarshal(body2, &after)
	if len(after.Sent) != 0 {
		t.Fatalf("after reset sent = %+v, want empty", after.Sent)
	}
}

// TestKfStubIsConcurrentSafe exercises simultaneous sync and send through one
// httptest server, the way the E2E's jobs and delivery roles hit it.
func TestKfStubIsConcurrentSafe(t *testing.T) {
	srv, _ := startFake(t, modeOK, 0)
	postJSON(t, srv.URL+kfScriptPath, `{"pages":[],"reset":true}`).Body.Close()

	var wg sync.WaitGroup
	srvURL := srv.URL
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			postJSON(t, srvURL+kfSyncPath, `{"open_kfid":"wk1"}`).Body.Close()
		}(i)
		go func() {
			defer wg.Done()
			postJSON(t, srvURL+kfSendPath,
				`{"touser":"u","open_kfid":"wk1","msgtype":"text","text":{"content":"x"}}`).Body.Close()
		}()
	}
	wg.Wait()

	var sent struct {
		Sent []kfSentMsg `json:"sent"`
	}
	resp, err := http.Get(srvURL + kfSentPath)
	if err != nil {
		t.Fatalf("GET sent: %v", err)
	}
	defer resp.Body.Close()
	_ = json.NewDecoder(resp.Body).Decode(&sent)
	if len(sent.Sent) != 8 {
		t.Fatalf("concurrent sends recorded %d, want 8", len(sent.Sent))
	}
}
