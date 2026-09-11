package main

// The 企业微信 (WeCom) stub of fake-model: the app-message send endpoint the
// reliable delivery path posts to, plus a control path that lists what was
// sent. It exists for the same reason the KF stub does — the compose stack
// has no real WeCom credentials — and it mirrors the official request shape
// (touser / msgtype / agentid / text.content) so the sender's payload
// decoding runs against the same keys production does.

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

const (
	wecomSendPath = "/cgi-bin/message/send"
	wecomSentPath = "/__wecom_sent"
)

// wecomSentMsg is one recorded message/send delivery.
type wecomSentMsg struct {
	ToUser  string `json:"touser"`
	AgentID int    `json:"agentid"`
	Content string `json:"content"`
}

// wecomState is the stub's mutable side, guarded because the handlers run on
// separate goroutines.
type wecomState struct {
	mu   sync.Mutex
	sent []wecomSentMsg
	log  *log.Logger
}

func (s *server) handleWeCom(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case wecomSentPath:
		s.wecom.getSent(w, r)
	case wecomSendPath:
		s.wecom.sendMsg(w, r)
	default:
		writeErr(w, http.StatusNotFound, "unknown WeCom endpoint "+r.URL.Path)
	}
}

func (s *wecomState) sendMsg(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST a message/send request")
		return
	}
	var req struct {
		ToUser  string `json:"touser"`
		MsgType string `json:"msgtype"`
		AgentID int    `json:"agentid"`
		Text    struct {
			Content string `json:"content"`
		} `json:"text"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "decode message/send: "+err.Error())
		return
	}
	s.mu.Lock()
	s.sent = append(s.sent, wecomSentMsg{ToUser: req.ToUser, AgentID: req.AgentID, Content: req.Text.Content})
	total := len(s.sent)
	s.mu.Unlock()
	s.log.Printf("wecom: message/send #%d to=%q agentid=%d bytes=%d",
		total, req.ToUser, req.AgentID, len(req.Text.Content))
	writeJSON(w, http.StatusOK, map[string]any{"errcode": 0, "errmsg": "ok", "msgid": "wecom-sent-1"})
}

func (s *wecomState) getSent(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	sent := append([]wecomSentMsg(nil), s.sent...)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"sent": sent})
}
