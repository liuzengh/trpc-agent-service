package wecom

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// fakeWeComAPI fakes gettoken and message/send for Send tests.
type fakeWeComAPI struct {
	tokenCalls  atomic.Int32
	sendCalls   atomic.Int32
	lastBodies  []string
	groupBodies []string
	failNextTok bool // answer the next send with errcode 40014 (expired token)
}

func (f *fakeWeComAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/cgi-bin/gettoken", func(w http.ResponseWriter, r *http.Request) {
		f.tokenCalls.Add(1)
		if !strings.Contains(r.URL.RawQuery, "corpsecret=corp-secret") {
			_, _ = w.Write([]byte(`{"errcode":40001,"errmsg":"invalid secret"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"access_token":"tok","expires_in":7200}`))
	})
	mux.HandleFunc("/cgi-bin/message/send", func(w http.ResponseWriter, r *http.Request) {
		f.sendCalls.Add(1)
		var body json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.lastBodies = append(f.lastBodies, string(body))
		if f.failNextTok {
			f.failNextTok = false
			_, _ = w.Write([]byte(`{"errcode":40014,"errmsg":"invalid access_token"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	})
	mux.HandleFunc("/cgi-bin/appchat/send", func(w http.ResponseWriter, r *http.Request) {
		f.sendCalls.Add(1)
		var body json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.groupBodies = append(f.groupBodies, string(body))
		if !strings.Contains(string(body), `"chatid"`) {
			_, _ = w.Write([]byte(`{"errcode":40013,"errmsg":"missing chatid"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	})
	return mux
}

func TestSendCachesToken(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	for i := 0; i < 2; i++ {
		if err := c.Send(t.Context(), channels.OutboundMessage{
			Channel: "wecom", MsgID: fmt.Sprint(i), UserID: "zhangsan", Text: "你好",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if n := fake.tokenCalls.Load(); n != 1 {
		t.Fatalf("token must be fetched once and cached, got %d fetches", n)
	}
	if n := fake.sendCalls.Load(); n != 2 {
		t.Fatalf("want 2 sends, got %d", n)
	}
	if !strings.Contains(fake.lastBodies[0], `"touser":"zhangsan"`) ||
		!strings.Contains(fake.lastBodies[0], `"agentid":1000002`) {
		t.Fatalf("bad send payload: %s", fake.lastBodies[0])
	}
}

func TestSendRefreshesExpiredToken(t *testing.T) {
	fake := &fakeWeComAPI{failNextTok: true}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好",
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.tokenCalls.Load(); n != 2 {
		t.Fatalf("expired token must trigger exactly one refresh, got %d fetches", n)
	}
}

func TestSendSplitsLongText(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	long := strings.Repeat("汉", 1500) // 4500 bytes → 3 segments
	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: long,
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.sendCalls.Load(); n != 3 {
		t.Fatalf("want 3 segments, got %d sends", n)
	}
	var joined strings.Builder
	for _, body := range fake.lastBodies {
		var payload struct {
			Text struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if err := json.Unmarshal([]byte(body), &payload); err != nil {
			t.Fatal(err)
		}
		joined.WriteString(payload.Text.Content)
	}
	if joined.String() != long {
		t.Fatal("segments must reassemble into the original text")
	}
}

// A mid-split failure makes the sender retry the whole reply from segment 1;
// the platform's touser+content duplicate check absorbs the already-delivered
// prefix segments, so message/send must ask for it.
func TestSendEnablesDuplicateCheck(t *testing.T) {
	fake := &scriptWecomAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好",
	}); err != nil {
		t.Fatal(err)
	}
	sends := fake.sends()
	if len(sends) != 1 {
		t.Fatalf("want 1 send, got %d", len(sends))
	}
	if !strings.Contains(sends[0], `"enable_duplicate_check":1`) ||
		!strings.Contains(sends[0], `"duplicate_check_interval":1800`) {
		t.Fatalf("message/send must request the platform duplicate check: %s", sends[0])
	}
}

// An empty reply must not reach the platform at all: no token fetch, no send,
// no error.
func TestSendEmptyTextSkipsAPI(t *testing.T) {
	fake := &scriptWecomAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	for _, text := range []string{"", "   ", "\n\t"} {
		if err := c.Send(t.Context(), channels.OutboundMessage{
			Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: text,
		}); err != nil {
			t.Fatalf("empty reply must succeed silently, got %v", err)
		}
	}
	if n := fake.tokenCalls(); n != 0 {
		t.Fatalf("empty replies must not fetch a token, got %d fetches", n)
	}
	if n := len(fake.sends()); n != 0 {
		t.Fatalf("empty replies must not hit message/send, got %d sends", n)
	}
}

func TestSendGroupUsesAppchat(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", ChatID: "roomA", Text: "群公告",
	}); err != nil {
		t.Fatal(err)
	}
	if n := fake.sendCalls.Load(); n != 1 {
		t.Fatalf("want 1 appchat send, got %d", n)
	}
}

// Refreshing an already-cached identity must reuse its LRU node: pushing a
// second node would grow the list without a matching map entry, and an
// eviction popping the orphan would delete the LIVE token entry. Two
// refreshes of one identity keep exactly one node, and evictions afterwards
// never drop the refreshed identity.
func TestTokenRefreshReusesLRUNode(t *testing.T) {
	fake := &scriptWecomAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)
	ctx := t.Context()

	get := func(corpID, secretRef string) {
		t.Helper()
		if _, err := c.getAccessToken(ctx, corpID, secretRef); err != nil {
			t.Fatal(err)
		}
	}
	expire := func(key string) {
		t.Helper()
		c.tokenMu.Lock()
		defer c.tokenMu.Unlock()
		e := c.tokens[key]
		e.expiry = time.Now().Add(-time.Minute)
		c.tokens[key] = e
	}
	state := func() (listLen, mapLen int) {
		t.Helper()
		c.tokenMu.Lock()
		defer c.tokenMu.Unlock()
		return c.tokenOrder.Len(), len(c.tokens)
	}
	cached := func(key string) bool {
		t.Helper()
		c.tokenMu.Lock()
		defer c.tokenMu.Unlock()
		_, ok := c.tokens[key]
		return ok
	}

	key := testCorpID + "|secret"
	get(testCorpID, "secret")
	expire(key)
	get(testCorpID, "secret") // refresh path
	if listLen, mapLen := state(); listLen != 1 || mapLen != 1 {
		t.Fatalf("refreshing one identity must keep one LRU node, got list=%d map=%d", listLen, mapLen)
	}

	// Fill the cache to capacity with other identities.
	for i := 0; i < maxTokenCacheEntries-1; i++ {
		get(fmt.Sprintf("corp-%d", i), "secret")
	}
	// Refreshing the original identity while full must not evict an innocent
	// one — the eviction loop only runs for a NEW identity.
	expire(key)
	get(testCorpID, "secret")
	if listLen, mapLen := state(); listLen != maxTokenCacheEntries || mapLen != maxTokenCacheEntries {
		t.Fatalf("a refresh must not grow or shrink the cache, got list=%d map=%d", listLen, mapLen)
	}
	if !cached("corp-0|secret") {
		t.Fatal("refreshing a cached identity at capacity must not evict another identity")
	}

	// One genuinely new identity evicts the LRU back (corp-0), never the
	// just-refreshed identity.
	get("corp-new", "secret")
	if listLen, mapLen := state(); listLen != maxTokenCacheEntries || mapLen != maxTokenCacheEntries {
		t.Fatalf("eviction must keep the cache bounded, got list=%d map=%d", listLen, mapLen)
	}
	if !cached(key) {
		t.Fatal("eviction must not drop the recently refreshed identity")
	}
	if cached("corp-0|secret") {
		t.Fatal("the least recently used identity must be the eviction victim")
	}
}

func TestSplitTextRespectsRuneBoundaries(t *testing.T) {
	s := strings.Repeat("汉", 3) + strings.Repeat("a", 10)
	segs := splitText(s, 5)
	for _, seg := range segs {
		if len(seg) > 5 {
			t.Fatalf("segment too long: %d bytes", len(seg))
		}
	}
	if strings.Join(segs, "") != s {
		t.Fatal("segments must reassemble losslessly")
	}
}

// A card reply to a direct chat goes out as one template_card (text_notice)
// message; the URL turns it into a jump link (card_action type 1).
func TestSendCardDirectChat(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan",
		Text: "兜底文案",
		Card: &channels.Card{Title: "危险操作待确认", Desc: "工具：op_a", URL: "https://example.com/detail"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := fake.sendCalls.Load(); n != 1 {
		t.Fatalf("a card must be one send, got %d", n)
	}
	body := fake.lastBodies[0]
	var payload struct {
		ToUser  string `json:"touser"`
		MsgType string `json:"msgtype"`
		AgentID int    `json:"agentid"`
		Card    struct {
			CardType  string `json:"card_type"`
			MainTitle struct {
				Title string `json:"title"`
				Desc  string `json:"desc"`
			} `json:"main_title"`
			CardAction struct {
				Type int    `json:"type"`
				URL  string `json:"url"`
			} `json:"card_action"`
		} `json:"template_card"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.MsgType != "template_card" || payload.Card.CardType != "text_notice" {
		t.Fatalf("want a text_notice template_card, got %s", body)
	}
	if payload.ToUser != "zhangsan" || payload.AgentID != 1000002 {
		t.Fatalf("routing fields lost: %s", body)
	}
	if payload.Card.MainTitle.Title != "危险操作待确认" || payload.Card.MainTitle.Desc != "工具：op_a" {
		t.Fatalf("main_title mismatch: %s", body)
	}
	if payload.Card.CardAction.Type != 1 || payload.Card.CardAction.URL != "https://example.com/detail" {
		t.Fatalf("card_action mismatch: %s", body)
	}
	if !strings.Contains(body, `"enable_duplicate_check":1`) {
		t.Fatalf("the card send must keep the platform duplicate check: %s", body)
	}
}

// Without a URL the card is a plain notice: no card_action key at all.
func TestSendCardWithoutURL(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan",
		Text: "兜底文案",
		Card: &channels.Card{Title: "危险操作待确认", Desc: "工具：op_a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := fake.lastBodies[0]
	if !strings.Contains(body, `"msgtype":"template_card"`) {
		t.Fatalf("want a template_card send, got %s", body)
	}
	if strings.Contains(body, "card_action") {
		t.Fatalf("a URL-less card must not carry card_action: %s", body)
	}
}

// A card is atomic: however long the fallback text is, the card goes out as a
// single unsegmented send.
func TestSendCardIsNotSplit(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	long := strings.Repeat("汉", 1500) // 4500 bytes, would split into 3 text segments
	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan",
		Text: long,
		Card: &channels.Card{Title: "危险操作待确认", Desc: "工具：op_a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := fake.sendCalls.Load(); n != 1 {
		t.Fatalf("a card must not be segmented, got %d sends", n)
	}
	if !strings.Contains(fake.lastBodies[0], `"msgtype":"template_card"`) {
		t.Fatalf("want a template_card send, got %s", fake.lastBodies[0])
	}
}

// appchat/send has no template_card: a card addressed to a group chat falls
// back to the plain text payload, which is why producers must always fill Text.
func TestSendCardGroupFallsBackToText(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", ChatID: "roomA",
		Text: "群兜底文案",
		Card: &channels.Card{Title: "危险操作待确认", Desc: "工具：op_a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := fake.sendCalls.Load(); n != 1 {
		t.Fatalf("want 1 appchat send, got %d", n)
	}
	if len(fake.groupBodies) != 1 {
		t.Fatalf("the group reply must ride appchat/send, got %d group sends", len(fake.groupBodies))
	}
	body := fake.groupBodies[0]
	if !strings.Contains(body, `"msgtype":"text"`) || !strings.Contains(body, "群兜底文案") {
		t.Fatalf("a group card must fall back to the text payload: %s", body)
	}
	if strings.Contains(body, "template_card") {
		t.Fatalf("appchat/send must never carry a template_card: %s", body)
	}
}

// A message without a Card is untouched by the card path: text msgtype, no
// template_card key.
func TestSendWithoutCardUnaffected(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	if err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan", Text: "你好",
	}); err != nil {
		t.Fatal(err)
	}
	body := fake.lastBodies[0]
	if !strings.Contains(body, `"msgtype":"text"`) || strings.Contains(body, "template_card") {
		t.Fatalf("a card-less reply must stay a plain text send: %s", body)
	}
}

// A direct-chat card renders without any text, so an empty fallback Text
// (contract-violating input) must not silently drop the card.
func TestSendCardWithEmptyText(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan",
		Card: &channels.Card{Title: "危险操作待确认", Desc: "工具：op_a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := fake.sendCalls.Load(); n != 1 {
		t.Fatalf("a direct-chat card with empty text must still be sent, got %d sends", n)
	}
	if !strings.Contains(fake.lastBodies[0], `"msgtype":"template_card"`) {
		t.Fatalf("want a template_card send, got %s", fake.lastBodies[0])
	}
}

// appchat/send has no template_card: a group-chat card falls back to the text
// payload, so an empty Text must still skip the API like any empty reply.
func TestSendCardGroupEmptyTextSkipped(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", ChatID: "roomA",
		Card: &channels.Card{Title: "危险操作待确认", Desc: "工具：op_a"},
	})
	if err != nil {
		t.Fatalf("an empty group reply must succeed silently, got %v", err)
	}
	if n := fake.tokenCalls.Load(); n != 0 {
		t.Fatalf("an empty group reply must not fetch a token, got %d fetches", n)
	}
	if n := fake.sendCalls.Load(); n != 0 {
		t.Fatalf("an empty group reply must not hit the send API, got %d sends", n)
	}
}

// template_card fields are byte-capped by the platform; cardPayload must
// truncate title/desc on rune boundaries and drop an over-long URL.
func TestCardPayloadTruncates(t *testing.T) {
	card := &channels.Card{
		Title: strings.Repeat("汉", 100), // 300 bytes > 128
		Desc:  strings.Repeat("汉", 300), // 900 bytes > 512
		URL:   "https://example.com/" + strings.Repeat("a", 2000),
	}
	payload := cardPayload(card)
	mainTitle := payload["main_title"].(map[string]string)
	if len(mainTitle["title"]) > maxCardTitleBytes {
		t.Fatalf("title exceeds %d bytes: %d", maxCardTitleBytes, len(mainTitle["title"]))
	}
	if len(mainTitle["desc"]) > maxCardDescBytes {
		t.Fatalf("desc exceeds %d bytes: %d", maxCardDescBytes, len(mainTitle["desc"]))
	}
	if !utf8.ValidString(mainTitle["title"]) || !utf8.ValidString(mainTitle["desc"]) {
		t.Fatal("truncation must not split a UTF-8 sequence")
	}
	if !strings.HasPrefix(card.Title, mainTitle["title"]) || !strings.HasPrefix(card.Desc, mainTitle["desc"]) {
		t.Fatal("truncated fields must be prefixes of the originals")
	}
	if _, ok := payload["card_action"]; ok {
		t.Fatalf("an over-long URL must be dropped, not truncated: %v", payload["card_action"])
	}
}

// A markdown TextType only steers the text path; a card still wins on a
// direct chat.
func TestSendCardMarkdownPrefersCard(t *testing.T) {
	fake := &fakeWeComAPI{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	c := testChannel(t, srv.URL)

	err := c.Send(t.Context(), channels.OutboundMessage{
		Channel: "wecom", MsgID: "1", UserID: "zhangsan",
		Text: "兜底文案", TextType: "markdown",
		Card: &channels.Card{Title: "危险操作待确认", Desc: "工具：op_a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := fake.lastBodies[0]
	if !strings.Contains(body, `"msgtype":"template_card"`) || strings.Contains(body, `"markdown"`) {
		t.Fatalf("a card must take priority over the markdown text path: %s", body)
	}
}
