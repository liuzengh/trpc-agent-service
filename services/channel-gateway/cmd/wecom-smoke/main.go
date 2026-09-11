// Command wecom-smoke runs a loopback-only credential form and a bounded real
// WeCom protocol smoke test. It does not register a platform account or run an Agent.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
)

type snapshot struct {
	Marker        string             `json:"marker"`
	State         string             `json:"state"`
	Probe         *wecom.ProbeResult `json:"probe,omitempty"`
	Authenticated bool               `json:"authenticated"`
	Received      int                `json:"received"`
	ReplyAccepted int                `json:"reply_accepted"`
	BotIDMatched  bool               `json:"bot_id_matched"`
	ReplyError    string             `json:"reply_error,omitempty"`
	Closed        bool               `json:"closed"`
}
type lab struct {
	mu                 sync.Mutex
	bot, nonce, origin string
	status             snapshot
	busy               bool
	cancel             context.CancelFunc
	done               chan struct{}
	ctx                context.Context
}

func random() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("random source failed")
	}
	return hex.EncodeToString(b[:])
}
func (l *lab) handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
	if r.Host != strings.TrimPrefix(l.origin, "http://") || r.URL.Path != "/"+l.nonce || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, strings.ReplaceAll(page, "BOT_ID", l.bot))
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method", 405)
		return
	}
	if r.Header.Get("Origin") != l.origin {
		http.Error(w, "origin", 403)
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		http.Error(w, "json", 415)
		return
	}
	var input struct {
		Action  string `json:"action"`
		Secret  string `json:"secret"`
		Confirm bool   `json:"confirm"`
	}
	raw, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 8192))
	if readErr != nil {
		http.Error(w, "input", 400)
		return
	}
	defer clear(raw)
	canonical, canonicalErr := jcs.Transform(raw)
	if canonicalErr != nil {
		http.Error(w, "input", 400)
		return
	}
	defer clear(canonical)
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&input) != nil || dec.Decode(new(any)) != io.EOF {
		http.Error(w, "input", 400)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch input.Action {
	case "status":
	case "stop":
		l.stop()
	case "probe", "connect":
		if !input.Confirm {
			http.Error(w, "confirmation_required", 400)
			return
		}
		cfg := wecom.Config{BotID: l.bot, Secret: input.Secret, MaxReconnects: 0}
		if _, err := wecom.NewClient(cfg); err != nil {
			http.Error(w, "invalid_credentials", 400)
			return
		}
		l.mu.Lock()
		if l.busy {
			l.mu.Unlock()
			http.Error(w, "already_running", 409)
			return
		}
		l.busy = true
		opCtx, cancel := context.WithTimeout(l.ctx, 15*time.Minute)
		l.cancel = cancel
		l.done = make(chan struct{})
		l.status = snapshot{Marker: l.status.Marker, State: "CONNECTING"}
		done := l.done
		l.mu.Unlock()
		if input.Action == "probe" {
			go func() {
				defer close(done)
				defer cancel()
				result, err := wecom.ProbeAuthentication(opCtx, cfg)
				l.mu.Lock()
				defer l.mu.Unlock()
				if err != nil {
					result = wecom.ProbeResult{Code: "PROBE_INTERRUPTED"}
				}
				l.status.Probe = &result
				l.status.Authenticated = result.Authenticated != nil && *result.Authenticated
				l.status.State = "PROBE_FINISHED"
				l.status.Closed = true
				l.busy = false
			}()
		} else {
			go l.connect(opCtx, cancel, done, cfg)
		}
	default:
		http.Error(w, "action", 400)
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = json.NewEncoder(w).Encode(l.status)
}
func (l *lab) connect(ctx context.Context, cancel context.CancelFunc, done chan struct{}, cfg wecom.Config) {
	defer close(done)
	defer cancel()
	// Verify via the same public probe used by Gateway; release that connection
	// before starting the separate message smoke session.
	probe, probeErr := wecom.ProbeAuthentication(ctx, cfg)
	l.mu.Lock()
	l.status.Probe = &probe
	l.mu.Unlock()
	if probeErr != nil || probe.Authenticated == nil || !*probe.Authenticated {
		l.mu.Lock()
		l.status.State = "PROBE_FAILED"
		l.status.Closed = true
		l.busy = false
		l.mu.Unlock()
		return
	}
	client, err := wecom.NewClient(cfg)
	if err != nil {
		l.mu.Lock()
		l.status.State = "INVALID"
		l.status.Closed = true
		l.busy = false
		l.mu.Unlock()
		return
	}
	statesDone := make(chan struct{})
	go func() {
		defer close(statesDone)
		for state := range client.States() {
			l.mu.Lock()
			l.status.State = string(state.State)
			if state.State == wecom.StateReady {
				l.status.Authenticated = true
			}
			l.mu.Unlock()
		}
	}()
	_ = client.Run(ctx, func(ctx context.Context, e wecom.Event) error {
		l.mu.Lock()
		marker := l.status.Marker
		l.mu.Unlock()
		if e.Kind != wecom.EventText || strings.TrimSpace(e.Text) != marker {
			return nil
		}
		l.mu.Lock()
		l.status.Received++
		l.status.BotIDMatched = e.BotID == l.bot
		l.mu.Unlock()
		_, err := client.Reply(ctx, wecom.ReplyRequest{RequestID: e.RequestID, Generation: e.Generation, StreamID: "smoke-" + random(), Content: "企微接入验证通过：" + marker})
		l.mu.Lock()
		defer l.mu.Unlock()
		if err == nil {
			l.status.ReplyAccepted++
		} else {
			var ce *wecom.CommandError
			if errors.As(err, &ce) {
				l.status.ReplyError = string(ce.Certainty) + "/" + string(ce.Code)
			} else {
				l.status.ReplyError = "UNKNOWN"
			}
		}
		return nil
	})
	closeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	_ = client.Close(closeCtx)
	stop()
	<-statesDone
	l.mu.Lock()
	l.status.Closed = true
	l.busy = false
	l.mu.Unlock()
}
func (l *lab) stop() {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(8 * time.Second):
		}
	}
}
func main() {
	listen := flag.String("listen", "127.0.0.1:0", "loopback only")
	bot := flag.String("bot-id", "", "test bot identity; Secret is entered only in the local form")
	flag.Parse()
	// Bot IDs are rendered in HTML; use a bounded conservative presentation grammar.
	if len(*bot) == 0 || len(*bot) > 1024 || strings.Trim(*bot, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != "" {
		log.Fatal("invalid bot identity")
	}
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || host != "127.0.0.1" {
		log.Fatal("listen must be IPv4 loopback")
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal("loopback listener failed")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	l := &lab{bot: *bot, nonce: random(), origin: "http://" + ln.Addr().String(), ctx: ctx, status: snapshot{Marker: "wecom-smoke-" + random()[:12], State: "IDLE", Closed: true}}
	server := &http.Server{Handler: http.HandlerFunc(l.handler), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	fmt.Println("LOCAL_URL=" + l.origin + "/" + l.nonce)
	go func() {
		<-ctx.Done()
		l.stop()
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = server.Shutdown(shutdown)
	}()
	if err = server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("local server stopped")
	}
}

const page = `<!doctype html><meta charset="utf-8"><title>企微机器人接入验证</title><style>body{font:16px system-ui;max-width:820px;margin:40px auto;padding:24px;background:#f5f7fa;color:#17263b}input,button{font:inherit;padding:12px;margin:8px 0}input[type=password]{width:90%}button{cursor:pointer;background:#2463eb;color:white;border:0;border-radius:6px;margin-right:10px}pre{white-space:pre-wrap;background:white;padding:20px;border-radius:12px}label{display:block;line-height:1.8}</style><h1>企业微信智能机器人 · 接入验证</h1><p>Bot ID：<code>BOT_ID</code></p><p>使用仓库公开 Go 包，不单独部署 Connector。Secret 仅保存在本机进程内存，页面不会保存或回显；连接只发往企业微信官方长连接地址。</p><label>Bot Secret<input id="secret" type="password" autocomplete="off" spellcheck="false"></label><label><input id="confirm" type="checkbox">我确认这是待验证机器人；认证会建立真实订阅，可能替换同 Bot 的其他连接。测试期间可能收到回调，但不执行 Agent、不创建平台账户。</label><button id="probe">仅验证认证</button><button id="connect">连接并验证收发</button><button id="stop">停止并释放连接</button><p>收发验证：连接 READY 后，在企微向此机器人发送下方 marker。程序只回复这个测试标记，不记录其他聊天内容。</p><pre id="marker"></pre><pre id="status">等待输入</pre><script>
const call=async(action)=>{const data={action};if(action==='connect'||action==='probe'){if(!document.querySelector('#confirm').checked)throw Error('请先确认连接影响');data.secret=document.querySelector('#secret').value;data.confirm=true;}const r=await fetch(location.pathname,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});if(!r.ok)throw Error(await r.text());return r.json()};const show=s=>{document.querySelector('#marker').textContent=s.marker;document.querySelector('#status').textContent=JSON.stringify(s,null,2)};for(const action of ['probe','connect','stop'])document.getElementById(action).onclick=async()=>{try{show(await call(action));if(action!=='stop')document.querySelector('#secret').value=''}catch(e){document.querySelector('#status').textContent=e.message}};setInterval(()=>call('status').then(show).catch(()=>{}),1200);call('status').then(show);
</script>`
