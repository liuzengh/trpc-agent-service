// trpc-wecomsetup is an operator-only, explicitly authorized onboarding command.
// It never prints credential values, raw conversation identifiers or messages.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string, out io.Writer) error {
	f := flag.NewFlagSet("wecomsetup", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	mode := f.String("mode", "", "prepare or apply")
	authorized := f.Bool("authorized", false, "operator authorization for the confirmed test group")
	sessions := f.String("sessions-file", "", "fresh private sessions snapshot")
	messages := f.String("messages-file", "", "reuse a private, confirmed-group-bound message snapshot")
	attempt := f.String("attempt-file", "", "previous confirmed reply attempt record")
	stamp := f.String("sample-time", "", "previous authorized sample timestamp (UTC+8)")
	file := f.String("binding-file", "", "private binding file")
	directory := f.String("output-dir", "data", "private temporary sample directory")
	adminAddress := f.String("admin-address", "http://127.0.0.1:8080", "loopback Admin origin")
	if f.Parse(args) != nil || f.NArg() != 0 || !*authorized || (*mode != "prepare" && *mode != "apply") || *file == "" {
		return errors.New("invalid or unauthorized onboarding arguments")
	}
	if _, err := config.LoadDotEnv(".env"); err != nil {
		return errors.New("cannot load onboarding configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if *mode == "prepare" {
		endpoint := os.Getenv("WECOM_MCP_URL")
		source := *messages
		if source == "" {
			report, err := wecommcp.ReadConfirmedGroupSample(ctx, endpoint, *sessions, *attempt, *stamp, *directory)
			if err != nil {
				return err
			}
			// Print only the private artifact path so incomplete setup can be cleaned.
			_ = json.NewEncoder(out).Encode(map[string]any{"private_sample": report.File, "provider_status": report.Status})
			source = report.File
		}
		binding, err := wecommcp.BuildConfirmedGroupBinding(endpoint, *sessions, source, *attempt, controlplane.ChannelBinding{ID: "wecom-mcp-tutorial", TenantID: "tutorial-tenant", AppID: "tutorial-app", AccountID: "tutorial-mcp-bot", CallbackKey: "wecom-mcp-tutorial-route", SecretRef: "env://WECOM_MCP_URL"}, time.Now())
		if err != nil {
			return err
		}
		target, err := os.OpenFile(*file, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return errors.New("binding file exists or cannot be created")
		}
		defer func() { _ = target.Close() }()
		if json.NewEncoder(target).Encode(binding) != nil || target.Sync() != nil {
			return errors.New("cannot persist private binding")
		}
		cfg, _ := wecommcp.ParseBinding(binding)
		return json.NewEncoder(out).Encode(map[string]any{"binding_file": *file, "binding_id": binding.ID, "groups": 1, "human_senders": 1, "mention_configured": true, "mention_style": cfg.MentionStyle, "start_at": cfg.StartAt, "saved_only": true})
	}
	input, err := os.Open(*file)
	if err != nil {
		return errors.New("cannot open prepared binding")
	}
	defer func() { _ = input.Close() }()
	var binding controlplane.ChannelBinding
	if json.NewDecoder(io.LimitReader(input, 65536)).Decode(&binding) != nil {
		return errors.New("invalid prepared binding")
	}
	if _, err := wecommcp.ParseBinding(binding); err != nil {
		return err
	}
	if binding.ID != "wecom-mcp-tutorial" || binding.TenantID != "tutorial-tenant" || binding.AppID != "tutorial-app" || binding.Status != controlplane.StatusActive {
		return errors.New("onboarding only applies the prepared tutorial binding")
	}
	admin, err := config.LoadAdminConfigFromEnv()
	if err != nil || !admin.Enabled {
		return errors.New("local Admin configuration unavailable")
	}
	token := ""
	for _, p := range admin.Principals {
		if p.Role == "superadmin" || (p.Role == "tenant_admin" && slices.Contains(p.TenantIDs, binding.TenantID)) {
			token = p.Token
			break
		}
	}
	if token == "" {
		return errors.New("no authorized local Admin principal")
	}
	if err := applyBinding(ctx, *adminAddress, token, binding); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(map[string]any{"binding_id": binding.ID, "status": "active", "configured": true})
}

func applyBinding(ctx context.Context, address, token string, binding controlplane.ChannelBinding) error {
	u, err := url.Parse(address)
	if err != nil {
		return errors.New("invalid Admin address")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "http" || ip == nil || !ip.IsLoopback() || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("admin address must be a loopback HTTP origin")
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("admin redirects disabled") }}
	call := func(path string, body any) (int, []byte, error) {
		raw, _ := json.Marshal(body)
		request, err := http.NewRequestWithContext(ctx, "POST", address+path, bytes.NewReader(raw))
		if err != nil {
			return 0, nil, errors.New("cannot create Admin request")
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(request)
		if err != nil {
			return 0, nil, errors.New("admin request failed (details omitted)")
		}
		defer func() { _ = response.Body.Close() }()
		data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			return 0, nil, errors.New("cannot read Admin response")
		}
		return response.StatusCode, data, nil
	}
	verify := func(data []byte) bool {
		var current controlplane.ChannelBinding
		if json.Unmarshal(data, &current) != nil {
			return false
		}
		got, e1 := wecommcp.ParseBinding(current)
		want, e2 := wecommcp.ParseBinding(binding)
		return e1 == nil && e2 == nil && current.ID == binding.ID && current.TenantID == binding.TenantID && current.AppID == binding.AppID && current.AccountID == binding.AccountID && current.CallbackKey == binding.CallbackKey && current.Status == binding.Status && wecommcp.ConfigFingerprint(current, got) == wecommcp.ConfigFingerprint(binding, want)
	}
	status, data, err := call("/admin/channel-bindings/get", map[string]string{"tenant_id": binding.TenantID, "binding_id": binding.ID})
	if err != nil {
		return err
	}
	if status == 200 {
		if verify(data) {
			return nil
		}
		return errors.New("binding already exists with different configuration; not overwritten")
	}
	if status != 404 {
		return fmt.Errorf("admin preflight rejected: HTTP %d", status)
	}
	status, data, err = call("/admin/channel-bindings", binding)
	if err != nil {
		return errors.New("binding create outcome unavailable; rerun same prepared file to verify, do not recreate it")
	}
	if status != 201 || !verify(data) {
		return fmt.Errorf("binding creation not confirmed: HTTP %d", status)
	}
	return nil
}
