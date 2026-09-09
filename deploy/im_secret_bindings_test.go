package deploy_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestIMSecretBindingsMatchRuntimeAndProcessScopes(t *testing.T) {
	helper, err := filepath.Abs(filepath.Join("..", "scripts", "im_secret_bindings.ps1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.ReadFile(helper); err != nil {
		t.Fatal(err)
	}
	var shell string
	for _, name := range []string{"pwsh", "powershell"} {
		if shell, err = exec.LookPath(name); err == nil {
			break
		}
	}
	if shell == "" {
		t.Skip("PowerShell is required to execute the Windows provisioning helper")
	}
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
	outputPath := filepath.Join(t.TempDir(), "bindings.json")
	bootstrap := filepath.Join(filepath.Dir(helper), "wecom_sandbox_bootstrap.ps1")
	command := `[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false); $ErrorActionPreference='Stop'; Set-StrictMode -Version Latest; . ` + quote(helper) + `
$source = [IO.File]::ReadAllText(` + quote(bootstrap) + `)
$assignment = [regex]::Match($source, '(?m)^\s*\$existing = [^\r\n]+').Value
if (-not $assignment) { throw 'Missing sandbox tenant lookup' }
$tenantName = 'fixture-sandbox'
function Invoke-AdminJSON { param($Method, $Path); return $script:fixtureTenants }
$script:fixtureTenants = @()
Invoke-Expression $assignment
$emptyTenantMatches = $existing.Count
$script:fixtureTenants = @([pscustomobject]@{name=$tenantName;id='fixture-tenant'})
Invoke-Expression $assignment
$singleTenantMatches = $existing.Count
$names = @(
  (Get-IMSecretBindingName -TenantId 'tenant-a' -Purpose 'model' -Provider 'openai' -Model 'gpt-4o-mini'),
  (Get-IMSecretBindingName -TenantId '租户甲' -Purpose 'channel_token' -Provider 'wework' -Model '账号一'),
  (Get-IMSecretBindingName -TenantId 'tenant_a' -Purpose 'model' -Provider 'openai' -Model 'gpt-4o-mini')
)
$definition = New-IMSecretBindingsOverride -TenantId 'tenant-a' -ModelProvider 'openai' -ModelName 'gpt-4o-mini' -ModelSecretRef 'env://TRPC_SECRET_MODEL' -ChannelType 'wework' -ChannelAccountId 'account-1' -ChannelTokenRef 'env://TRPC_SECRET_TOKEN' -ChannelSecretRef 'env://TRPC_SECRET_CORP' -ChannelEncodingAESKeyRef 'env://TRPC_SECRET_AES'
$null = Write-IMSecretBindingsOverride -Path ` + quote(outputPath) + ` -Definition $definition
$telegram = New-IMSecretBindingsOverride -TenantId 'tenant-b' -ModelProvider 'openai' -ModelName 'gpt-4o-mini' -ModelSecretRef 'env://TRPC_SECRET_MODEL' -ChannelType 'telegram' -ChannelAccountId 'bot-1' -ChannelTokenRef 'env://TRPC_SECRET_BOT' -ChannelSecretRef 'env://TRPC_SECRET_WEBHOOK'
$wecomBot = New-IMSecretBindingsOverride -TenantId 'tenant-c' -ModelProvider 'openai' -ModelName 'gpt-4o-mini' -ModelSecretRef 'env://TRPC_SECRET_MODEL' -ChannelType 'wecom_bot' -ChannelAccountId 'aib_fixture' -ChannelTokenRef 'env://TRPC_SECRET_BRIDGE'
$rejected = 0
foreach ($invalid in @('raw-model-secret', 'ENV://TRPC_SECRET_MODEL', ("env://TRPC_SECRET_MODEL" + [char]10))) {
  try { $null = New-IMSecretBindingsOverride -TenantId 'tenant-a' -ModelProvider 'openai' -ModelName 'gpt-4o-mini' -ModelSecretRef $invalid -ChannelType 'telegram' -ChannelAccountId 'account-1' -ChannelTokenRef 'env://TRPC_SECRET_TOKEN' -ChannelSecretRef 'env://TRPC_SECRET_CORP' } catch { $rejected++ }
}
[ordered]@{ names=$names; definition=$definition; telegram=$telegram; wecomBot=$wecomBot; rejected=$rejected; emptyTenantMatches=$emptyTenantMatches; singleTenantMatches=$singleTenantMatches } | ConvertTo-Json -Depth 10 -Compress`
	result, err := exec.Command(shell, "-NoProfile", "-NonInteractive", "-Command", command).CombinedOutput()
	if err != nil {
		t.Fatalf("execute provisioning helper: %v\n%s", err, result)
	}
	type override struct {
		Services map[string]struct {
			Environment map[string]string `json:"environment"`
		} `json:"services"`
	}
	var got struct {
		Names               []string `json:"names"`
		Definition          override `json:"definition"`
		Telegram            override `json:"telegram"`
		WeComBot            override `json:"wecomBot"`
		Rejected            int      `json:"rejected"`
		EmptyTenantMatches  int      `json:"emptyTenantMatches"`
		SingleTenantMatches int      `json:"singleTenantMatches"`
	}
	if err := json.Unmarshal(result, &got); err != nil {
		t.Fatalf("parse helper result: %v\n%s", err, result)
	}
	wantNames := []string{
		tenant.SecretBindingEnvironmentName("tenant-a", "model", "openai", "gpt-4o-mini"),
		tenant.SecretBindingEnvironmentName("租户甲", "channel_token", "wework", "账号一"),
		tenant.SecretBindingEnvironmentName("tenant_a", "model", "openai", "gpt-4o-mini"),
	}
	if len(got.Names) != len(wantNames) {
		t.Fatalf("binding names count=%d", len(got.Names))
	}
	for i, want := range wantNames {
		if got.Names[i] != want {
			t.Errorf("binding %d=%q, want runtime identity %q", i, got.Names[i], want)
		}
	}
	if got.Rejected != 3 {
		t.Error("helper accepted a raw credential or malformed reference")
	}
	if got.EmptyTenantMatches != 0 || got.SingleTenantMatches != 1 {
		t.Error("sandbox lookup does not preserve empty and single-result arrays under StrictMode")
	}
	for name, definition := range map[string]override{"wework": got.Definition, "telegram": got.Telegram, "wecom_bot": got.WeComBot} {
		if len(definition.Services) != 5 {
			t.Fatalf("%s must expose only five allowed services", name)
		}
		tenantID, account := "tenant-a", "account-1"
		channelRefs := map[string]string{"channel_token": "env://TRPC_SECRET_TOKEN", "channel_secret": "env://TRPC_SECRET_CORP", "channel_encoding_aes_key": "env://TRPC_SECRET_AES"}
		if name == "telegram" {
			tenantID, account = "tenant-b", "bot-1"
			channelRefs = map[string]string{"channel_token": "env://TRPC_SECRET_BOT", "channel_secret": "env://TRPC_SECRET_WEBHOOK"}
		}
		if name == "wecom_bot" {
			tenantID, account = "tenant-c", "aib_fixture"
			channelRefs = map[string]string{"channel_token": "env://TRPC_SECRET_BRIDGE"}
		}
		for _, service := range []string{"admin", "worker", "summary-worker"} {
			env := definition.Services[service].Environment
			if len(env) != 1 || env[tenant.SecretBindingEnvironmentName(tenantID, "model", "openai", "gpt-4o-mini")] != "env://TRPC_SECRET_MODEL" {
				t.Errorf("%s/%s has the wrong model authorization scope", name, service)
			}
		}
		for _, service := range []string{"gateway", "delivery"} {
			env := definition.Services[service].Environment
			if len(env) != len(channelRefs) {
				t.Errorf("%s/%s has extra/missing channel authorizations", name, service)
			}
			for purpose, ref := range channelRefs {
				if env[tenant.SecretBindingEnvironmentName(tenantID, purpose, name, account)] != ref {
					t.Errorf("%s/%s is missing exact authorization %s", name, service, purpose)
				}
			}
		}
	}
	written, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	var persisted override
	if err := json.Unmarshal(written, &persisted); err != nil || len(persisted.Services) != 5 {
		t.Fatalf("persisted Compose override is invalid: %v", err)
	}
}

func TestWeComBootstrapInstallsBindingsBeforeVersionAdmission(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "scripts", "wecom_sandbox_bootstrap.ps1"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(data)
	for _, required := range []string{"im_secret_bindings.ps1", "New-IMSecretBindingsOverride", "Write-IMSecretBindingsOverride", "'-f', $bindingOverride", "'admin', 'worker', 'summary-worker', 'gateway', 'delivery'"} {
		if !strings.Contains(script, required) {
			t.Errorf("WeCom bootstrap lacks scoped authorization wiring %q", required)
		}
	}
	install := strings.Index(script, "New-IMSecretBindingsOverride")
	admit := strings.Index(script, "-Path '/api/v1/agent-versions'")
	if install < 0 || admit < 0 || install > admit {
		t.Error("bindings must be installed before version admission")
	}
}
