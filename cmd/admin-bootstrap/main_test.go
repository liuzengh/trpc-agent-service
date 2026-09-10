package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
)

func TestSecretThenTokenCreatesPrivateProjectionWithoutPrintingMaterial(t *testing.T) {
	root := t.TempDir()
	tenantID := "tenant-a"
	material := bytes.Repeat([]byte{7}, 48)
	var secretOutput bytes.Buffer
	if err := run([]string{"secret", "-secret-root", root, "-tenant-id", tenantID}, &secretOutput, bytes.NewReader(material), fixedNow); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(secretOutput.String(), string(material[:32])) {
		t.Fatal("secret material leaked to command output")
	}
	value := secretFlags{root: root, tenantID: tenantID, secretRef: defaultSecretRef, secretVersion: 1}
	name, err := secretfs.StableFilename(scope(value), ref(value))
	if err != nil {
		t.Fatal(err)
	}
	provider, err := secretfs.New(root, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := provider.Resolve(context.Background(), scope(value), ref(value))
	if err != nil || !bytes.Equal(stored.Bytes, material[:32]) {
		t.Fatalf("secret length=%d err=%v", len(stored.Bytes), err)
	}
	if !strings.Contains(secretOutput.String(), name) {
		t.Fatalf("output did not identify projection: %s", secretOutput.String())
	}

	var tokenOutput bytes.Buffer
	if err := run([]string{"token", "-secret-root", root, "-tenant-id", tenantID, "-tenant-version", "3", "-ttl", "5m"}, &tokenOutput, bytes.NewReader(bytes.Repeat([]byte{9}, 16)), fixedNow); err != nil {
		t.Fatal(err)
	}
	resolver, err := admin.NewHMACPrincipalResolver(stored.Bytes, admin.HMACPrincipalOptions{Clock: fixedNow, VersionCheck: func(id string, version int64) error {
		if id != tenantID || version != 3 {
			t.Fatalf("version check tenant=%s version=%d", id, version)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer resolver.Close()
	request := httptest.NewRequest(http.MethodGet, "/v1/tenants/tenant-a/configs", nil)
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(tokenOutput.String()))
	principal, err := resolver.Resolve(request)
	if err != nil || principal.SubjectID != defaultSubject || !principal.CanManage {
		t.Fatalf("principal=%#v err=%v", principal, err)
	}
}

func TestSecretRefusesOverwriteAndTokenRequiresCurrentTenantVersion(t *testing.T) {
	root := t.TempDir()
	args := []string{"secret", "-secret-root", root, "-tenant-id", "tenant-a"}
	if err := run(args, &bytes.Buffer{}, bytes.NewReader(bytes.Repeat([]byte{1}, 32)), fixedNow); err != nil {
		t.Fatal(err)
	}
	if err := run(args, &bytes.Buffer{}, bytes.NewReader(bytes.Repeat([]byte{2}, 32)), fixedNow); err == nil {
		t.Fatal("existing projection overwritten")
	}
	if err := run([]string{"token", "-secret-root", root, "-tenant-id", "tenant-a"}, &bytes.Buffer{}, bytes.NewReader(bytes.Repeat([]byte{3}, 16)), fixedNow); err == nil {
		t.Fatal("token without tenant version accepted")
	}
}

func fixedNow() time.Time { return time.Unix(1_800_000_000, 0).UTC() }
