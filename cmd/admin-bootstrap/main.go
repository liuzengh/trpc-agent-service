// Command admin-bootstrap provisions a local filesystem SecretProvider entry
// and, when explicitly requested, issues a short-lived development token.
// It is intentionally not part of the service binary and never prints the
// generated HMAC material.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
)

const (
	defaultSecretRef     = "secret://local/admin-auth"
	defaultSecretVersion = int64(1)
	defaultSubject       = "local-admin"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, rand.Reader, time.Now); err != nil {
		fmt.Fprintf(os.Stderr, "admin-bootstrap: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer, random io.Reader, now func() time.Time) error {
	if len(args) == 0 {
		return errors.New("usage: admin-bootstrap <secret|token> ...")
	}
	if output == nil || random == nil || now == nil {
		return errors.New("invalid command dependencies")
	}
	switch args[0] {
	case "secret":
		return createSecret(args[1:], output, random)
	case "token":
		return issueToken(args[1:], output, random, now)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

type secretFlags struct {
	root, tenantID, secretRef string
	secretVersion             int64
}

func parseSecretFlags(name string, args []string) (secretFlags, error) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	value := secretFlags{}
	flags.StringVar(&value.root, "secret-root", "", "private filesystem SecretProvider root")
	flags.StringVar(&value.tenantID, "tenant-id", "", "existing active tenant ID")
	flags.StringVar(&value.secretRef, "secret-ref", defaultSecretRef, "Admin secret reference")
	flags.Int64Var(&value.secretVersion, "secret-version", defaultSecretVersion, "Admin secret version")
	if err := flags.Parse(args); err != nil {
		return secretFlags{}, err
	}
	if len(flags.Args()) != 0 {
		return secretFlags{}, fmt.Errorf("usage: admin-bootstrap %s -secret-root <dir> -tenant-id <tenant> [-secret-ref <ref>] [-secret-version <n>]", name)
	}
	if err := validateSecretFlags(name, value); err != nil {
		return secretFlags{}, err
	}
	return value, nil
}

func validateSecretFlags(name string, value secretFlags) error {
	if strings.TrimSpace(value.root) == "" || strings.TrimSpace(value.tenantID) == "" ||
		strings.TrimSpace(value.secretRef) == "" || value.secretVersion < 1 {
		return fmt.Errorf("usage: admin-bootstrap %s -secret-root <dir> -tenant-id <tenant> [-secret-ref <ref>] [-secret-version <n>]", name)
	}
	return nil
}

func scope(value secretFlags) secrets.Scope {
	return secrets.Scope{TenantID: value.tenantID, Subject: "admin", Purpose: secrets.PurposeAdminAuth,
		ResourceID: "admin-auth", ResourceVersion: value.secretVersion}
}

func ref(value secretFlags) secrets.SecretRef {
	return secrets.SecretRef{Ref: value.secretRef, Version: value.secretVersion}
}

func createSecret(args []string, output io.Writer, random io.Reader) error {
	value, err := parseSecretFlags("secret", args)
	if err != nil {
		return err
	}
	if _, err := secretfs.New(value.root, 64<<10); err != nil {
		return fmt.Errorf("secret root rejected: %w", err)
	}
	name, err := secretfs.StableFilename(scope(value), ref(value))
	if err != nil {
		return fmt.Errorf("secret coordinate rejected: %w", err)
	}
	material := make([]byte, 32)
	if _, err := io.ReadFull(random, material); err != nil {
		return fmt.Errorf("generate HMAC key: %w", err)
	}
	path := filepath.Join(value.root, name)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("refusing to overwrite existing Admin secret projection %q", name)
		}
		return err
	}
	if _, err := file.Write(material); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	clear(material)
	fmt.Fprintf(output, "created private Admin secret projection %s\n", name)
	fmt.Fprintf(output, "TRPC_ADMIN_PROBE_TENANT_ID=%s\nTRPC_ADMIN_AUTH_SECRET_REF=%s\nTRPC_ADMIN_AUTH_SECRET_VERSION=%d\n", value.tenantID, value.secretRef, value.secretVersion)
	fmt.Fprintln(output, "Run `admin-bootstrap token` with the tenant's current version to issue a short-lived local token.")
	return nil
}

func issueToken(args []string, output io.Writer, random io.Reader, now func() time.Time) error {
	flags := flag.NewFlagSet("token", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var value secretFlags
	var tenantVersion int64
	var subject string
	var ttl time.Duration
	var canManageReleases bool
	flags.StringVar(&value.root, "secret-root", "", "private filesystem SecretProvider root")
	flags.StringVar(&value.tenantID, "tenant-id", "", "existing active tenant ID")
	flags.StringVar(&value.secretRef, "secret-ref", defaultSecretRef, "Admin secret reference")
	flags.Int64Var(&value.secretVersion, "secret-version", defaultSecretVersion, "Admin secret version")
	flags.Int64Var(&tenantVersion, "tenant-version", 0, "current tenant version")
	flags.StringVar(&subject, "subject", defaultSubject, "admin subject")
	flags.DurationVar(&ttl, "ttl", 15*time.Minute, "token lifetime")
	flags.BoolVar(&canManageReleases, "can-manage-releases", false, "grant platform release management for local use")
	if err := flags.Parse(args); err != nil || len(flags.Args()) != 0 || validateSecretFlags("token", value) != nil ||
		tenantVersion < 1 || strings.TrimSpace(subject) == "" || ttl <= 0 || ttl > 15*time.Minute {
		return errors.New("usage: admin-bootstrap token -secret-root <dir> -tenant-id <tenant> -tenant-version <n> [-subject <sub>] [-ttl <=15m]")
	}
	provider, err := secretfs.New(value.root, 64<<10)
	if err != nil {
		return fmt.Errorf("secret root rejected: %w", err)
	}
	secret, err := provider.Resolve(context.Background(), scope(value), ref(value))
	if err != nil {
		return fmt.Errorf("resolve Admin secret: %w", err)
	}
	defer clear(secret.Bytes)
	tokenID := make([]byte, 16)
	if _, err := io.ReadFull(random, tokenID); err != nil {
		return fmt.Errorf("generate token ID: %w", err)
	}
	issuedAt := now().UTC()
	token, err := admin.SignToken(secret.Bytes, admin.Claims{Version: 1, TenantID: value.tenantID, TenantVersion: tenantVersion,
		SubjectID: subject, CanManage: true, CanManageReleases: canManageReleases, IssuedAt: issuedAt.Unix(), ExpiresAt: issuedAt.Add(ttl).Unix(),
		TokenID: base64.RawURLEncoding.EncodeToString(tokenID)})
	clear(tokenID)
	if err != nil {
		return err
	}
	fmt.Fprintln(output, token)
	return nil
}

func clear(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
