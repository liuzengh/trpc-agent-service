// trpc-init prepares a NEW self-hosted installation. It never loads the host's
// .env, starts services, resets a database, or replaces existing credentials.
package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
)

func main() {
	dir := flag.String("dir", "/setup", "dedicated installation volume")
	show := flag.Bool("show-token", false, "explicitly display the existing administrator token")
	flag.Parse()
	if err := run(*dir, *show); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(dir string, show bool) error {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) == "/" {
		return errors.New("a dedicated absolute installation directory is required")
	}
	path := filepath.Join(dir, "platform.env")
	if show {
		values, err := godotenv.Read(path)
		if err != nil || len(values["TRPC_AGENT_ADMIN_TOKEN"]) != 64 {
			return errors.New("installation credentials unavailable")
		}
		fmt.Println(values["TRPC_AGENT_ADMIN_TOKEN"])
		return nil
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		return errors.New("cannot prepare installation directory")
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("installation directory must not be a symlink")
	}
	if os.Geteuid() != 0 {
		return errors.New("the one-time init container must own the new volume as root")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.New("cannot inspect installation directory")
	}
	if len(entries) > 0 {
		marker, markerErr := os.Lstat(filepath.Join(dir, "initialized.lock"))
		if markerErr != nil || !marker.Mode().IsRegular() {
			return errors.New("refusing to initialize a nonempty directory without this installer's marker")
		}
	}
	// Exclusive creation is also the concurrency gate. A partial initialization
	// remains visibly failed; never regenerate a password against an existing DB.
	lock, err := os.OpenFile(filepath.Join(dir, "initialized.lock"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		values, readErr := godotenv.Read(path)
		password, passErr := os.ReadFile(filepath.Join(dir, "postgres-password"))
		master, keyErr := base64.StdEncoding.DecodeString(values["TRPC_AGENT_MODEL_MASTER_KEY"])
		if readErr != nil || passErr != nil || len(strings.TrimSpace(string(password))) != 64 || len(values["TRPC_AGENT_ADMIN_TOKEN"]) != 64 || keyErr != nil || len(master) != 32 {
			return errors.New("installation is incomplete; preserve this volume and inspect init failure, do not reset credentials")
		}
		fmt.Println("Existing installation retained; no credentials changed.")
		return nil
	}
	if err != nil {
		return errors.New("cannot lock installation volume")
	}
	if err := lock.Close(); err != nil {
		return err
	}
	if err := os.Chown(dir, 65534, 65534); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0750); err != nil {
		return err
	}
	randomValue := func() (string, error) {
		var b [32]byte
		_, err := rand.Read(b[:])
		return hex.EncodeToString(b[:]), err
	}
	adminToken, err := randomValue()
	if err != nil {
		return err
	}
	dbPassword, err := randomValue()
	if err != nil {
		return err
	}
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		return err
	}
	config := "TRPC_AGENT_ADDR=:8080\nTRPC_AGENT_ROLE=all\n" +
		"TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres\nTRPC_AGENT_POSTGRES_URL=postgres://trpc_agent:" + dbPassword + "@postgres:5432/trpc_agent?sslmode=disable\n" +
		"TRPC_AGENT_POSTGRES_AUTO_MIGRATE=false\nTRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=false\n" +
		"TRPC_AGENT_SESSION_BACKEND=redis\nTRPC_AGENT_COORDINATOR_BACKEND=redis\nTRPC_AGENT_IDEMPOTENCY_BACKEND=redis\n" +
		"TRPC_AGENT_QUEUE_BACKEND=redis\nTRPC_AGENT_QUOTA_BACKEND=redis\nREDIS_URL=redis://redis:6379/0\n" +
		"TRPC_AGENT_ADMIN_ENABLED=true\nTRPC_AGENT_ADMIN_TOKEN=" + adminToken + "\n" +
		"TRPC_AGENT_MODEL_PROVIDER=mock\nTRPC_AGENT_MODEL_MASTER_KEY=" + base64.StdEncoding.EncodeToString(key[:]) + "\n" +
		"TRPC_AGENT_HTTP_API_ENABLED=false\n"
	write := func(name, value string, mode os.FileMode) error {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		if err := f.Chown(65534, 65534); err != nil {
			return err
		}
		if err := f.Chmod(mode); err != nil {
			return err
		}
		if _, err := f.WriteString(value); err != nil {
			return err
		}
		return f.Sync()
	}
	// PostgreSQL's entrypoint is in supplemental group 65534. It can read only
	// its password file, not the platform token or model encryption key.
	if err := write("postgres-password", dbPassword+"\n", 0640); err != nil {
		return errors.New("cannot persist database credential; installation left incomplete")
	}
	if err := write("platform.env", config, 0600); err != nil {
		return errors.New("cannot persist platform configuration; installation left incomplete")
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	if err := directory.Sync(); err != nil {
		return err
	}
	fmt.Println("Installation initialized. Credentials are stored in the private setup volume, not printed in logs.")
	return nil
}
