// Command configctl manages tenant configuration OUTSIDE the running platform.
//
// Tenant configuration rollback exists inside the platform as well
// (tenant_config_versions + POST /tenants/{id}/config-rollback, surfaced in the
// admin UI). This binary is the second, independent path, for the cases the API
// cannot cover:
//
//   - the platform is down, mid-upgrade, or refusing to accept writes, and a
//     tenant's configuration has to be restored anyway;
//   - configuration belongs in change control: `export` writes one canonical
//     JSON file per tenant so a diff in a pull request shows exactly which
//     quota / audit policy / data backend changed, and `apply` puts a reviewed
//     file back;
//   - a migration or data fix has to touch the table directly, and the operator
//     wants the rollback history to still be the platform's, not a side table
//     invented by a script.
//
// It talks to the same MySQL the platform uses and goes through the domain
// manager, so an apply/rollback records a new configuration version exactly
// like the API does (the history stays coherent no matter which path wrote it).
//
// Usage:
//
//	configctl -dsn "$TRPC_MYSQL_DSN" export   -out ./tenant-config
//	configctl -dsn "$TRPC_MYSQL_DSN" diff     -dir ./tenant-config
//	configctl -dsn "$TRPC_MYSQL_DSN" apply    -file ./tenant-config/t-demo.json
//	configctl -dsn "$TRPC_MYSQL_DSN" versions -tenant t-demo
//	configctl -dsn "$TRPC_MYSQL_DSN" rollback -tenant t-demo -version 3
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/tenantstore"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("TRPC_MYSQL_DSN"), "MySQL DSN (defaults to $TRPC_MYSQL_DSN)")
	mode := flag.String("mode", "", "export | diff | apply | versions | rollback")
	out := flag.String("out", "", "export: output directory")
	dir := flag.String("dir", "", "diff: directory holding exported tenant files")
	file := flag.String("file", "", "apply: JSON file to apply (the file name must be <tenant_id>.json)")
	tenantID := flag.String("tenant", "", "versions/rollback: tenant id")
	version := flag.Int("version", 0, "rollback: configuration version to restore")
	dryRun := flag.Bool("dry-run", false, "apply: report what would change without writing")
	flag.Parse()

	if *dsn == "" {
		fatal("a MySQL DSN is required (-dsn or $TRPC_MYSQL_DSN)")
	}
	// The subcommand is accepted both as -mode and as a bare first argument,
	// because both spellings are natural at a shell.
	cmd := *mode
	if cmd == "" && flag.NArg() > 0 {
		cmd = flag.Arg(0)
	}
	if cmd == "" {
		fatal("usage: configctl -dsn <dsn> <export|diff|apply|versions|rollback> [flags]")
	}

	ctx := context.Background()
	db, err := storage.OpenMySQL(*dsn)
	if err != nil {
		fatal("open mysql: %v", err)
	}
	defer db.Close()
	mgr := tenantstore.NewMySQLManager(db)

	switch cmd {
	case "export":
		mustExport(ctx, mgr, *out)
	case "diff":
		mustDiff(ctx, mgr, *dir)
	case "apply":
		mustApply(ctx, mgr, *file, *dryRun)
	case "versions":
		mustVersions(ctx, mgr, *tenantID)
	case "rollback":
		mustRollback(ctx, mgr, *tenantID, *version)
	default:
		fatal("unknown mode %q", cmd)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "configctl: "+format+"\n", args...)
	os.Exit(1)
}

// mustExport writes one file per tenant. The file is the exact JSON the API
// returns for GET /tenants/{id}, so a person can read the diff and the platform
// can round-trip it.
func mustExport(ctx context.Context, mgr *tenant.Manager, out string) {
	if out == "" {
		fatal("export requires -out <dir>")
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		fatal("create %s: %v", out, err)
	}
	all, err := mgr.List(ctx)
	if err != nil {
		fatal("list tenants: %v", err)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	for _, t := range all {
		raw, err := json.MarshalIndent(t, "", "  ")
		if err != nil {
			fatal("encode %s: %v", t.ID, err)
		}
		path := filepath.Join(out, t.ID+".json")
		if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
			fatal("write %s: %v", path, err)
		}
		fmt.Printf("exported %s\n", path)
	}
	fmt.Printf("%d tenant(s) exported to %s\n", len(all), out)
}

// mustDiff compares the database with the exported directory: the drift report
// an operator reads before deciding whether to apply or to re-export.
func mustDiff(ctx context.Context, mgr *tenant.Manager, dir string) {
	if dir == "" {
		fatal("diff requires -dir <dir>")
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		fatal("scan %s: %v", dir, err)
	}
	drift := 0
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			fatal("read %s: %v", path, err)
		}
		var want tenant.Tenant
		if err := json.Unmarshal(raw, &want); err != nil {
			fatal("parse %s: %v", path, err)
		}
		got, err := mgr.Get(ctx, want.ID)
		if err != nil {
			fmt.Printf("~ %s: missing in the database (%v)\n", want.ID, err)
			drift++
			continue
		}
		if !sameConfig(got, &want) {
			fmt.Printf("~ %s: configuration differs\n    db:   %s\n    file: %s\n",
				want.ID, compact(got), compact(&want))
			drift++
		}
	}
	if drift == 0 {
		fmt.Printf("no drift across %d file(s)\n", len(files))
		return
	}
	fmt.Printf("%d of %d tenant(s) drifted\n", drift, len(files))
	os.Exit(2) // a non-zero exit makes this usable as a CI gate
}

// mustApply writes a reviewed file into the platform. Update snapshots the
// previous state, so an apply is itself revertible through `rollback`.
func mustApply(ctx context.Context, mgr *tenant.Manager, file string, dryRun bool) {
	if file == "" {
		fatal("apply requires -file <path>")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		fatal("read %s: %v", file, err)
	}
	var want tenant.Tenant
	if err := json.Unmarshal(raw, &want); err != nil {
		fatal("parse %s: %v", file, err)
	}
	if want.ID == "" {
		// Fall back to the file name so a hand-written file cannot silently
		// apply to the wrong tenant.
		want.ID = strings.TrimSuffix(filepath.Base(file), ".json")
	}
	if err := want.Validate(); err != nil {
		fatal("invalid tenant in %s: %v", file, err)
	}

	current, err := mgr.Get(ctx, want.ID)
	if err != nil {
		if dryRun {
			fmt.Printf("would create tenant %s\n", want.ID)
			return
		}
		if err := mgr.Create(ctx, &want); err != nil {
			fatal("create %s: %v", want.ID, err)
		}
		fmt.Printf("created tenant %s\n", want.ID)
		return
	}
	if sameConfig(current, &want) {
		fmt.Printf("tenant %s already matches %s (nothing to do)\n", want.ID, file)
		return
	}
	if dryRun {
		fmt.Printf("would update %s\n    db:   %s\n    file: %s\n", want.ID, compact(current), compact(&want))
		return
	}
	if err := mgr.Update(ctx, &want); err != nil {
		fatal("update %s: %v", want.ID, err)
	}
	vs, err := mgr.ConfigVersions(ctx, want.ID)
	if err == nil && len(vs) > 0 {
		fmt.Printf("updated tenant %s (recorded as config version %d)\n", want.ID, vs[0].Version)
		return
	}
	fmt.Printf("updated tenant %s\n", want.ID)
}

func mustVersions(ctx context.Context, mgr *tenant.Manager, id string) {
	if id == "" {
		fatal("versions requires -tenant <id>")
	}
	vs, err := mgr.ConfigVersions(ctx, id)
	if err != nil {
		fatal("config versions: %v", err)
	}
	if len(vs) == 0 {
		fmt.Printf("tenant %s has no recorded configuration versions\n", id)
		return
	}
	for _, v := range vs {
		fmt.Printf("v%-4d %s  %s\n", v.Version, v.CreatedAt.Format(time.RFC3339), compact(v.Config))
	}
}

func mustRollback(ctx context.Context, mgr *tenant.Manager, id string, version int) {
	if id == "" || version <= 0 {
		fatal("rollback requires -tenant <id> -version <n> (see `versions`)")
	}
	restored, err := mgr.RollbackConfig(ctx, id, version)
	if err != nil {
		fatal("rollback %s to v%d: %v", id, version, err)
	}
	fmt.Printf("tenant %s restored to config version %d: %s\n", id, version, compact(restored))
}

// sameConfig compares the fields a configuration version actually captures.
// UpdatedAt is excluded on purpose: it changes on every write, so including it
// would make every diff non-empty and train the operator to ignore the report.
func sameConfig(a, b *tenant.Tenant) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.ID == b.ID &&
		a.Name == b.Name &&
		a.Status == b.Status &&
		jsonEqual(a.DataBackend, b.DataBackend) &&
		jsonEqual(a.Quota, b.Quota) &&
		jsonEqual(a.AuditPolicy, b.AuditPolicy)
}

func jsonEqual(a, b any) bool {
	ra, err1 := json.Marshal(a)
	rb, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return string(ra) == string(rb)
}

func compact(t *tenant.Tenant) string {
	if t == nil {
		return "(nil)"
	}
	raw, _ := json.Marshal(t)
	return string(raw)
}
