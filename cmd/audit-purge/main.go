// Command audit-purge is the operator-facing control plane for compliance
// retention. Destructive actions (approve, quarantine) run only through the
// guarded compliance SQL functions and are never exposed over HTTP.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "audit-purge: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: audit-purge <approve|list|certificate|quarantine> ...")
	}
	dsn := strings.TrimSpace(os.Getenv("TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN"))
	if dsn == "" {
		return fmt.Errorf("TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	switch args[0] {
	case "approve":
		return approve(ctx, db, args[1:])
	case "list":
		return list(ctx, db, args[1:])
	case "certificate":
		return certificate(ctx, db, args[1:])
	case "quarantine":
		return quarantine(ctx, db, args[1:])
	case "plan":
		return plan(ctx, db, args[1:])
	case "execute":
		return execute(ctx, db, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// plan creates a durable purge intent for one (tenant, class) and cutoff.
func plan(ctx context.Context, db *sql.DB, args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	actor := fs.String("actor", "", "operator subject")
	reason := fs.String("reason", "manual", "plan reason")
	maxBatch := fs.Int64("max-batch", 50000, "maximum candidate count")
	_ = fs.Parse(args)
	rest := fs.Args()
	if *actor == "" || len(rest) != 3 {
		return fmt.Errorf("usage: plan -actor <sub> <tenant> <class> <cutoff-rfc3339>")
	}
	cutoff, err := time.Parse(time.RFC3339, rest[2])
	if err != nil {
		return fmt.Errorf("invalid cutoff: %w", err)
	}
	var batchID string
	if err := db.QueryRowContext(ctx, `SELECT compliance.plan_audit_purge_batch($1,$2,$3,$4,$5,interval '1 hour',$6)`,
		rest[0], rest[1], cutoff, *actor, *reason, *maxBatch).Scan(&batchID); err != nil {
		return err
	}
	fmt.Printf("planned batch %s/%s\n", rest[0], batchID)
	return nil
}

// execute runs the guarded purge function once for a batch and reports the code.
func execute(ctx context.Context, db *sql.DB, args []string) error {
	fs := flag.NewFlagSet("execute", flag.ExitOnError)
	owner := fs.String("owner", "", "operator subject")
	_ = fs.Parse(args)
	rest := fs.Args()
	if *owner == "" || len(rest) != 2 {
		return fmt.Errorf("usage: execute -owner <sub> <tenant> <batch>")
	}
	var code string
	if err := db.QueryRowContext(ctx, `SELECT compliance.execute_audit_purge_batch($1,$2,$3)`, rest[0], rest[1], *owner).Scan(&code); err != nil {
		return err
	}
	fmt.Printf("batch %s/%s: %s\n", rest[0], rest[1], code)
	return nil
}

func approve(ctx context.Context, db *sql.DB, args []string) error {
	fs := flag.NewFlagSet("approve", flag.ExitOnError)
	approver := fs.String("approver", "", "approver subject")
	reason := fs.String("reason", "", "approval reason")
	_ = fs.Parse(args)
	rest := fs.Args()
	if *approver == "" || len(rest) != 2 {
		return fmt.Errorf("usage: approve -approver <sub> -reason <why> <tenant> <batch>")
	}
	_, err := db.ExecContext(ctx, `SELECT compliance.approve_audit_purge_batch($1,$2,$3,$4)`, rest[0], rest[1], *approver, *reason)
	if err != nil {
		return err
	}
	fmt.Printf("approved batch %s/%s\n", rest[0], rest[1])
	return nil
}

func list(ctx context.Context, db *sql.DB, args []string) error {
	query := `SELECT tenant_id,batch_id,state,class,cutoff_at,planned_count,deleted_count,delete_attempt,COALESCE(last_error_class,'')
FROM compliance.audit_purge_batch ORDER BY tenant_id,batch_id`
	var rows *sql.Rows
	var err error
	if len(args) == 1 {
		rows, err = db.QueryContext(ctx, `SELECT tenant_id,batch_id,state,class,cutoff_at,planned_count,deleted_count,delete_attempt,COALESCE(last_error_class,'')
FROM compliance.audit_purge_batch WHERE tenant_id=$1 ORDER BY batch_id`, args[0])
	} else if len(args) == 0 {
		rows, err = db.QueryContext(ctx, query)
	} else {
		return fmt.Errorf("usage: list [tenant]")
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tenantID, batchID, state, class, lastErr string
		var cutoff time.Time
		var planned, deleted, attempts int64
		if err := rows.Scan(&tenantID, &batchID, &state, &class, &cutoff, &planned, &deleted, &attempts, &lastErr); err != nil {
			return err
		}
		fmt.Printf("%s\t%s\t%s\t%s\t%s\tplanned=%d\tdeleted=%d\tattempts=%d\t%s\n",
			tenantID, batchID, state, class, cutoff.Format(time.RFC3339), planned, deleted, attempts, lastErr)
	}
	return rows.Err()
}

func certificate(ctx context.Context, db *sql.DB, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: certificate <tenant> <batch>")
	}
	var from, to time.Time
	var count, alertCount, policyVersion, floorVersion int64
	var digest, class, approver, reason string
	err := db.QueryRowContext(ctx, `SELECT from_occurred_at,to_occurred_at,count,alert_count,event_digest,policy_version,floor_version,class,approved_by,reason
FROM compliance.audit_purge_certificate WHERE tenant_id=$1 AND batch_id=$2`, args[0], args[1]).Scan(
		&from, &to, &count, &alertCount, &digest, &policyVersion, &floorVersion, &class, &approver, &reason)
	if err != nil {
		return err
	}
	fmt.Printf("tenant=%s batch=%s class=%s count=%d alerts=%d policy=%d floor=%d approver=%s\nwindow=%s .. %s\ndigest=%s\nreason=%s\n",
		args[0], args[1], class, count, alertCount, policyVersion, floorVersion, approver,
		from.Format(time.RFC3339), to.Format(time.RFC3339), digest, reason)
	return nil
}

func quarantine(ctx context.Context, db *sql.DB, args []string) error {
	fs := flag.NewFlagSet("quarantine", flag.ExitOnError)
	owner := fs.String("owner", "", "operator subject")
	errClass := fs.String("error", "manual", "error class")
	_ = fs.Parse(args)
	rest := fs.Args()
	if *owner == "" || len(rest) != 2 {
		return fmt.Errorf("usage: quarantine -owner <sub> <tenant> <batch>")
	}
	if _, err := db.ExecContext(ctx, `SELECT compliance.quarantine_audit_purge_batch($1,$2,$3,$4)`, rest[0], rest[1], *owner, *errClass); err != nil {
		return err
	}
	fmt.Printf("quarantined batch %s/%s\n", rest[0], rest[1])
	return nil
}
