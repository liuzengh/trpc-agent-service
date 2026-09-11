package bootstrap

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

type databaseTarget struct {
	Database, Schema, Role, Login                                            string
	Admin, Owner, Create, Membership, DatabaseCreate, Temporary, CrossSchema bool
}

func inspectDatabase(ctx context.Context, p *pgxpool.Pool) (databaseTarget, error) {
	var t databaseTarget
	err := p.QueryRow(ctx, `SELECT current_database(),COALESCE(current_schema(),''),current_user,session_user,
 r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls,
 COALESCE(n.nspowner=r.oid,false),COALESCE(has_schema_privilege(r.oid,n.oid,'CREATE'),false),
 EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid),has_database_privilege(current_user,current_database(),'CREATE'),has_database_privilege(current_user,current_database(),'TEMP'),
 EXISTS(SELECT 1 FROM pg_namespace other WHERE other.nspname IN ('control','gateway','runtime_session','public') AND has_schema_privilege(current_user,other.oid,'USAGE,CREATE'))
 FROM pg_roles r LEFT JOIN pg_namespace n ON n.nspname=current_schema() WHERE r.rolname=current_user`).Scan(&t.Database, &t.Schema, &t.Role, &t.Login, &t.Admin, &t.Owner, &t.Create, &t.Membership, &t.DatabaseCreate, &t.Temporary, &t.CrossSchema)
	if err != nil {
		return t, errors.New("inspect Worker database identity")
	}
	return t, nil
}
func checkDatabaseTargets(m, r databaseTarget) error {
	if m.Database == "" || m.Database != r.Database || m.Schema != "worker" || r.Schema != "worker" || m.Role != "worker_migrator" || r.Role != "worker_runtime" || m.Login != m.Role || r.Login != r.Role || m.Admin || r.Admin || m.Membership || r.Membership || !m.Owner || !m.Create || r.Owner || r.Create || m.DatabaseCreate || m.Temporary || m.CrossSchema || r.DatabaseCreate || r.Temporary || r.CrossSchema {
		return errors.New("Worker database requires isolated worker schema and direct worker_migrator/worker_runtime identities")
	}
	return nil
}
func openDatabase(ctx context.Context, c Config) (*pgxpool.Pool, error) {
	m, err := pgxpool.ParseConfig(c.MigrationDatabaseURL)
	if err != nil {
		return nil, errors.New("parse Worker migration database")
	}
	r, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, errors.New("parse Worker runtime database")
	}
	if m.ConnConfig.Host != r.ConnConfig.Host || m.ConnConfig.Port != r.ConnConfig.Port || m.ConnConfig.Database != r.ConnConfig.Database {
		return nil, errors.New("Worker migration/runtime database destinations differ")
	}
	m.MaxConns = 2
	r.MaxConns = c.Limits.MaxDatabaseConnections
	migration, err := pgxpool.NewWithConfig(ctx, m)
	if err != nil {
		return nil, errors.New("open Worker migration database")
	}
	defer migration.Close()
	runtime, err := pgxpool.NewWithConfig(ctx, r)
	if err != nil {
		return nil, errors.New("open Worker runtime database")
	}
	ok := false
	defer func() {
		if !ok {
			runtime.Close()
		}
	}()
	mt, err := inspectDatabase(ctx, migration)
	if err != nil {
		return nil, err
	}
	rt, err := inspectDatabase(ctx, runtime)
	if err != nil {
		return nil, err
	}
	if err = checkDatabaseTargets(mt, rt); err != nil {
		return nil, err
	}
	if err = migrations.ApplyForRuntime(ctx, migration, "worker_runtime"); err != nil {
		return nil, errors.New("apply Worker migrations")
	}
	var valid bool
	if err = runtime.QueryRow(ctx, `SELECT has_table_privilege(current_user,'worker.worker_schema_migrations','SELECT') AND NOT has_table_privilege(current_user,'worker.worker_schema_migrations','INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')`).Scan(&valid); err != nil || !valid {
		return nil, errors.New("Worker migration ledger permissions invalid")
	}
	ok = true
	return runtime, nil
}
