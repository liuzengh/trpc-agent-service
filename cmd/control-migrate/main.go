package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
)

func main() {
	defaultPath := os.Getenv("TRPC_CONTROL_PLANE_SQLITE_PATH")
	if defaultPath == "" {
		defaultPath = "data/control-plane.db"
	}
	path := flag.String("sqlite", defaultPath, "SQLite control-plane path")
	postgres := flag.String("postgres", os.Getenv("TRPC_CONTROL_PLANE_POSTGRES_DSN"), "PostgreSQL control-plane DSN")
	flag.Parse()
	var err error
	if *postgres != "" {
		err = platform.MigratePostgresControlPlane(*postgres)
	} else {
		err = platform.MigrateSQLiteControlPlane(*path)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: control-plane migration failed")
		os.Exit(1)
	}
	fmt.Printf("control-plane schema is at version %d\n", platform.ControlPlaneSchemaVersion)
}
