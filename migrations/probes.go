package migrations

import "embed"

//go:embed probes/*.sql
var probeFiles embed.FS

func ContractProbes() (string, error) {
	body, err := probeFiles.ReadFile("probes/runtime.sql")
	return string(body), err
}
