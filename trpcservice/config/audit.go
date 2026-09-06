package config

import (
	"os"
	"strings"
)

type AuditConfig struct {
	SpoolDirectory     string
	MaxBufferedRecords int
}

func LoadAuditConfigFromEnv() (AuditConfig, error) {
	limit, err := parsePositiveIntEnv("TRPC_AGENT_AUDIT_SPOOL_MAX_RECORDS", 10000)
	return AuditConfig{strings.TrimSpace(os.Getenv("TRPC_AGENT_AUDIT_SPOOL_DIR")), limit}, err
}
