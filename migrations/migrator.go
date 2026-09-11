// Package migrations applies the repository's current PostgreSQL baseline.
package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
)

//go:embed 000001_init.sql
var baselineSQL string

const (
	baselineVersion int64 = 1
	baselineName          = "000001_init.sql"
)

// Apply installs the repository's single mutable development baseline. The
// recorded checksum prevents an older database that already marked version 1
// as applied from silently running against a newer 000001 schema.
func Apply(ctx context.Context, database *sql.DB) error {
	if database == nil {
		return fmt.Errorf("migration database is required")
	}
	digest := sha256.Sum256([]byte(baselineSQL))
	checksum := fmt.Sprintf("%x", digest)

	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "trpc-agent-service:migrations"); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}

	if _, err := transaction.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
	checksum CHAR(64) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`); err != nil {
		return fmt.Errorf("ensure migration table: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, "ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum CHAR(64)"); err != nil {
		return fmt.Errorf("ensure migration checksum column: %w", err)
	}

	var unexpectedVersion int64
	err = transaction.QueryRowContext(
		ctx,
		"SELECT version FROM schema_migrations WHERE version <> $1 ORDER BY version LIMIT 1",
		baselineVersion,
	).Scan(&unexpectedVersion)
	if err == nil {
		return fmt.Errorf("database contains unsupported migration version %d; development schema uses only %s", unexpectedVersion, baselineName)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check migration versions: %w", err)
	}

	var appliedChecksum sql.NullString
	err = transaction.QueryRowContext(
		ctx,
		"SELECT checksum FROM schema_migrations WHERE version = $1",
		baselineVersion,
	).Scan(&appliedChecksum)
	switch {
	case err == nil:
		if !appliedChecksum.Valid || strings.TrimSpace(appliedChecksum.String) != checksum {
			return fmt.Errorf("database schema baseline %s has changed; rebuild the development database from the current baseline", baselineName)
		}
	case !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("read baseline state: %w", err)
	default:
		for _, statement := range splitStatements(baselineSQL) {
			if _, err := transaction.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply baseline %s: %w", baselineName, err)
			}
		}
		if _, err := transaction.ExecContext(
			ctx,
			"INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)",
			baselineVersion, checksum,
		); err != nil {
			return fmt.Errorf("record baseline: %w", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func splitStatements(source string) []string {
	const (
		normal = iota
		lineComment
		blockComment
		singleQuoted
		doubleQuoted
		dollarQuoted
	)

	statements := make([]string, 0)
	state := normal
	statementStart := 0
	blockCommentDepth := 0
	dollarDelimiter := ""
	hasSQLToken := false

	for index := 0; index < len(source); {
		switch state {
		case normal:
			switch source[index] {
			case '-':
				if index+1 < len(source) && source[index+1] == '-' {
					state = lineComment
					index += 2
					continue
				}
			case '/':
				if index+1 < len(source) && source[index+1] == '*' {
					state = blockComment
					blockCommentDepth = 1
					index += 2
					continue
				}
			case '\'':
				hasSQLToken = true
				state = singleQuoted
				index++
				continue
			case '"':
				hasSQLToken = true
				state = doubleQuoted
				index++
				continue
			case '$':
				if delimiter, ok := dollarQuoteDelimiter(source[index:]); ok {
					hasSQLToken = true
					state = dollarQuoted
					dollarDelimiter = delimiter
					index += len(delimiter)
					continue
				}
			case ';':
				if statement := strings.TrimSpace(source[statementStart:index]); hasSQLToken && statement != "" {
					statements = append(statements, statement)
				}
				statementStart = index + 1
				hasSQLToken = false
				index++
				continue
			}
			if !isSQLWhitespace(source[index]) {
				hasSQLToken = true
			}
			index++
		case lineComment:
			if source[index] == '\n' {
				state = normal
			}
			index++
		case blockComment:
			if index+1 < len(source) {
				if source[index] == '/' && source[index+1] == '*' {
					blockCommentDepth++
					index += 2
					continue
				}
				if source[index] == '*' && source[index+1] == '/' {
					blockCommentDepth--
					index += 2
					if blockCommentDepth == 0 {
						state = normal
					}
					continue
				}
			}
			index++
		case singleQuoted:
			if source[index] == '\'' {
				index++
				if index < len(source) && source[index] == '\'' {
					index++
					continue
				}
				state = normal
				continue
			}
			index++
		case doubleQuoted:
			if source[index] == '"' {
				index++
				if index < len(source) && source[index] == '"' {
					index++
					continue
				}
				state = normal
				continue
			}
			index++
		case dollarQuoted:
			if strings.HasPrefix(source[index:], dollarDelimiter) {
				index += len(dollarDelimiter)
				state = normal
				continue
			}
			index++
		}
	}
	if statement := strings.TrimSpace(source[statementStart:]); hasSQLToken && statement != "" {
		statements = append(statements, statement)
	}
	return statements
}

func isSQLWhitespace(value byte) bool {
	switch value {
	case ' ', '\t', '\r', '\n', '\f':
		return true
	default:
		return false
	}
}

func dollarQuoteDelimiter(source string) (string, bool) {
	if len(source) < 2 || source[0] != '$' {
		return "", false
	}
	if source[1] == '$' {
		return "$$", true
	}
	if !isDollarQuoteTagStart(source[1]) {
		return "", false
	}
	for index := 2; index < len(source); index++ {
		if source[index] == '$' {
			return source[:index+1], true
		}
		if !isDollarQuoteTagPart(source[index]) {
			return "", false
		}
	}
	return "", false
}

func isDollarQuoteTagStart(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isDollarQuoteTagPart(value byte) bool {
	return isDollarQuoteTagStart(value) || value >= '0' && value <= '9'
}
