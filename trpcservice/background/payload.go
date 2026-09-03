package background

import "github.com/liuzengh/trpc-agent-service/trpcservice/storage"

type SessionJobPayload struct {
	StorageScope string `json:"storage_scope"`
	UserID       string `json:"user_id"`
	SessionID    string `json:"session_id"`
	TurnSeq      int64  `json:"turn_seq"`
}

type KnowledgeUpsertPayload struct {
	Document storage.KnowledgeDocument `json:"document"`
}

type KnowledgeDeletePayload struct {
	DocumentID string `json:"document_id"`
}

type MemoryMigrationPayload struct {
	MigrationID     string   `json:"migration_id"`
	UserIDs         []string `json:"user_ids"`
	ExpectedVersion int64    `json:"expected_version"`
}

type SessionMigrationPayload struct {
	MigrationID     string                         `json:"migration_id"`
	Sessions        []storage.SessionMigrationItem `json:"sessions"`
	ExpectedVersion int64                          `json:"expected_version"`
}
