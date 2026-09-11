package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"

	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestCommitPublicationLocksThenRechecksReceiptBeforeLatestCAS(t *testing.T) {
	commit := publicationCommitFixture()
	stale := int64(1)
	commit.ExpectedLatestRevisionNumber = &stale
	winner := commit.Receipt
	winner.Result = json.RawMessage(`{"published":{"revision":{"id":"winner"}}}`)
	tx := &transactionStub{rows: []pgx.Row{
		rowFunc(func(dest ...any) error {
			latest := int64(9)
			*dest[0].(**int64) = &latest
			return nil
		}),
		receiptRow(winner),
	}}
	store := NewStore(&transactionDBStub{tx: tx})

	result, err := store.CommitPublication(context.Background(), commit)
	if err != nil {
		t.Fatalf("CommitPublication() error = %v", err)
	}
	if result.Created || string(result.Receipt.Result) != string(winner.Result) {
		t.Fatalf("result = %#v", result)
	}
	if !tx.committed || len(tx.actions) != 2 ||
		!strings.Contains(strings.ToLower(tx.actions[0]), "for update") ||
		!strings.Contains(strings.ToLower(tx.actions[1]), "deployment_command_receipts") {
		t.Fatalf("transaction actions = %#v committed=%v", tx.actions, tx.committed)
	}
}

func TestCommitPublicationChecksLatestOnlyAfterMissingReceipt(t *testing.T) {
	commit := publicationCommitFixture()
	stale := int64(1)
	commit.ExpectedLatestRevisionNumber = &stale
	tx := &transactionStub{rows: []pgx.Row{
		rowFunc(func(dest ...any) error {
			latest := int64(2)
			*dest[0].(**int64) = &latest
			return nil
		}),
		rowFunc(func(...any) error { return pgx.ErrNoRows }),
	}}
	store := NewStore(&transactionDBStub{tx: tx})

	_, err := store.CommitPublication(context.Background(), commit)
	if !errors.Is(err, application.ErrLatestRevisionConflict) {
		t.Fatalf("CommitPublication() error = %v", err)
	}
	if tx.committed || !tx.rolledBack || len(tx.actions) != 2 {
		t.Fatalf("transaction actions = %#v committed=%v rolledBack=%v", tx.actions, tx.committed, tx.rolledBack)
	}
}

func TestCommitPublicationAtomicallyWritesRevisionManifestOutboxReceiptAndLatest(t *testing.T) {
	commit := publicationCommitFixture()
	tx := &transactionStub{
		rows: []pgx.Row{
			rowFunc(func(dest ...any) error {
				*dest[0].(**int64) = nil
				return nil
			}),
			rowFunc(func(...any) error { return pgx.ErrNoRows }),
			rowFunc(func(dest ...any) error {
				*dest[0].(*string) = commit.Revision.ID
				return nil
			}),
			receiptRow(commit.Receipt),
		},
		execTags: []pgconn.CommandTag{
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("INSERT 0 1"),
			pgconn.NewCommandTag("UPDATE 1"),
		},
	}
	store := NewStore(&transactionDBStub{tx: tx})

	result, err := store.CommitPublication(context.Background(), commit)
	if err != nil {
		t.Fatalf("CommitPublication() error = %v", err)
	}
	if !result.Created || result.Receipt.RuntimeManifestID != commit.Manifest.ID || !tx.committed {
		t.Fatalf("result = %#v committed=%v", result, tx.committed)
	}
	wantOrder := []string{
		"for update", "deployment_command_receipts", "insert into deployment_revisions",
		"insert into runtime_manifests", "insert into control_outbox",
		"insert into deployment_command_receipts", "update deployments",
	}
	if len(tx.actions) != len(wantOrder) {
		t.Fatalf("transaction actions = %#v", tx.actions)
	}
	for index, fragment := range wantOrder {
		if !strings.Contains(strings.ToLower(tx.actions[index]), fragment) {
			t.Fatalf("action[%d] = %q, want fragment %q", index, tx.actions[index], fragment)
		}
	}
}

func TestValidatePublicationCommitRejectsInconsistentImmutableDocuments(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*application.PublicationCommit)
	}{
		{
			name: "input digest",
			mutate: func(commit *application.PublicationCommit) {
				commit.Revision.InputDigest = "sha256:" + strings.Repeat("0", 64)
			},
		},
		{
			name: "input unknown field",
			mutate: func(commit *application.PublicationCommit) {
				tampered := append(commit.Revision.CanonicalInput[:len(commit.Revision.CanonicalInput)-1], []byte(`,"unknown":true}`)...)
				commit.Revision.CanonicalInput, commit.Revision.InputDigest, _ = canonicalDocument(tampered)
			},
		},
		{
			name: "manifest content digest",
			mutate: func(commit *application.PublicationCommit) {
				commit.Manifest.ContentDigest = "sha256:" + strings.Repeat("0", 64)
			},
		},
		{
			name: "manifest source tenant",
			mutate: func(commit *application.PublicationCommit) {
				var content domain.ManifestContent
				if err := json.Unmarshal(commit.Manifest.Content, &content); err != nil {
					panic(err)
				}
				content.TenantID = "tnt_other"
				encoded, err := json.Marshal(content)
				if err != nil {
					panic(err)
				}
				commit.Manifest.Content, commit.Manifest.ContentDigest, _ = canonicalDocument(encoded)
			},
		},
		{
			name: "event payload digest",
			mutate: func(commit *application.PublicationCommit) {
				commit.OutboxEvent.PayloadDigest = "sha256:" + strings.Repeat("0", 64)
			},
		},
		{
			name: "receipt manifest view",
			mutate: func(commit *application.PublicationCommit) {
				var result publicationReceiptResult
				if err := json.Unmarshal(commit.Receipt.Result, &result); err != nil {
					panic(err)
				}
				result.Published.ManifestView = json.RawMessage(`{"schema_version":"v1"}`)
				encoded, err := json.Marshal(result)
				if err != nil {
					panic(err)
				}
				commit.Receipt.Result = encoded
			},
		},
		{
			name: "receipt private manifest content",
			mutate: func(commit *application.PublicationCommit) {
				var result publicationReceiptResult
				if err := json.Unmarshal(commit.Receipt.Result, &result); err != nil {
					panic(err)
				}
				result.Published.ManifestView = append(json.RawMessage(nil), commit.Manifest.Content...)
				encoded, err := json.Marshal(result)
				if err != nil {
					panic(err)
				}
				commit.Receipt.Result = encoded
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commit := publicationCommitFixture()
			test.mutate(&commit)
			if err := validatePublicationCommit(commit); !errors.Is(err, application.ErrPublicationIntegrity) {
				t.Fatalf("validatePublicationCommit() error = %v, want %v", err, application.ErrPublicationIntegrity)
			}
		})
	}
}

func TestCreateDeploymentUniqueReceiptRaceRollsBackThenReadsWinner(t *testing.T) {
	commit := createCommitFixture()
	winner := commit.Receipt
	winner.DeploymentID = "dep_winner"
	winner.Result = json.RawMessage(`{"deployment":{"id":"dep_winner"}}`)
	tx := &transactionStub{
		rows: []pgx.Row{
			rowFunc(func(...any) error { return pgx.ErrNoRows }),
			rowFunc(func(...any) error {
				return &pgconn.PgError{
					Code: "23505", ConstraintName: "deployment_command_receipts_pkey",
				}
			}),
		},
		execTags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1")},
	}
	db := &transactionDBStub{tx: tx, row: receiptRow(winner)}
	store := NewStore(db)

	result, err := store.CreateDeployment(context.Background(), commit)
	if err != nil {
		t.Fatalf("CreateDeployment() error = %v", err)
	}
	if result.Created || result.Receipt.DeploymentID != winner.DeploymentID ||
		!tx.rolledBack || tx.committed {
		t.Fatalf("result=%#v committed=%v rolledBack=%v", result, tx.committed, tx.rolledBack)
	}
	if len(tx.actions) != 3 ||
		!strings.Contains(strings.ToLower(tx.actions[0]), "deployment_command_receipts") ||
		!strings.Contains(strings.ToLower(tx.actions[1]), "insert into deployments") ||
		!strings.Contains(strings.ToLower(tx.actions[2]), "insert into deployment_command_receipts") {
		t.Fatalf("transaction actions = %#v", tx.actions)
	}
	if !strings.Contains(strings.ToLower(db.query), "deployment_command_receipts") {
		t.Fatalf("winner lookup query = %q", db.query)
	}
}

func TestFindCreateReceiptCoalescesNullablePublicationReferences(t *testing.T) {
	commit := createCommitFixture()
	db := &transactionDBStub{row: receiptRow(commit.Receipt)}
	store := NewStore(db)

	receipt, found, err := store.FindCommandReceipt(
		context.Background(), receiptKey(commit.Receipt),
	)
	if err != nil || !found {
		t.Fatalf("FindCommandReceipt() found=%v error=%v", found, err)
	}
	if receipt.DeploymentRevisionID != "" || receipt.RuntimeManifestID != "" {
		t.Fatalf("nullable references = %#v", receipt)
	}
	query := strings.ToLower(db.query)
	if !strings.Contains(query, "coalesce(deployment_revision_id, '')") ||
		!strings.Contains(query, "coalesce(runtime_manifest_id, '')") {
		t.Fatalf("receipt query does not normalize nullable references:\n%s", db.query)
	}
}

func TestValidateCreateCommitRejectsReceiptThatIsNotTheClosedCreationSnapshot(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*application.CreateCommit)
	}{
		{
			name: "partial deployment",
			mutate: func(commit *application.CreateCommit) {
				commit.Receipt.Result = json.RawMessage(`{"deployment":{"id":"dep_loser"}}`)
			},
		},
		{
			name: "different deployment metadata",
			mutate: func(commit *application.CreateCommit) {
				result := createReceiptResult{Deployment: commit.Deployment}
				result.Deployment.Name = "Other"
				commit.Receipt.Result, _ = json.Marshal(result)
			},
		},
		{
			name: "unknown receipt field",
			mutate: func(commit *application.CreateCommit) {
				commit.Receipt.Result = append(commit.Receipt.Result[:len(commit.Receipt.Result)-1], []byte(`,"private":"value"}`)...)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commit := createCommitFixture()
			test.mutate(&commit)
			if err := validateCreateCommit(commit); !errors.Is(err, application.ErrInvalidDeployment) {
				t.Fatalf("validateCreateCommit() error = %v, want %v", err, application.ErrInvalidDeployment)
			}
		})
	}
}

func createCommitFixture() application.CreateCommit {
	now := time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)
	digest := "sha256:" + strings.Repeat("b", 64)
	deployment := domain.Deployment{
		ID: "dep_loser", TenantID: "tnt_1", Name: "Deployment",
		MetadataRevision: 1, CreatedBy: "usr_1", CreatedAt: now, UpdatedAt: now,
	}
	result, _ := json.Marshal(createReceiptResult{Deployment: deployment})
	return application.CreateCommit{
		Deployment: deployment,
		Receipt: domain.CommandReceipt{
			TenantID: "tnt_1", Operation: "create", ScopeID: "tnt_1",
			KeyHash: digest, RequestDigest: digest, DeploymentID: deployment.ID,
			Result:    result,
			CreatedBy: "usr_1", CreatedAt: now,
		},
	}
}

func publicationCommitFixture() application.PublicationCommit {
	return publicationCommitFixtureWithPlatform(domain.DefaultPlatformExecutionContract())
}

func publicationCommitFixtureWithPlatform(platform domain.PlatformExecutionContract) application.PublicationCommit {
	username := "agent" // Preserve historical platform-v1 fixture bytes.
	if platform.Version == deploymentv1.WorkerV1PlatformVersion {
		username = deploymentv1.WorkerV1SessionRuntimeRole
	}
	now := time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)
	agentDigest := "sha256:" + strings.Repeat("a", 64)
	profileDigest := "sha256:" + strings.Repeat("b", 64)
	input := domain.DeploymentInput{
		SchemaVersion: domain.SchemaVersionV1,
		Agent:         domain.AgentInput{AgentID: "agt_1", VersionNumber: 2},
		Profile:       domain.ProfileInput{ProfileID: "rpf_1", RevisionNumber: 3},
	}
	inputJSON, _ := json.Marshal(input)
	canonicalInput, inputDigest, _ := canonicalDocument(inputJSON)
	compiled, report := domain.Compile(domain.CompileInput{
		TenantID: "tnt_1",
		Agent: domain.AgentVersionSource{
			TenantID: "tnt_1", AgentID: "agt_1", VersionID: "agv_2", VersionNumber: 2,
			SchemaVersion: domain.SchemaVersionV1, SpecDigest: agentDigest,
			Spec: agentdomain.Spec{
				SchemaVersion: domain.SchemaVersionV1, Root: "assistant",
				Requirements: agentdomain.Requirements{
					Models: map[string]agentdomain.ModelRequirement{
						"primary": {Capabilities: []string{profiledomain.CapabilityChat}},
					},
					Tools: map[string]agentdomain.CapabilityRequirement{}, Knowledge: map[string]agentdomain.CapabilityRequirement{},
				},
				Nodes: map[string]agentdomain.Node{
					"assistant": {
						Kind: agentdomain.NodeKindLLM, Instruction: "Answer.", ModelSlot: "primary",
						ToolSlots: []string{}, KnowledgeSlots: []string{},
					},
				},
			},
		},
		Profile: domain.ProfileRevisionSource{
			TenantID: "tnt_1", ProfileID: "rpf_1", RevisionID: "rpr_3", RevisionNumber: 3,
			SchemaVersion: domain.SchemaVersionV1, SpecDigest: profileDigest,
			Spec: profiledomain.Spec{
				SchemaVersion: domain.SchemaVersionV1, CredentialProtocolVersion: profiledomain.CredentialProtocolVersionV1,
				Models: map[string]profiledomain.ModelResource{
					"primary": {
						Kind: profiledomain.ModelKindOpenAICompatible, Model: "gpt-test",
						BaseURL:            "https://model.example/v1",
						APIKeyCredentialID: "crd_00000000000000000000000000000001",
						Capabilities:       []string{profiledomain.CapabilityChat},
					},
				},
				Tools: map[string]profiledomain.ToolResource{}, Knowledge: map[string]profiledomain.KnowledgeResource{},
				Storage: map[string]profiledomain.StorageResource{
					"session": {
						Kind:            profiledomain.StorageKindPostgresState,
						DSNCredentialID: "crd_00000000000000000000000000000002",
						Destination: profiledomain.StorageDestination{
							Host: "state.example", Port: 5432, Database: "state",
							Username: username, SSLMode: "verify-full",
						},
					},
				},
			},
		},
		Platform: platform,
	})
	if !report.Valid {
		panic("invalid publication fixture")
	}
	revision := domain.DeploymentRevision{
		ID: "dpr_1", TenantID: "tnt_1", DeploymentID: "dep_1", RevisionNumber: 1,
		SchemaVersion: domain.SchemaVersionV1, Input: input, CanonicalInput: canonicalInput,
		InputDigest: inputDigest, AgentVersionID: "agv_2", AgentSchemaVersion: "v1",
		AgentSpecDigest: agentDigest, ProfileRevisionID: "rpr_3",
		ProfileSchemaVersion: "v1", ProfileSpecDigest: profileDigest,
		PublishedBy: "usr_1", PublishedAt: now,
	}
	manifest := domain.RuntimeManifest{
		ID: "rmf_1", TenantID: "tnt_1", DeploymentID: "dep_1",
		DeploymentRevisionID: revision.ID, RevisionNumber: 1,
		SchemaVersion: compiled.Content.SchemaVersion, CompilerVersion: compiled.Content.CompilerVersion,
		RuntimeContractVersion: compiled.Content.RuntimeContractVersion,
		Content:                compiled.CanonicalContent,
		ContentDigest:          compiled.ContentDigest, PublishedAt: now,
	}
	event := domain.OutboxEvent{
		ID: "evt_1", TenantID: "tnt_1", AggregateType: "deployment",
		AggregateID: "dep_1", AggregateRevision: 1,
		EventType: domain.ManifestPublishedType, SchemaVersion: domain.EventSchemaVersionV1,
		CreatedAt: now,
	}
	eventJSON, _ := json.Marshal(domain.RuntimeManifestPublishedEvent{
		SchemaVersion: event.SchemaVersion, EventType: event.EventType, EventID: event.ID,
		TenantID: event.TenantID, DeploymentID: event.AggregateID,
		DeploymentRevisionID: revision.ID, RevisionNumber: revision.RevisionNumber,
		OccurredAt: now, Manifest: manifest,
	})
	event.Payload, event.PayloadDigest, _ = canonicalDocument(eventJSON)
	manifestView, _ := json.Marshal(domain.NewPublicManifestView(compiled.Content))
	published := domain.PublishedRevision{
		Revision: revision, ManifestID: manifest.ID,
		ManifestDigest: manifest.ContentDigest, ManifestView: manifestView,
	}
	receiptJSON, _ := json.Marshal(publicationReceiptResult{Published: published, Validation: report})
	requestDigest := "sha256:" + strings.Repeat("c", 64)
	receipt := domain.CommandReceipt{
		TenantID: "tnt_1", Operation: "publish", ScopeID: "dep_1",
		KeyHash: requestDigest, RequestDigest: requestDigest, DeploymentID: "dep_1",
		DeploymentRevisionID: revision.ID, RuntimeManifestID: manifest.ID,
		Result:    receiptJSON,
		CreatedBy: "usr_1", CreatedAt: now,
	}
	return application.PublicationCommit{
		Revision: revision, Manifest: manifest, OutboxEvent: event, Receipt: receipt,
	}
}

type transactionDBStub struct {
	tx    pgx.Tx
	row   pgx.Row
	query string
}

func (stub *transactionDBStub) Begin(context.Context) (pgx.Tx, error) { return stub.tx, nil }

func (stub *transactionDBStub) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	stub.query = query
	if stub.row != nil {
		return stub.row
	}
	return rowFunc(func(...any) error { return pgx.ErrNoRows })
}
func (*transactionDBStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

type rowFunc func(...any) error

func (fn rowFunc) Scan(dest ...any) error { return fn(dest...) }

func receiptRow(receipt domain.CommandReceipt) pgx.Row {
	return rowFunc(func(dest ...any) error {
		values := []string{
			receipt.TenantID, receipt.Operation, receipt.ScopeID, receipt.KeyHash,
			receipt.RequestDigest, receipt.DeploymentID, receipt.DeploymentRevisionID,
			receipt.RuntimeManifestID,
		}
		for index, value := range values {
			*dest[index].(*string) = value
		}
		*dest[8].(*json.RawMessage) = append(json.RawMessage(nil), receipt.Result...)
		*dest[9].(*string) = receipt.CreatedBy
		*dest[10].(*time.Time) = receipt.CreatedAt
		return nil
	})
}

type transactionStub struct {
	rows       []pgx.Row
	execTags   []pgconn.CommandTag
	actions    []string
	committed  bool
	rolledBack bool
}

func (stub *transactionStub) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("unexpected nested transaction")
}
func (stub *transactionStub) Commit(context.Context) error {
	stub.committed = true
	return nil
}
func (stub *transactionStub) Rollback(context.Context) error {
	stub.rolledBack = true
	return nil
}
func (*transactionStub) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("unexpected CopyFrom")
}
func (*transactionStub) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (stub *transactionStub) LargeObjects() pgx.LargeObjects                    { return pgx.LargeObjects{} }
func (*transactionStub) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("unexpected Prepare")
}
func (stub *transactionStub) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	stub.actions = append(stub.actions, query)
	if len(stub.execTags) == 0 {
		return pgconn.CommandTag{}, errors.New("unexpected Exec")
	}
	tag := stub.execTags[0]
	stub.execTags = stub.execTags[1:]
	return tag, nil
}
func (*transactionStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected Query")
}
func (stub *transactionStub) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	stub.actions = append(stub.actions, query)
	if len(stub.rows) == 0 {
		return rowFunc(func(...any) error { return errors.New("unexpected QueryRow") })
	}
	row := stub.rows[0]
	stub.rows = stub.rows[1:]
	return row
}
func (*transactionStub) Conn() *pgx.Conn { return nil }
