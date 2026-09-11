package toolexec

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

const WorkItemTool = "create_work_item"

type WorkItemPayload struct {
	Title string `json:"title"`
}
type WorkItemReceipt struct {
	OperationID string `json:"operation_id"`
	WorkItemID  string `json:"work_item_id"`
}

// WorkItems is a real platform-local business backend, NOT an integration with
// external ticketing, payment or order systems. SQL uniqueness is its contract.
type WorkItems struct {
	db    *sql.DB
	mu    sync.Mutex
	items map[string]workItem
}
type workItem struct {
	operation Operation
	title     string
}

func NewWorkItems(db *sql.DB) *WorkItems { return &WorkItems{db: db, items: map[string]workItem{}} }
func (*WorkItems) Name() string          { return WorkItemTool }
func (*WorkItems) Idempotent() bool      { return true }
func workItemReceipt(op Operation) json.RawMessage {
	data, _ := json.Marshal(WorkItemReceipt{OperationID: op.ID, WorkItemID: "wi_" + Hash([]byte(op.ID))[:40]})
	return data
}
func (s *WorkItems) Execute(ctx context.Context, op Operation, payload json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var input WorkItemPayload
	if json.Unmarshal(payload, &input) != nil || strings.TrimSpace(input.Title) == "" || len(input.Title) > 512 || Hash(payload) != op.InputHash {
		return nil, ErrOperationRejected
	}
	if s.db == nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		if old, ok := s.items[op.ID]; ok {
			if !sameOperation(old.operation, op) {
				return nil, ErrOperationRejected
			}
		} else {
			s.items[op.ID] = workItem{operation: cloneOperation(op), title: input.Title}
		}
		return workItemReceipt(op), nil
	}
	var receipt WorkItemReceipt
	_ = json.Unmarshal(workItemReceipt(op), &receipt)
	_, err := s.db.ExecContext(ctx, `INSERT INTO work_item(work_item_id,operation_id,tenant_id,app_id,user_id,input_hash,title)
VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(operation_id) DO NOTHING`, receipt.WorkItemID, op.ID, op.TenantID, op.AppID, op.UserID, op.InputHash, input.Title)
	if err != nil {
		return nil, err
	}
	result, found, err := s.Lookup(ctx, op)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrOperationUnknown
	}
	return result, nil
}
func (s *WorkItems) Lookup(ctx context.Context, op Operation) (json.RawMessage, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.db == nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		item, ok := s.items[op.ID]
		if !ok {
			return nil, false, nil
		}
		if !sameOperation(item.operation, op) {
			return nil, false, ErrOperationConflict
		}
		return workItemReceipt(op), true, nil
	}
	var inputHash, workItemID string
	err := s.db.QueryRowContext(ctx, `SELECT input_hash,work_item_id FROM work_item WHERE operation_id=$1 AND tenant_id=$2 AND app_id=$3 AND user_id=$4`, op.ID, op.TenantID, op.AppID, op.UserID).Scan(&inputHash, &workItemID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if inputHash != op.InputHash {
		return nil, false, ErrOperationConflict
	}
	result, _ := json.Marshal(WorkItemReceipt{OperationID: op.ID, WorkItemID: workItemID})
	return result, true, nil
}

func NewOperationBackend(repository controlplane.Repository) (OperationStore, *WorkItems, error) {
	if repository == nil {
		return nil, nil, errors.New("operation control plane is required")
	}
	if sqlProvider, ok := repository.(interface{ SQLDB() *sql.DB }); ok {
		return NewPostgresOperations(sqlProvider.SQLDB()), NewWorkItems(sqlProvider.SQLDB()), nil
	}
	return NewMemoryOperations(), NewWorkItems(nil), nil
}
