package skill

import (
	"context"
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"
)

// mysqlStore persists skill metadata to MySQL. It implements the store
// contract; the 005_skills.sql schema must be applied (via deployments/mysql/init
// or the testcontainers init script) before opening this manager.
type mysqlStore struct {
	db *sql.DB
}

func newMySQLStore(db *sql.DB) *mysqlStore { return &mysqlStore{db: db} }

const skillCols = "skill_id, scope, owner_tenant_id, code, name, description, current_version, status, created_at, updated_at"

const skillVersionCols = "skill_id, version, content_md, checksum, prompt_template, executor_type, timeout_seconds, status, published_at"

const agentSkillCols = "agent_id, skill_id, version, sort_order, created_at"

func isDup(err error) bool {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == 1062
	}
	return false
}

// ----- Skill -----

func (s *mysqlStore) Create(ctx context.Context, sk *Skill) error {
	var owner any
	if sk.OwnerTenantID != nil {
		owner = *sk.OwnerTenantID
	}
	var desc any
	if sk.Description != "" {
		desc = sk.Description
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO skills (skill_id, scope, owner_tenant_id, code, name, description, current_version, status)
		 VALUES (?, ?, ?, ?, ?, ?, 0, 'draft')`,
		sk.SkillID, sk.Scope, owner, sk.Code, sk.Name, desc)
	if isDup(err) {
		return ErrSkillCodeExists
	}
	if err != nil {
		return err
	}
	return s.fillSkill(ctx, sk)
}

func (s *mysqlStore) Update(ctx context.Context, sk *Skill) error {
	var desc any
	if sk.Description != "" {
		desc = sk.Description
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE skills SET name = ?, description = ?, status = ? WHERE skill_id = ? AND is_deleted = 0`,
		sk.Name, desc, sk.Status, sk.SkillID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrSkillNotFound
	}
	return s.fillSkill(ctx, sk)
}

func (s *mysqlStore) Delete(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`UPDATE skills SET is_deleted = 1 WHERE skill_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM agent_skills WHERE skill_id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *mysqlStore) Get(ctx context.Context, id string) (*Skill, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+skillCols+` FROM skills WHERE skill_id = ? AND is_deleted = 0`, id)
	sk, err := scanSkill(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrSkillNotFound
	}
	return sk, err
}

func (s *mysqlStore) List(ctx context.Context, tenantID string) ([]*Skill, error) {
	q := `SELECT ` + skillCols + ` FROM skills WHERE is_deleted = 0 AND scope = 'global'`
	args := []any{}
	if tenantID != "" {
		q += ` OR (scope = 'tenant' AND owner_tenant_id = ?)`
		args = append(args, tenantID)
	}
	q += ` ORDER BY code`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]*Skill, 0, 8)
	for rows.Next() {
		sk, err := scanSkill(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sk)
	}
	return out, rows.Err()
}

// fillSkill reloads the row into sk so timestamps + default fields are
// populated after a write.
func (s *mysqlStore) fillSkill(ctx context.Context, sk *Skill) error {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+skillCols+` FROM skills WHERE skill_id = ?`, sk.SkillID)
	got, err := scanSkill(row)
	if err != nil {
		return err
	}
	*sk = *got
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSkill(s rowScanner) (*Skill, error) {
	var (
		sk      Skill
		owner   sql.NullString
		desc    sql.NullString
		current int
	)
	if err := s.Scan(&sk.SkillID, &sk.Scope, &owner, &sk.Code, &sk.Name, &desc, &current, &sk.Status, &sk.CreatedAt, &sk.UpdatedAt); err != nil {
		return nil, err
	}
	if owner.Valid {
		v := owner.String
		sk.OwnerTenantID = &v
	}
	if desc.Valid {
		sk.Description = desc.String
	}
	sk.CurrentVersion = current
	return &sk, nil
}

// ----- Version -----

func (s *mysqlStore) CreateVersion(ctx context.Context, v *SkillVersion) error {
	var prompt any
	if v.PromptTemplate != "" {
		prompt = v.PromptTemplate
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO skill_versions (skill_id, version, content_md, checksum, prompt_template, executor_type, timeout_seconds, status)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 'draft')`,
		v.SkillID, v.Version, v.ContentMD, v.Checksum, prompt, v.ExecutorType, v.TimeoutSeconds)
	if isDup(err) {
		return ErrVersionExists
	}
	if err != nil {
		return err
	}
	return s.fillVersion(ctx, v)
}

func (s *mysqlStore) PublishVersion(ctx context.Context, skillID string, version int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var status string
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM skill_versions WHERE skill_id = ? AND version = ? FOR UPDATE`,
		skillID, version).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrVersionNotFound
	}
	if err != nil {
		return err
	}
	if status == StatusPublished {
		// Idempotent
		return tx.Commit()
	}
	if status == StatusDisabled {
		return ErrVersionNotDraft
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE skill_versions SET status = 'published', published_at = NOW() WHERE skill_id = ? AND version = ?`,
		skillID, version); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE skills SET current_version = ?, status = IF(status='disabled','disabled','published'), updated_at = NOW() WHERE skill_id = ? AND is_deleted = 0`,
		version, skillID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *mysqlStore) GetVersion(ctx context.Context, skillID string, version int) (*SkillVersion, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+skillVersionCols+` FROM skill_versions WHERE skill_id = ? AND version = ?`,
		skillID, version)
	v, err := scanVersion(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrVersionNotFound
	}
	return v, err
}

func (s *mysqlStore) ListVersions(ctx context.Context, skillID string) ([]*SkillVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+skillVersionCols+` FROM skill_versions WHERE skill_id = ? ORDER BY version DESC`,
		skillID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]*SkillVersion, 0, 4)
	for rows.Next() {
		v, err := scanVersion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *mysqlStore) fillVersion(ctx context.Context, v *SkillVersion) error {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+skillVersionCols+` FROM skill_versions WHERE skill_id = ? AND version = ?`,
		v.SkillID, v.Version)
	got, err := scanVersion(row)
	if err != nil {
		return err
	}
	*v = *got
	return nil
}

func scanVersion(s rowScanner) (*SkillVersion, error) {
	var (
		v        SkillVersion
		prompt   sql.NullString
		executor string
		timeout  int
		status   string
		pubAt    sql.NullTime
	)
	if err := s.Scan(&v.SkillID, &v.Version, &v.ContentMD, &v.Checksum, &prompt, &executor, &timeout, &status, &pubAt); err != nil {
		return nil, err
	}
	if prompt.Valid {
		v.PromptTemplate = prompt.String
	}
	v.ExecutorType = executor
	v.TimeoutSeconds = timeout
	v.Status = status
	if pubAt.Valid {
		v.PublishedAt = pubAt.Time
	}
	return &v, nil
}

// ----- Agent binding -----

func (s *mysqlStore) BindAgentSkill(ctx context.Context, agentID, skillID string, version, sortOrder int) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO agent_skills (agent_id, skill_id, version, sort_order)
		 VALUES (?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE version = VALUES(version), sort_order = VALUES(sort_order)`,
		agentID, skillID, version, sortOrder)
	return err
}

func (s *mysqlStore) UnbindAgentSkill(ctx context.Context, agentID, skillID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM agent_skills WHERE agent_id = ? AND skill_id = ?`, agentID, skillID)
	return err
}

func (s *mysqlStore) ListAgentSkills(ctx context.Context, agentID string) ([]*AgentSkill, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+agentSkillCols+` FROM agent_skills WHERE agent_id = ? ORDER BY sort_order`,
		agentID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]*AgentSkill, 0, 4)
	for rows.Next() {
		var b AgentSkill
		if err := rows.Scan(&b.AgentID, &b.SkillID, &b.Version, &b.SortOrder, &b.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &b)
	}
	return out, rows.Err()
}

// newUUID placeholder removed: Manager.Create assigns a uuid before calling
// the store, so mysqlStore always receives a non-empty SkillID.
