package console

import (
	"context"
	"encoding/json"
	"time"
)

type Observation struct {
	Component  string    `json:"component"`
	TenantID   string    `json:"tenant_id,omitempty"`
	BindingID  string    `json:"binding_id,omitempty"`
	ConfigHash string    `json:"config_hash,omitempty"`
	State      string    `json:"state"`
	ObservedAt time.Time `json:"observed_at"`
}
type WorkerView struct {
	ID        string        `json:"worker_id"`
	UpdatedAt time.Time     `json:"updated_at"`
	Checks    []Observation `json:"checks"`
}

func (s *Store) WorkerObservations(ctx context.Context, tenant string) ([]WorkerView, error) {
	records, err := s.List(ctx, Filter{Kind: "worker", AllTenants: true, Limit: 100})
	if err != nil {
		return nil, err
	}
	items := []WorkerView{}
	for _, record := range records {
		age := time.Since(record.UpdatedAt)
		if age < 0 || age > 30*time.Second {
			continue
		}
		var data struct {
			Checks []Observation `json:"checks"`
		}
		if json.Unmarshal(record.Data, &data) != nil {
			continue
		}
		v := WorkerView{ID: record.ID, UpdatedAt: record.UpdatedAt, Checks: []Observation{}}
		for _, check := range data.Checks {
			if check.TenantID != "" && check.TenantID != tenant {
				continue
			}
			age := time.Since(check.ObservedAt)
			if age < 0 || age > 30*time.Second {
				check.State = "unknown"
			}
			v.Checks = append(v.Checks, check)
		}
		items = append(items, v)
	}
	return items, nil
}
func (e *Engine) observe(ctx context.Context) {
	if e.Probe == nil {
		return
	}
	limited, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	checks := e.Probe(limited)
	if len(checks) > 40 {
		checks = checks[:40]
	}
	e.probeMu.Lock()
	e.observations = checks
	e.probeMu.Unlock()
}
