package wecombot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type spoolRecord struct {
	Key     string    `json:"key"`
	Hash    string    `json:"hash"`
	Created time.Time `json:"created"`
	Payload []byte    `json:"payload,omitempty"`
	Status  string    `json:"status"`
}
type spool struct {
	mu      sync.Mutex
	dir     string
	lock    *os.File
	records map[string]spoolRecord
}

// Each record is atomically persisted before a network side effect. Payloads
// disappear on admission; bounded acknowledgement metadata prevents rewrapping
// duplicate callbacks with a new receipt timestamp and therefore a new hash.
func openSpool(dir, binding string) (*spool, error) {
	if os.MkdirAll(dir, 0700) != nil {
		return nil, ErrSpool
	}
	lock, err := acquireStateLock(filepath.Join(dir, "owner"))
	if err != nil {
		return nil, err
	}
	s := &spool{dir: dir, lock: lock, records: map[string]spoolRecord{}}
	ok := false
	defer func() {
		if !ok {
			lock.Close()
		}
	}()
	path := filepath.Join(dir, "binding")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		if atomicWrite(path, []byte(binding)) != nil {
			return nil, ErrSpool
		}
	} else if err != nil || string(data) != binding {
		return nil, ErrSpool
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, ErrSpool
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		f, err := os.Open(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, ErrSpool
		}
		raw, err := io.ReadAll(io.LimitReader(f, 2*maxFrameBytes+4097))
		f.Close()
		var r spoolRecord
		if err != nil || len(raw) > 2*maxFrameBytes+4096 || json.Unmarshal(raw, &r) != nil || r.Key+".json" != entry.Name() || len(r.Hash) != 64 || r.Created.IsZero() {
			return nil, ErrSpool
		}
		if r.Status != "pending" && r.Status != "admitted" && r.Status != "started" && r.Status != "sent" && r.Status != "rejected" && r.Status != "not_sent" {
			return nil, ErrSpool
		}
		if r.Status == "pending" && (!json.Valid(r.Payload) || len(r.Payload) > maxFrameBytes+1024) {
			return nil, ErrSpool
		}
		if r.Status != "pending" && len(r.Payload) != 0 {
			return nil, ErrSpool
		}
		s.records[r.Key] = r
		if len(s.records) > 10000 {
			return nil, ErrSpoolFull
		}
	}
	ok = true
	return s, nil
}
func (s *spool) Close() error { return s.lock.Close() }
func spoolKey(kind, id string) string {
	h := sha256.Sum256([]byte(id))
	return kind + "-" + hex.EncodeToString(h[:])
}
func dataHash(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func atomicWrite(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".spool-*")
	if err != nil {
		return ErrSpool
	}
	temp := f.Name()
	defer os.Remove(temp)
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return ErrSpool
	}
	if f.Sync() != nil || f.Close() != nil || replaceCheckpoint(temp, path) != nil {
		return ErrSpool
	}
	return nil
}
func (s *spool) save(r spoolRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return ErrSpool
	}
	if err = atomicWrite(filepath.Join(s.dir, r.Key+".json"), data); err != nil {
		return err
	}
	s.records[r.Key] = r
	return nil
}
func (s *spool) prune(now time.Time) error {
	for key, r := range s.records {
		// Unknown/started dispatches are never removed automatically.
		if (r.Status == "admitted" || r.Status == "sent" || r.Status == "rejected" || r.Status == "not_sent") && now.Sub(r.Created) > 25*time.Hour {
			if os.Remove(filepath.Join(s.dir, key+".json")) != nil {
				return ErrSpool
			}
			delete(s.records, key)
		}
	}
	if len(s.records) >= 10000 {
		return ErrSpoolFull
	}
	return nil
}
func (s *spool) enqueue(id string, raw []byte, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var input frame
	if json.Unmarshal(raw, &input) != nil {
		return ErrProtocol
	}
	var normalized any
	if json.Unmarshal(input.Body, &normalized) != nil {
		return ErrProtocol
	}
	canonical, _ := json.Marshal(normalized)
	key, hash := spoolKey("in", id), dataHash(canonical)
	if previous, ok := s.records[key]; ok {
		if previous.Hash != hash {
			return ErrProtocol
		}
		return nil
	}
	if err := s.prune(now); err != nil {
		return err
	}
	pending := 0
	for _, r := range s.records {
		if r.Status == "pending" {
			pending++
		}
	}
	if pending >= 256 {
		return ErrSpoolFull
	}
	data, err := json.Marshal(struct {
		ReceivedAt time.Time       `json:"received_at"`
		Frame      json.RawMessage `json:"frame"`
	}{now, raw})
	if err != nil {
		return ErrSpool
	}
	return s.save(spoolRecord{Key: key, Hash: hash, Created: now, Payload: data, Status: "pending"})
}
func (s *spool) next() *spoolRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var found *spoolRecord
	keys := make([]string, 0, len(s.records))
	for k := range s.records {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		r := s.records[key]
		if r.Status == "pending" && (found == nil || r.Created.Before(found.Created)) {
			copy := r
			found = &copy
		}
	}
	return found
}
func (s *spool) admit(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[key]
	if !ok {
		return ErrSpool
	}
	r.Payload = nil
	r.Status = "admitted"
	return s.save(r)
}
func (s *spool) beginDelivery(id, hash string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := spoolKey("out", id)
	if r, ok := s.records[key]; ok {
		if r.Hash != hash {
			return "conflict", nil
		}
		if r.Status != "not_sent" {
			return r.Status, nil
		}
	}
	if err := s.prune(time.Now()); err != nil {
		return "", err
	}
	if err := s.save(spoolRecord{Key: key, Hash: hash, Created: time.Now().UTC(), Status: "started"}); err != nil {
		return "", err
	}
	return "new", nil
}
func (s *spool) finishDelivery(id, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[spoolKey("out", id)]
	if !ok {
		return ErrSpool
	}
	r.Status = status
	return s.save(r)
}
func (c *Connector) forwardLoop(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		record := c.spool.next()
		if record == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.wake:
			case <-time.After(time.Second):
			}
			continue
		}
		for attempt := 0; ; attempt++ {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.gatewayURL, bytes.NewReader(record.Payload))
			if err != nil {
				return ErrConfiguration
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(bridgeHeader, c.config.BridgeToken)
			resp, err := c.client.Do(req)
			status := 0
			if resp != nil {
				status = resp.StatusCode
				if resp.Body != nil {
					io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
					resp.Body.Close()
				}
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil && status == 200 {
				if err = c.spool.admit(record.Key); err != nil {
					return err
				}
				c.emit("gateway", "acknowledged", 200)
				break
			}
			if err == nil && status < 500 && status != 429 {
				return ErrGateway
			}
			c.emit("gateway", "retry", status)
			delay := time.Second << min(attempt, 5)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}

func openLockFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, ErrSpool
	}
	return file, nil
}
