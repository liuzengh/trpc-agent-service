package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

const maxSpoolEvent = 256 << 10

var auditID = regexp.MustCompile(`^audit-[A-Za-z0-9_-]{1,55}$`)

type spool struct {
	dir string
	max int
	mu  sync.Mutex
}
type spoolEntry struct {
	Format string `json:"format"`
	Event  Event  `json:"event"`
}

func newSpool(dir string, maxRecords int) (*spool, error) {
	if maxRecords <= 0 {
		return nil, errors.New("audit spool capacity must be positive")
	}
	clean, err := filepath.Abs(dir)
	if err != nil || clean == string(filepath.Separator) {
		return nil, errors.New("invalid audit spool directory")
	}
	if err := os.MkdirAll(clean, 0700); err != nil {
		return nil, errors.New("cannot create audit spool")
	}
	info, err := os.Lstat(clean)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("audit spool must be a private, non-symlink directory")
	}
	return &spool{dir: clean, max: maxRecords}, nil
}
func (s *spool) append(event Event) error {
	if !auditID.MatchString(event.ID) {
		return errors.New("invalid buffered audit identity")
	}
	data, err := json.Marshal(spoolEntry{"trpc-audit-v1", event})
	if err != nil || len(data) > maxSpoolEvent {
		return errors.New("audit event cannot be buffered")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, event.ID+".json")
	if old, err := s.read(path); err == nil {
		encoded, _ := json.Marshal(spoolEntry{"trpc-audit-v1", old})
		if bytes.Equal(encoded, data) {
			return syncDirectory(s.dir)
		}
		return ErrEventConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("audit spool entry unavailable")
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return errors.New("audit spool unavailable")
	}
	if len(entries) >= s.max {
		return errors.New("audit spool capacity reached; operation refused")
	}
	file, err := os.CreateTemp(s.dir, ".audit-pending-")
	if err != nil {
		return errors.New("audit spool write unavailable")
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return errors.New("audit spool flush failed")
	}
	if err := os.Link(temporary, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			old, readErr := s.read(path)
			encoded, _ := json.Marshal(spoolEntry{"trpc-audit-v1", old})
			if readErr == nil && bytes.Equal(encoded, data) {
				return syncDirectory(s.dir)
			}
			return ErrEventConflict
		}
		return errors.New("audit spool publish failed")
	}
	return syncDirectory(s.dir)
}
func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return errors.New("audit spool directory unavailable")
	}
	defer f.Close()
	if f.Sync() != nil {
		return errors.New("audit spool directory flush failed")
	}
	return nil
}
func (s *spool) read(path string) (Event, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return Event{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > maxSpoolEvent {
		return Event{}, errors.New("unsafe audit spool entry")
	}
	file, err := os.Open(path)
	if err != nil {
		return Event{}, err
	}
	defer file.Close()
	var item spoolEntry
	decoder := json.NewDecoder(io.LimitReader(file, maxSpoolEvent+1))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&item) != nil || decoder.Decode(new(any)) != io.EOF || item.Format != "trpc-audit-v1" || !auditID.MatchString(item.Event.ID) || filepath.Base(path) != item.Event.ID+".json" {
		return Event{}, errors.New("invalid audit spool entry")
	}
	return item.Event, nil
}
func (s *spool) flush(ctx context.Context, writer Writer, limit int) error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return errors.New("audit spool unavailable")
	}
	processed := 0
	for _, entry := range entries {
		if processed >= limit {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || !auditID.MatchString(strings.TrimSuffix(entry.Name(), ".json")) {
			continue
		}
		path := filepath.Join(s.dir, entry.Name())
		event, err := s.read(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return errors.New("audit spool requires repair")
		}
		// Stable audit ID makes commit-before-response-loss and concurrent
		// flushes idempotent. No filesystem lock is held across network I/O.
		if writer.Record(ctx, event) != nil {
			return errors.New("audit spool replay unavailable")
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return errors.New("audit spool acknowledgement failed")
		}
		processed++
	}
	if processed > 0 {
		return syncDirectory(s.dir)
	}
	return nil
}
