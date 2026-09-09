package telegramingress

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

type pendingUpdate struct {
	ID int64 `json:"id"`
	// []byte is base64 encoded by encoding/json. A RawMessage would be
	// compacted when saving and change Gateway's payload hash on restart.
	Body []byte `json:"body"`
}

type checkpoint struct {
	Version   int            `json:"version"`
	BotID     int64          `json:"botId"`
	RouteHash string         `json:"routeHash"`
	Offset    int64          `json:"offset"`
	Pending   *pendingUpdate `json:"pending,omitempty"`
}

func loadCheckpoint(path string) (*checkpoint, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrStateRead
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 2*maxUpdateBytes+4097))
	if err != nil || len(raw) > 2*maxUpdateBytes+4096 {
		return nil, ErrStateRead
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var state checkpoint
	if decoder.Decode(&state) != nil || decoder.Decode(new(any)) != io.EOF || state.Version != 1 ||
		state.BotID <= 0 || len(state.RouteHash) != 64 || state.Offset < 0 {
		return nil, ErrStateRead
	}
	if state.Pending != nil {
		id, err := updateIdentity(state.Pending.Body)
		if err != nil || id != state.Pending.ID || id < state.Offset {
			return nil, ErrStateRead
		}
	}
	return &state, nil
}

func saveCheckpoint(path string, state *checkpoint) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return ErrStateWrite
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".telegram-state-*")
	if err != nil {
		return ErrStateWrite
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	defer file.Close()
	if _, err := file.Write(raw); err != nil {
		return ErrStateWrite
	}
	if file.Sync() != nil || file.Close() != nil {
		return ErrStateWrite
	}
	if replaceCheckpoint(temporary, path) != nil {
		return ErrStateWrite
	}
	return nil
}

func openLockFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, ErrStateWrite
	}
	file, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, ErrStateWrite
	}
	return file, nil
}
