//go:build !linux

package localrun

import (
	"context"
	"errors"
)

var ErrNotRunning = errors.New("service is not running")
var ErrIdentity = errors.New("safe process management requires Linux pidfd support")

type Process struct{ PID int }
type Manager struct{ Root, Executable string }

func (Manager) Running() (Process, error)                { return Process{}, ErrIdentity }
func (Manager) Record(int) error                         { return ErrIdentity }
func (Manager) Stop(context.Context) error               { return ErrIdentity }
func (Manager) RecordStarted(context.Context, int) error { return ErrIdentity }
