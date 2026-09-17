package main

import (
	"errors"
	"syscall"
	"testing"
	"time"
)

func TestRunPreStopSignalsContainerInitAndSettles(t *testing.T) {
	var gotPID int
	var gotSignal syscall.Signal
	var gotDelay time.Duration
	err := runPreStop(mapEnvironment(map[string]string{
		preStopTargetPIDEnvironment: "1",
		preStopSettleEnvironment:    "750ms",
	}), func(pid int, signal syscall.Signal) error {
		gotPID, gotSignal = pid, signal
		return nil
	}, func(delay time.Duration) { gotDelay = delay })
	if err != nil {
		t.Fatal(err)
	}
	if gotPID != 1 || gotSignal != syscall.SIGTERM || gotDelay != 750*time.Millisecond {
		t.Fatalf("unexpected hook result pid=%d signal=%v delay=%s", gotPID, gotSignal, gotDelay)
	}
}

func TestRunPreStopFailsClosed(t *testing.T) {
	tests := []map[string]string{
		nil,
		{preStopTargetPIDEnvironment: "2"},
		{preStopTargetPIDEnvironment: "1", preStopSettleEnvironment: "99ms"},
		{preStopTargetPIDEnvironment: "1", preStopSettleEnvironment: "11s"},
	}
	for _, environment := range tests {
		called := false
		err := runPreStop(mapEnvironment(environment), func(int, syscall.Signal) error {
			called = true
			return nil
		}, func(time.Duration) {})
		if err == nil || called {
			t.Fatalf("expected configuration to fail closed: environment=%v err=%v", environment, err)
		}
	}

	err := runPreStop(mapEnvironment(map[string]string{preStopTargetPIDEnvironment: "1"}), func(int, syscall.Signal) error {
		return errors.New("denied")
	}, func(time.Duration) { t.Fatal("must not settle after a failed signal") })
	if err == nil {
		t.Fatal("expected signal failure")
	}
}
