package main

import (
	"errors"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	preStopTargetPIDEnvironment = "TRPC_PRESTOP_TARGET_PID"
	preStopSettleEnvironment    = "TRPC_PRESTOP_SETTLE_DELAY"
)

// runPreStop is the shell-free Kubernetes preStop hook. It signals the main
// process so the role enters its existing lifecycle drain before kubelet sends
// the final SIGTERM, then leaves a short window for /readyz to turn unready.
func runPreStop(getenv func(string) string, signalProcess func(int, syscall.Signal) error, sleep func(time.Duration)) error {
	if getenv == nil || signalProcess == nil || sleep == nil {
		return errors.New("invalid prestop dependencies")
	}
	pid, err := strconv.Atoi(strings.TrimSpace(getenv(preStopTargetPIDEnvironment)))
	if err != nil || pid != 1 {
		return errors.New("TRPC_PRESTOP_TARGET_PID must explicitly select container pid 1")
	}
	delay := 2 * time.Second
	if raw := strings.TrimSpace(getenv(preStopSettleEnvironment)); raw != "" {
		delay, err = time.ParseDuration(raw)
		if err != nil || delay < 100*time.Millisecond || delay > 10*time.Second {
			return errors.New("invalid TRPC_PRESTOP_SETTLE_DELAY")
		}
	}
	if err := signalProcess(pid, syscall.SIGTERM); err != nil {
		return errors.New("failed to signal main process")
	}
	sleep(delay)
	return nil
}
