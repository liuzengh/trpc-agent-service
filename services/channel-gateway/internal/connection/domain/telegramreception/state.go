// Package telegramreception defines Telegram receiver ownership and cursor
// state, independently of SQL, HTTP and the account's routing target.
package telegramreception

import (
	"errors"
	"time"
)

var (
	ErrOwnership = errors.New("telegram reception: ownership unavailable")
	ErrCursor    = errors.New("telegram reception: cursor conflict")
	ErrConflict  = errors.New("telegram reception: unmanaged webhook")
)

const (
	LeaseDuration    = 30 * time.Second
	CallBudget       = 8 * time.Second
	QuiescenceMargin = 2 * time.Second
	// The provider may randomize update IDs after one week without updates.
	// Probe from offset zero before that boundary; zero confirms nothing.
	IdleRecoveryAfter = 6 * 24 * time.Hour
)

type Lease struct {
	BotID, ScopeID, AccountID, InstanceID, InstanceEpoch, SourceEpoch string
	Revision, Epoch, NextOffset                                       int64
	Until, LastUpdateAt                                               time.Time
	ManagedURL, PendingURL                                            string
}

func (l Lease) PollOffset(now time.Time) int64 {
	if l.NextOffset > 0 && !l.LastUpdateAt.IsZero() && now.Sub(l.LastUpdateAt) >= IdleRecoveryAfter {
		return 0
	}
	return l.NextOffset
}

type Call struct {
	ID       string
	Deadline time.Time
}

// ManagedRemote accepts an empty endpoint or the endpoint for which this
// receiver durably recorded a registration intent. It never authorizes an
// arbitrary pre-existing endpoint solely because the bot token is valid.
func ManagedRemote(actual, managed string) bool {
	return actual == "" || managed != "" && actual == managed
}
