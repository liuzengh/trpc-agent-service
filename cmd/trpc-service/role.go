// Role plan: which node capabilities a `-role` value enables.
//
// The platform runs as one binary in several node shapes. A node's role decides
// three things, and they must agree — a role that registers an API whose loop it
// never runs (or vice versa) is the bug this table exists to prevent:
//
//   - admin API:   the management REST surface (tenants, members, agents,
//     endpoints, tools, knowledge, skills, channels, secrets, audit, usage)
//   - chat API:    the conversation surface (/chat, /chat/stream) plus the
//     business ledger read side (/sessions, /sessions/{id}/messages)
//   - worker loop: consume inbound messages, run agents, drain the outbox
//   - IM gateway:  hold the IM connections and answer outbound replies back to
//     the platform's chat channels
//
// The chat surface rides the worker: POST /chat publishes an inbound message and
// streams the outbound reply, so a node without the worker loop would accept
// conversations nobody consumes. The DLQ is operator-facing but only meaningful
// where the worker runs.
package main

import "fmt"

// Role names accepted by the -role flag.
const (
	roleAll     = "all"
	roleAdmin   = "admin"
	roleWorker  = "worker"
	roleGateway = "gateway"
)

// rolePlan is the resolved capability set for one role.
type rolePlan struct {
	AdminAPI   bool
	ChatAPI    bool
	WorkerLoop bool
	IMGatway   bool
}

// planFor resolves a role name. An unknown role is an error rather than a silent
// fallback: a typo must not quietly produce a node with no capabilities.
func planFor(role string) (rolePlan, error) {
	switch role {
	case roleAll:
		return rolePlan{AdminAPI: true, ChatAPI: true, WorkerLoop: true, IMGatway: true}, nil
	case roleAdmin:
		return rolePlan{AdminAPI: true}, nil
	case roleWorker:
		return rolePlan{ChatAPI: true, WorkerLoop: true}, nil
	case roleGateway:
		return rolePlan{IMGatway: true}, nil
	default:
		return rolePlan{}, fmt.Errorf("unknown role %q (want %s|%s|%s|%s)",
			role, roleAdmin, roleWorker, roleGateway, roleAll)
	}
}
