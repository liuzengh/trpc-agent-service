package config

import (
	"fmt"
	"strings"
)

const (
	RoleAll     = "all"
	RoleGateway = "gateway"
	RoleRelay   = "relay"
	RoleWorker  = "worker"
	RoleSender  = "sender"
	RoleAdmin   = "admin"
)

// Roles controls which long-running components start in this process.
type Roles struct {
	Gateway bool
	Relay   bool
	Worker  bool
	Sender  bool
	Admin   bool
}

func ParseRole(value string) (Roles, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", RoleAll:
		return Roles{Gateway: true, Relay: true, Worker: true, Sender: true, Admin: true}, nil
	case RoleGateway:
		return Roles{Gateway: true}, nil
	case RoleRelay:
		return Roles{Relay: true}, nil
	case RoleWorker:
		return Roles{Worker: true}, nil
	case RoleSender:
		return Roles{Sender: true}, nil
	case RoleAdmin:
		return Roles{Admin: true}, nil
	default:
		return Roles{}, fmt.Errorf("unsupported service role %q", value)
	}
}
