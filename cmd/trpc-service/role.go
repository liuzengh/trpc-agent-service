package main

import (
	"fmt"
	"strings"
)

type serviceRole string

const (
	roleAll     serviceRole = "all"
	roleGateway serviceRole = "gateway"
	roleChannel serviceRole = "channel"
	roleWorker  serviceRole = "worker"
)

func serviceRoleFromEnvironment(getenv environment) (serviceRole, error) {
	raw := strings.ToLower(strings.TrimSpace(getenv("SERVICE_ROLE")))
	if raw == "" {
		return roleAll, nil
	}
	switch serviceRole(raw) {
	case roleAll, roleGateway, roleChannel, roleWorker:
		return serviceRole(raw), nil
	default:
		return "", fmt.Errorf("SERVICE_ROLE must be one of all, gateway, channel, worker; got %q", raw)
	}
}

func (r serviceRole) runsGateway() bool {
	return r == roleAll || r == roleGateway
}

func (r serviceRole) runsWorker() bool {
	return r == roleAll || r == roleWorker
}

func (r serviceRole) runsChannel() bool {
	return r == roleAll || r == roleChannel
}
