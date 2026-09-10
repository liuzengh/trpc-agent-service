package main

import "testing"

func TestServiceRoleFromEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    serviceRole
		wantErr bool
	}{
		{name: "default", want: roleAll},
		{name: "all", raw: "all", want: roleAll},
		{name: "gateway", raw: " gateway ", want: roleGateway},
		{name: "channel", raw: " CHANNEL ", want: roleChannel},
		{name: "worker", raw: "WORKER", want: roleWorker},
		{name: "invalid", raw: "scheduler", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := serviceRoleFromEnvironment(mapEnvironment(map[string]string{"SERVICE_ROLE": test.raw}))
			if test.wantErr {
				if err == nil {
					t.Fatal("serviceRoleFromEnvironment() error = nil")
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("serviceRoleFromEnvironment() = %q, %v; want %q, nil", got, err, test.want)
			}
		})
	}
}

func TestServiceRoleCapabilities(t *testing.T) {
	t.Parallel()

	if !roleAll.runsGateway() || !roleAll.runsChannel() || !roleAll.runsWorker() {
		t.Fatal("all role must run gateway, channel and worker")
	}
	if !roleGateway.runsGateway() || roleGateway.runsChannel() || roleGateway.runsWorker() {
		t.Fatal("gateway role capability mismatch")
	}
	if roleChannel.runsGateway() || !roleChannel.runsChannel() || roleChannel.runsWorker() {
		t.Fatal("channel role capability mismatch")
	}
	if roleWorker.runsGateway() || roleWorker.runsChannel() || !roleWorker.runsWorker() {
		t.Fatal("worker role capability mismatch")
	}
}
