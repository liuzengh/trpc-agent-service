package redistopology

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	miniserver "github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

func TestVerifyPrimaryStandalone(t *testing.T) {
	server := miniredis.RunT(t)
	mockTopology(server, "master", "run-a", false)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	got, err := VerifyPrimaryStandalone(context.Background(), client)
	if err != nil || got != "run-a" {
		t.Fatalf("VerifyPrimaryStandalone()=(%q,%v)", got, err)
	}
}

func TestVerifyPrimaryStandaloneFailsClosed(t *testing.T) {
	tests := []struct {
		name           string
		role           string
		runID          string
		cluster        bool
		unknownCluster bool
		want           string
	}{
		{name: "replica", role: "slave", runID: "run-a", want: "primary Redis"},
		{name: "missing run id", role: "master", want: "run_id"},
		{name: "cluster", role: "master", runID: "run-a", cluster: true, want: "rejects Redis Cluster"},
		{name: "unknown cluster command", role: "master", runID: "run-a", unknownCluster: true, want: "topology check failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			if tt.unknownCluster {
				mockTopologyWithoutCluster(server, tt.role, tt.runID)
			} else {
				mockTopology(server, tt.role, tt.runID, tt.cluster)
			}
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			defer client.Close()
			_, err := VerifyPrimaryStandalone(context.Background(), client)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("VerifyPrimaryStandalone() error=%v, want %q", err, tt.want)
			}
		})
	}
}

func mockTopology(server *miniredis.Miniredis, role, runID string, cluster bool) {
	server.Server().SetPreHook(func(peer *miniserver.Peer, command string, args ...string) bool {
		switch command {
		case "ROLE":
			peer.WriteLen(3)
			peer.WriteBulk(role)
			peer.WriteInt(0)
			peer.WriteLen(0)
			return true
		case "INFO":
			peer.WriteBulk("# Server\r\nrun_id:" + runID + "\r\n")
			return true
		case "CLUSTER":
			if cluster {
				peer.WriteBulk("cluster_enabled:1\r\n")
			} else {
				peer.WriteError("ERR This instance has cluster support disabled")
			}
			return true
		default:
			return false
		}
	})
}

func mockTopologyWithoutCluster(server *miniredis.Miniredis, role, runID string) {
	server.Server().SetPreHook(func(peer *miniserver.Peer, command string, args ...string) bool {
		switch command {
		case "ROLE":
			peer.WriteLen(3)
			peer.WriteBulk(role)
			peer.WriteInt(0)
			peer.WriteLen(0)
			return true
		case "INFO":
			peer.WriteBulk("# Server\r\nrun_id:" + runID + "\r\n")
			return true
		default:
			return false
		}
	})
}
