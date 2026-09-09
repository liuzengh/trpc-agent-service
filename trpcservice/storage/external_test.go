package storage

import "testing"

func TestPhysicalNamespaceIsStableAndTenantScoped(t *testing.T) {
	first := physicalNamespace("Team Docs", "tenant-a", "app")
	if first != physicalNamespace("Team Docs", "tenant-a", "app") {
		t.Fatal("namespace is not stable")
	}
	if first == physicalNamespace("Team Docs", "tenant-b", "app") {
		t.Fatal("different tenants share a physical namespace")
	}
	if len(first) > 49 {
		t.Fatalf("namespace too long: %q", first)
	}
}

func TestQdrantEndpointParsing(t *testing.T) {
	host, port, tls, err := qdrantEndpoint("grpcs://cluster.example:7443")
	if err != nil || host != "cluster.example" || port != 7443 || !tls {
		t.Fatalf("host=%q port=%d tls=%v err=%v", host, port, tls, err)
	}
	for _, invalid := range []string{"cluster.example:6334", "ftp://cluster.example", "grpc://cluster.example/path", "grpc://cluster.example:99999"} {
		if _, _, _, err := qdrantEndpoint(invalid); err == nil {
			t.Fatalf("endpoint %q accepted", invalid)
		}
	}
}
