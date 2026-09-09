package openclaw

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestServerAppliesProductionTimeoutDefaults(t *testing.T) {
	component := &Server{Address: "127.0.0.1:0", Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}
	if err := component.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = component.Close(context.Background()) })
	if component.server.ReadHeaderTimeout != 10*time.Second || component.server.ReadTimeout != 30*time.Second || component.server.WriteTimeout != 0 || component.server.IdleTimeout != 120*time.Second || component.server.MaxHeaderBytes != 1<<20 {
		t.Fatalf("unexpected server limits: %+v", component.server)
	}
}
