package inmemory

import (
	"context"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestListScopesFiltersAndPaginatesBindings(t *testing.T) {
	clock := &testClock{now: time.Now().UTC()}
	repository := NewRepository(Options{Clock: clock.Now})
	digest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "list-route")
	if err != nil {
		t.Fatal(err)
	}
	first := mustCreate(t, repository, bindingInput("t_00000000000000000000000000", "primary", "corp-one", digest))
	second := mustCreate(t, repository, bindingInput(first.TenantID, "secondary", "corp-two", digest))
	_ = mustCreate(t, repository, bindingInput("t_00000000000000000000000001", "primary", "corp-three", digest))

	page, next, err := repository.List(context.Background(), first.TenantID, "", "", "", 1)
	if err != nil || len(page) != 1 || next == "" {
		t.Fatalf("first binding page = items=%+v next=%q err=%v", page, next, err)
	}
	page, next, err = repository.List(context.Background(), first.TenantID, "", "", next, 1)
	if err != nil || len(page) != 1 || next != "" {
		t.Fatalf("second binding page = items=%+v next=%q err=%v", page, next, err)
	}
	filtered, _, err := repository.List(context.Background(), first.TenantID, "corp-two", "", "", 50)
	if err != nil || len(filtered) != 1 || filtered[0].BindingID != second.BindingID || filtered[0].BindingID == first.BindingID {
		t.Fatalf("filtered binding list = items=%+v err=%v", filtered, err)
	}
}
