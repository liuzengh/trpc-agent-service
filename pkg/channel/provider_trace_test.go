package channel

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"testing"
)

func TestProviderWriteTraceMayArriveAfterRequestReturns(t *testing.T) {
	done := make(chan struct{})
	releaseWrite := make(chan struct{})
	defer func() {
		close(releaseWrite)
		<-done
	}()
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(req.Context())
		if trace.GotConn != nil {
			trace.GotConn(httptrace.GotConnInfo{})
		}
		go func() {
			defer close(done)
			<-releaseWrite
			trace.WroteRequest(httptrace.WroteRequestInfo{Err: context.Canceled})
		}()
		return nil, context.Canceled
	})}
	request, err := http.NewRequest(http.MethodPost, "https://provider.example.test/send", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	_, dispatched, err := doProviderRequest(client, request)
	if err == nil || !DeliveryOutcomeUnknown(providerTransportFailure("test", dispatched)) {
		t.Fatalf("connected transport failure must remain unknown before delayed write hook: dispatched=%v err=%v", dispatched, err)
	}
}
