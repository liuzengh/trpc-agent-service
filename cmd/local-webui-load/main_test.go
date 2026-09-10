package main

import (
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	valid := config{baseURL: "http://127.0.0.1:58086", routeKey: "local-webui", accountID: "local-webui",
		token: defaultToken, requests: 4, concurrency: 2, timeout: time.Minute, prompt: "hello"}
	if err := validate(valid); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	valid.concurrency = 5
	if err := validate(valid); err == nil {
		t.Fatal("concurrency above request count accepted")
	}
}
