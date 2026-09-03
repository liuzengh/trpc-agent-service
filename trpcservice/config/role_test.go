package config

import "testing"

func TestParseRole(t *testing.T) {
	all, err := ParseRole("all")
	if err != nil || !all.Gateway || !all.Relay || !all.Worker || !all.Sender {
		t.Fatalf("all roles = %+v, err = %v", all, err)
	}
	worker, err := ParseRole("worker")
	if err != nil || worker.Gateway || !worker.Worker || worker.Sender {
		t.Fatalf("worker role = %+v, err = %v", worker, err)
	}
	if _, err := ParseRole("unknown"); err == nil {
		t.Fatal("expected unknown role error")
	}
}
