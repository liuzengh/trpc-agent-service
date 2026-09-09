package main

import "testing"

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantQueue   string
		wantRestart bool
		wantErr     bool
	}{
		{name: "inbox", args: []string{"inbox", "10", "operator", "retry"}, wantQueue: "inbox"},
		{name: "outbox resume", args: []string{"outbox", "11", "operator", "provider recovered"}, wantQueue: "outbox"},
		{name: "outbox restart", args: []string{"outbox", "12", "operator", "operator accepts duplicates", "--restart"}, wantQueue: "outbox", wantRestart: true},
		{name: "restart inbox rejected", args: []string{"inbox", "13", "operator", "retry", "--restart"}, wantErr: true},
		{name: "unknown flag rejected", args: []string{"outbox", "14", "operator", "retry", "--force"}, wantErr: true},
		{name: "invalid id rejected", args: []string{"outbox", "0", "operator", "retry"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue, _, _, _, restart, err := parseArgs(tt.args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseArgs(%v) error = %v, wantErr %v", tt.args, err, tt.wantErr)
			}
			if err == nil && (queue != tt.wantQueue || restart != tt.wantRestart) {
				t.Fatalf("parseArgs(%v) = queue=%q restart=%v", tt.args, queue, restart)
			}
		})
	}
}

func TestReplayMode(t *testing.T) {
	tests := []struct {
		queue   string
		restart bool
		want    string
	}{
		{queue: "inbox", want: "restart"},
		{queue: "outbox", want: "resume"},
		{queue: "outbox", restart: true, want: "restart"},
	}
	for _, test := range tests {
		if got := string(replayMode(test.queue, test.restart)); got != test.want {
			t.Fatalf("replayMode(%q, %v) = %q, want %q", test.queue, test.restart, got, test.want)
		}
	}
}
