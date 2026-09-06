package main

import "testing"

func TestConnectedTrace(t *testing.T) {
	valid := []span{{id: "root", traceID: "trace"}, {id: "child", parent: "root", traceID: "trace"}}
	if err := connected(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range [][]span{
		{{id: "one", traceID: "a"}, {id: "two", traceID: "b"}},
		{{id: "one", traceID: "a"}, {id: "two", traceID: "a"}},
		{{id: "one", parent: "missing", traceID: "a"}},
		{{id: "one", parent: "two", traceID: "a"}, {id: "two", parent: "one", traceID: "a"}},
	} {
		if err := connected(invalid); err == nil {
			t.Fatalf("accepted disconnected trace: %+v", invalid)
		}
	}
}
