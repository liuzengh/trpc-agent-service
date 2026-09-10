package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunRejectsUnknownReportFields(t *testing.T) {
	var output bytes.Buffer
	err := run(nil, strings.NewReader(`{"schema_version":1,"unknown":true}`), &output)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("err=%v", err)
	}
}
