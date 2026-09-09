package sessiondir_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir/sessiondirtest"
)

// TestMemoryDirectoryConformance holds the in-memory reference to the same
// contract every other implementation runs. It lives in the external test
// package because the suite imports sessiondir, which an in-package test file
// could not do.
func TestMemoryDirectoryConformance(t *testing.T) {
	sessiondirtest.RunDirectorySuite(t, func(t *testing.T) sessiondir.Directory {
		return sessiondir.NewMemoryDirectory()
	})
}
