// Package trpcservice carries build metadata for the trpc-agent-service
// platform binary.
package trpcservice

// Version is the semantic version of the service. Release builds override it
// via -ldflags "-X github.com/cyl6/trpc-agent-service/trpcservice.Version=...".
var Version = "0.1.0"

// GitCommit is the source revision the binary was built from; "unknown" for
// local development builds.
var GitCommit = "unknown"
