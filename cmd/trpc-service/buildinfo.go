// Build identity stamped into the binary at link time.
//
// The Dockerfile passes these through -ldflags so a released image can name
// itself without any runtime configuration:
//
//	go build -ldflags "-X main.buildVersion=$VERSION -X main.buildGitSHA=$SHA" ...
//
// Both are empty by default; web.VersionHandler then falls back to the VCS
// revision the toolchain stamps into the binary (available for a local build)
// and finally to "dev".
package main

var (
	buildVersion string
	buildGitSHA  string
)
