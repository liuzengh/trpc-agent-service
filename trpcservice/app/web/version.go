// Platform build identity.
//
// A blue/green or canary rollout needs one objective answer to "which build
// served this request?": the ingress weight, the dashboard and the rollback
// decision are all guesses until the serving process can name itself. This
// endpoint is that answer, and it is deliberately the only thing it exposes —
// no configuration, no paths, no dependency versions, because it has to be
// reachable without a token (the ingress probes it, and an operator pastes the
// URL into a browser during an incident).
package web

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime/debug"
	"time"
)

// BuildInfo is the identity of a running platform process.
type BuildInfo struct {
	// Version is the release label of this build (TRPC_BUILD_VERSION, or the
	// VCS revision when the binary was built from a checkout).
	Version string
	// GitSHA is the source revision the binary was built from, when known.
	GitSHA string
	// BuildTime is the commit time of that revision, when known.
	BuildTime string
	// Role is the node role this process started with (all/admin/worker/gateway),
	// so a mixed-role deployment can be told apart in one request.
	Role string
}

// versionResponse is what GET /version returns.
type versionResponse struct {
	Version   string `json:"version"`
	GitSHA    string `json:"git_sha,omitempty"`
	BuildTime string `json:"build_time,omitempty"`
	Role      string `json:"role"`
	// Instance identifies the process, so a canary check can tell "routed to
	// the new build" from "routed to another replica of the old one".
	Instance string `json:"instance"`
	// StartedAt is when this process came up (seconds since epoch), which makes
	// "the canary node restarted" visible during a rollout.
	StartedAt int64 `json:"started_at"`
}

// VersionHandler serves GET /version for the given build identity.
//
// The environment is read once at construction: a rollout label never changes
// while the process lives, and reading it per request would let a config drift
// make two concurrent probes disagree.
func VersionHandler(info BuildInfo) http.HandlerFunc {
	if info.Version == "" {
		info.Version = resolveVersion()
	}
	if info.GitSHA == "" || info.BuildTime == "" {
		sha, at := vcsInfo()
		if info.GitSHA == "" {
			info.GitSHA = sha
		}
		if info.BuildTime == "" {
			info.BuildTime = at
		}
	}
	instance, _ := os.Hostname()
	if id := os.Getenv("TRPC_INSTANCE_ID"); id != "" {
		instance = id
	}
	started := time.Now().Unix()

	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		// Never cached: a stale answer would defeat the whole point during a
		// rollout.
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(versionResponse{
			Version:   info.Version,
			GitSHA:    info.GitSHA,
			BuildTime: info.BuildTime,
			Role:      info.Role,
			Instance:  instance,
			StartedAt: started,
		})
	}
}

// resolveVersion prefers the deployment's own release label and falls back to
// the VCS revision baked in at build time, then to "dev".
func resolveVersion() string {
	if v := os.Getenv("TRPC_BUILD_VERSION"); v != "" {
		return v
	}
	if sha, _ := vcsInfo(); sha != "" {
		if len(sha) > 12 {
			sha = sha[:12]
		}
		return "rev-" + sha
	}
	return "dev"
}

// vcsInfo returns the VCS revision and commit time stamped into the binary by
// the Go toolchain (available when the build ran inside a checkout).
func vcsInfo() (sha, commitTime string) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "", ""
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			sha = s.Value
		case "vcs.time":
			commitTime = s.Value
		}
	}
	return sha, commitTime
}
