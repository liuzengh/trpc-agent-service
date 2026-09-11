package deploymentv1

import (
	"encoding/json"
	"regexp"
)

const WorkspaceAdapterVersion = "sdk-sandbox-v1"

type ManifestExecutorResource struct {
	Kind           string `json:"kind"`
	AdapterVersion string `json:"adapter_version"`
}
type ManifestWorkspace struct {
	ExecutorResource string   `json:"executor_resource"`
	Tools            []string `json:"tools"`
}

func (w ManifestWorkspace) Validate() error {
	if !regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`).MatchString(w.ExecutorResource) || len(w.Tools) < 1 || len(w.Tools) > 2 {
		return ErrInvalidManifest
	}
	seen := map[string]bool{}
	for _, t := range w.Tools {
		if seen[t] || (t != "workspace_exec" && t != "workspace_save_artifact") {
			return ErrInvalidManifest
		}
		seen[t] = true
	}
	return nil
}
func (w *ManifestWorkspace) UnmarshalJSON(b []byte) error {
	type wire ManifestWorkspace
	var v wire
	if e := strictDecodeJSON(b, &v); e != nil {
		return e
	}
	out := ManifestWorkspace(v)
	if e := out.Validate(); e != nil {
		return e
	}
	*w = out
	return nil
}
func (r ManifestExecutorResource) Validate() error {
	if r.Kind != "sdk_sandbox" || r.AdapterVersion != WorkspaceAdapterVersion {
		return ErrInvalidManifest
	}
	return nil
}
func (r *ManifestExecutorResource) UnmarshalJSON(b []byte) error {
	type wire ManifestExecutorResource
	var v wire
	if e := strictDecodeJSON(b, &v); e != nil {
		return e
	}
	out := ManifestExecutorResource(v)
	if e := out.Validate(); e != nil {
		return e
	}
	*r = out
	return nil
}

var _ json.Unmarshaler = (*ManifestWorkspace)(nil)
