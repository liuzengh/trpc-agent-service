package messaging

// OutboundArtifactRef points at one immutable framework Artifact version. It
// intentionally carries neither bytes nor a Worker-local path; the Gateway
// materializes it immediately before the provider send.
type OutboundArtifactRef struct {
	Filename  string `json:"filename"`
	Version   int    `json:"version"`
	Name      string `json:"name,omitempty"`
	MimeType  string `json:"mime_type,omitempty"`
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
}
