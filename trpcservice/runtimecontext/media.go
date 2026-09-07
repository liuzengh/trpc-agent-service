package runtimecontext

type MediaReference struct {
	FileID         string `json:"file_id"`
	Name           string `json:"name,omitempty"`
	DeclaredMIME   string `json:"declared_mime,omitempty"`
	Size           int64  `json:"size,omitempty"`
	BindingVersion int64  `json:"binding_version"`
}
