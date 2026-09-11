package domain

// Error contains only a stable classification and an owner-generated field path.
// It never embeds submitted credentials or a provider response.
type Error struct {
	Code  string
	Field string
}

func (e *Error) Error() string         { return e.Code }
func failure(code, field string) error { return &Error{Code: code, Field: field} }

const (
	InputInvalid              = "CHANNEL_INPUT_INVALID"
	RevisionConflict          = "CHANNEL_REVISION_CONFLICT"
	BindingRevisionConflict   = "CHANNEL_BINDING_REVISION_CONFLICT"
	CredentialRequired        = "CHANNEL_CREDENTIAL_REQUIRED"
	CredentialVersionConflict = "CHANNEL_CREDENTIAL_VERSION_CONFLICT"
	AccountDisabled           = "CHANNEL_ACCOUNT_DISABLED"
	AccountMustBeDisabled     = "CHANNEL_ACCOUNT_MUST_BE_DISABLED"
	VersionExhausted          = "CHANNEL_VERSION_EXHAUSTED"
	RouteGenerationExhausted  = "CHANNEL_ROUTE_GENERATION_EXHAUSTED"
	SourceIntegrity           = "CHANNEL_SOURCE_INTEGRITY"
	TargetIntegrity           = "CHANNEL_TARGET_INTEGRITY"
)
