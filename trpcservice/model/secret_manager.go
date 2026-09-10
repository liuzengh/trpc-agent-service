package model

import "context"

// SecretManager reads one plaintext secret only inside an authorized,
// tenant-scoped factory call. Implementations must not persist, log, or return
// a secret through an error.
type SecretManager interface {
	Read(context.Context, SecretScope) (SecretValue, error)
}
