// Package argon2id implements Identity password hashing with the PHC string
// format.
package argon2id

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Parameters controls newly generated password hashes. Existing hashes carry
// their own parameters in the encoded PHC string.
type Parameters struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// DefaultParameters returns the V1 password-hashing cost.
func DefaultParameters() Parameters {
	return Parameters{
		Memory:      64 * 1024,
		Iterations:  3,
		Parallelism: 2,
		SaltLength:  16,
		KeyLength:   32,
	}
}

// Hasher creates and verifies Argon2id password hashes.
type Hasher struct {
	params Parameters
}

// New returns a password hasher with explicit generation parameters.
func New(params Parameters) *Hasher {
	return &Hasher{params: params}
}

// Hash returns an Argon2id PHC string containing a random salt and all
// parameters needed for later verification.
func (h *Hasher) Hash(password string) (string, error) {
	if h == nil {
		return "", errors.New("argon2id: nil hasher")
	}
	if err := validateParameters(h.params); err != nil {
		return "", err
	}

	salt := make([]byte, h.params.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("argon2id: generate salt: %w", err)
	}
	hash := argon2.IDKey(
		[]byte(password),
		salt,
		h.params.Iterations,
		h.params.Memory,
		h.params.Parallelism,
		h.params.KeyLength,
	)

	return fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		h.params.Memory,
		h.params.Iterations,
		h.params.Parallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	), nil
}

// Verify checks a plaintext password against an encoded Argon2id PHC string.
func (h *Hasher) Verify(encodedHash, password string) (bool, error) {
	params, salt, expected, err := parseEncodedHash(encodedHash)
	if err != nil {
		return false, err
	}

	actual := argon2.IDKey(
		[]byte(password),
		salt,
		params.Iterations,
		params.Memory,
		params.Parallelism,
		uint32(len(expected)),
	)
	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}

func parseEncodedHash(encoded string) (Parameters, []byte, []byte, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return Parameters{}, nil, nil, errors.New("argon2id: malformed PHC string")
	}

	version, err := strconv.Atoi(strings.TrimPrefix(parts[2], "v="))
	if err != nil || version != argon2.Version {
		return Parameters{}, nil, nil, errors.New("argon2id: unsupported version")
	}

	params, err := parseParameters(parts[3])
	if err != nil {
		return Parameters{}, nil, nil, err
	}
	salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
	if err != nil {
		return Parameters{}, nil, nil, errors.New("argon2id: malformed salt")
	}
	expected, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
	if err != nil {
		return Parameters{}, nil, nil, errors.New("argon2id: malformed hash")
	}
	params.SaltLength = uint32(len(salt))
	params.KeyLength = uint32(len(expected))
	if err := validateParameters(params); err != nil {
		return Parameters{}, nil, nil, err
	}
	return params, salt, expected, nil
}

func parseParameters(encoded string) (Parameters, error) {
	var params Parameters
	fields := strings.Split(encoded, ",")
	if len(fields) != 3 {
		return Parameters{}, errors.New("argon2id: malformed parameters")
	}

	memory, err := parseUintField(fields[0], "m=", 32)
	if err != nil {
		return Parameters{}, err
	}
	iterations, err := parseUintField(fields[1], "t=", 32)
	if err != nil {
		return Parameters{}, err
	}
	parallelism, err := parseUintField(fields[2], "p=", 8)
	if err != nil {
		return Parameters{}, err
	}
	params.Memory = uint32(memory)
	params.Iterations = uint32(iterations)
	params.Parallelism = uint8(parallelism)
	return params, nil
}

func parseUintField(value, prefix string, bitSize int) (uint64, error) {
	if !strings.HasPrefix(value, prefix) {
		return 0, errors.New("argon2id: malformed parameters")
	}
	parsed, err := strconv.ParseUint(strings.TrimPrefix(value, prefix), 10, bitSize)
	if err != nil {
		return 0, errors.New("argon2id: malformed parameters")
	}
	return parsed, nil
}

func validateParameters(params Parameters) error {
	if params.Memory < 8*uint32(params.Parallelism) || params.Memory > 1024*1024 {
		return errors.New("argon2id: memory outside supported range")
	}
	if params.Iterations == 0 || params.Iterations > 10 {
		return errors.New("argon2id: iterations outside supported range")
	}
	if params.Parallelism == 0 || params.Parallelism > 16 {
		return errors.New("argon2id: parallelism outside supported range")
	}
	if params.SaltLength < 8 || params.SaltLength > 64 {
		return errors.New("argon2id: salt length outside supported range")
	}
	if params.KeyLength < 16 || params.KeyLength > 64 {
		return errors.New("argon2id: key length outside supported range")
	}
	return nil
}
