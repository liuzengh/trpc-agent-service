package secretref

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A reference that parses gives back exactly the name it carried. The cases
// below are the ones that would differ under any normalization: leading
// underscores, digits after the first character, and a very long name are all
// valid, and none of them is rewritten.
func TestEnvNameAcceptsValidReferences(t *testing.T) {
	for _, tc := range []struct {
		ref  string
		want string
	}{
		{ref: "env:OPENAI_API_KEY", want: "OPENAI_API_KEY"},
		{ref: "env:_PRIVATE", want: "_PRIVATE"},
		{ref: "env:a", want: "a"},
		{ref: "env:MixedCase_9", want: "MixedCase_9"},
		{ref: "env:" + strings.Repeat("K", 512), want: strings.Repeat("K", 512)},
		// "env:" appearing again is part of the name only if it could be one; it
		// cannot, so this belongs with the refusals below. Here instead: a name
		// that merely starts with the scheme's letters.
		{ref: "env:envVAR", want: "envVAR"},
	} {
		t.Run(tc.ref, func(t *testing.T) {
			name, err := EnvName(tc.ref)
			require.NoError(t, err)
			require.Equal(t, tc.want, name)
		})
	}
}

// Everything else is refused, and the refusal never repeats the input. The
// likeliest malformed reference is a key that was pasted where a name belonged,
// so the one thing an error here must not do is print it.
func TestEnvNameRejectsEverythingElse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ref     string
		wantErr error
	}{
		{name: "empty", ref: "", wantErr: ErrScheme},
		{name: "no scheme", wantErr: ErrScheme, ref: "OPENAI_API_KEY"},
		{name: "a literal key", ref: "sk-live-0123456789", wantErr: ErrScheme},
		{name: "uppercase scheme", ref: "ENV:OPENAI_API_KEY", wantErr: ErrScheme},
		{name: "mixed-case scheme", ref: "Env:OPENAI_API_KEY", wantErr: ErrScheme},
		{name: "leading space", ref: " env:OPENAI_API_KEY", wantErr: ErrScheme},
		{name: "another scheme", ref: "file:/etc/key", wantErr: ErrScheme},
		{name: "vault scheme", ref: "vault://secret/key", wantErr: ErrScheme},
		{name: "scheme without colon", ref: "env", wantErr: ErrScheme},
		{name: "scheme only", ref: "env:", wantErr: ErrNoName},
		{name: "trailing space in name", ref: "env:OPENAI_API_KEY ", wantErr: ErrInvalidName},
		{name: "leading space in name", ref: "env: OPENAI_API_KEY", wantErr: ErrInvalidName},
		{name: "leading digit", ref: "env:1KEY", wantErr: ErrInvalidName},
		{name: "hyphen", ref: "env:OPENAI-API-KEY", wantErr: ErrInvalidName},
		{name: "dot", ref: "env:openai.api.key", wantErr: ErrInvalidName},
		{name: "equals", ref: "env:KEY=value", wantErr: ErrInvalidName},
		{name: "newline", ref: "env:KEY\n", wantErr: ErrInvalidName},
		{name: "null byte", ref: "env:KEY\x00", wantErr: ErrInvalidName},
		{name: "a key behind the scheme", ref: "env:sk-live-0123456789", wantErr: ErrInvalidName},
		{name: "repeated scheme", ref: "env:env:KEY", wantErr: ErrInvalidName},
		{name: "path", ref: "env:/etc/key", wantErr: ErrInvalidName},
		{name: "non-ascii", ref: "env:CLÉ", wantErr: ErrInvalidName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name, err := EnvName(tc.ref)
			require.ErrorIs(t, err, tc.wantErr)
			require.Empty(t, name)
			// A reference that is itself part of the scheme ("env", "env:")
			// unavoidably appears in a message that names the scheme, and
			// carries nothing anyway.
			if tc.ref != "" && !strings.HasPrefix(EnvScheme, tc.ref) {
				require.NotContains(t, err.Error(), tc.ref, "the refusal echoed the reference")
			}
		})
	}
}

// IsEnvName is used on its own by the entitlement rules, so its boundaries are
// checked directly rather than only through EnvName.
func TestIsEnvName(t *testing.T) {
	for _, name := range []string{"A", "_", "_9", "a9_Z", "TRPC_SERVICE_API_KEY"} {
		require.True(t, IsEnvName(name), name)
	}
	for _, name := range []string{"", "9", "9A", "a-b", "a b", "a\tb", "é", "a.b", "a/b"} {
		require.False(t, IsEnvName(name), name)
	}
}
