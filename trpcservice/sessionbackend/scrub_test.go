package sessionbackend_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/stretchr/testify/require"
)

// Scrub is exercised through the public API on purpose. It is the redaction
// every caller with a connection string is expected to use — the bootstrap in
// cmd/trpc-service runs its pool, ping and migration errors through it — so its
// contract has to hold from outside this package, not only where it is called
// internally.
//
// Nothing here touches a server: an error is just a string to redact.

// redactionMarker is the replacement Scrub leaves behind. It is spelled out
// rather than imported, because it is part of what a caller sees.
const redactionMarker = "[REDACTED]"

func TestScrubRemovesConnectionStringPasswords(t *testing.T) {
	cases := map[string]struct {
		connectionString string
		raw              string
		secret           string
	}{
		"url userinfo": {
			connectionString: "postgres://user:s3cret@host:5432/db",
			raw:              `failed to parse "postgres://user:s3cret@host:5432/db"`,
			secret:           "s3cret",
		},
		// A password with URL metacharacters reaches the driver percent
		// encoded, so the encoded spelling is what an echoed DSN contains.
		"percent encoded url userinfo": {
			connectionString: "postgres://user:p%40ss%20w0rd@host:5432/db",
			raw:              `failed to parse "postgres://user:p%40ss%20w0rd@host:5432/db"`,
			secret:           "p%40ss%20w0rd",
		},
		// The decoded spelling has to go too: a driver that percent-decodes the
		// userinfo before reporting it would otherwise print the password.
		"decoded spelling of a percent encoded password": {
			connectionString: "postgres://user:p%40ss%20w0rd@host:5432/db",
			raw:              "cannot authenticate user with password p@ss w0rd",
			secret:           "p@ss w0rd",
		},
		"libpq keyword": {
			connectionString: "host=h user=u password=s3cret",
			raw:              "cannot connect: host=h user=u password=s3cret",
			secret:           "s3cret",
		},
		// A quoted libpq value may contain spaces. Splitting on whitespace
		// would redact only "p@ss" and leave "w0rd" in the log.
		"quoted libpq keyword": {
			connectionString: "host=h password='p@ss w0rd' user=u",
			raw:              "cannot connect with password=p@ss w0rd",
			secret:           "p@ss w0rd",
		},
		"redis url": {
			connectionString: "redis://user:s3cret@host:6379",
			raw:              "dial redis://user:s3cret@host:6379: refused",
			secret:           "s3cret",
		},
		// A DSN that is not a valid URL, reported by a driver that quotes the
		// whole unparsed string back. Redaction here cannot depend on the
		// string parsing, since by definition it does not.
		//
		// This is the public contract, not pgx: the pinned pgx renders only the
		// bare "invalid port" message and never appends the original string.
		// Scrub is exported and used with other connection strings, so a driver
		// that does echo the whole DSN has to be covered independently.
		"connection string that is not a valid url": {
			connectionString: "postgres://user:s3cret@host:notaport/db",
			raw: `cannot parse ` + "`postgres://user:xxxxx@host:notaport/db`" +
				`: failed to parse as URL (parse "postgres://user:s3cret@host:notaport/db": ` +
				`invalid port ":notaport" after host)`,
			secret: "s3cret",
		},
		"unparseable url with an encoded password": {
			connectionString: "postgres://user:p%40ss%20w0rd@host:notaport/db",
			raw:              `parse "postgres://user:p%40ss%20w0rd@host:notaport/db": invalid port`,
			secret:           "p%40ss%20w0rd",
		},
		// The case that matters most, because it is the one pgx really leaks.
		// The raw text here is pgx's actual output for this DSN, not an
		// invented shape: an unencoded "/" ends the authority, so the "@" that
		// would delimit the userinfo is out of scope before it is looked for,
		// and the parser reports what precedes the "/" as the port. pgx's own
		// redaction rewrote its copy of the userinfo to xxxxxx and printed the
		// password anyway. Note the port is valid — it is the password that
		// makes this unparseable, so a correct DSN elsewhere is no protection.
		"unencoded slash in the password": {
			connectionString: "postgres://user:s3cret/x@host:5432/db",
			raw: "cannot parse `postgres://user:xxxxxx@host:5432/db`: " +
				`failed to parse as URL (invalid port ":s3cret" after host)`,
			secret: "s3cret",
		},
		// The same shape with the whole span echoed, which is what a driver
		// that quotes the string it was given prints.
		"unencoded slash in the password of an echoed dsn": {
			connectionString: "postgres://user:s3cret/x@host:5432/db",
			raw:              `dial "postgres://user:s3cret/x@host:5432/db": refused`,
			secret:           "s3cret/x",
		},
		// An unencoded "@" in a password is not legal in a URL, but a driver
		// still echoes it. The userinfo ends at the last "@" of the authority,
		// so the whole password goes.
		"password containing an unencoded at sign": {
			connectionString: "postgres://user:p@ss@host:5432/db",
			raw:              "dial postgres://user:p@ss@host:5432/db: refused",
			secret:           "p@ss",
		},
		// Every occurrence goes, not just the first: a driver often echoes the
		// connection string once per retry.
		"repeated occurrences": {
			connectionString: "postgres://user:s3cret@host:5432/db",
			raw: "attempt 1: postgres://user:s3cret@host:5432/db; " +
				"attempt 2: postgres://user:s3cret@host:5432/db",
			secret: "s3cret",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			scrubbed := sessionbackend.Scrub(errors.New(tc.raw), tc.connectionString)
			require.Error(t, scrubbed)
			require.NotContains(t, scrubbed.Error(), tc.secret)
			require.Contains(t, scrubbed.Error(), redactionMarker)

			// Applying it again is a no-op, which is what makes it safe to scrub
			// at both the step that built an error and the boundary that returns
			// it.
			twice := sessionbackend.Scrub(scrubbed, tc.connectionString)
			require.Equal(t, scrubbed.Error(), twice.Error())
		})
	}
}

// TestScrubRedactsAPasswordFragmentQuotedAsAPort covers the fragment a global
// substring replacement cannot reach.
//
// The raw text here is pgx's real output for this DSN, taken from a run against
// the pinned version: its own copy of the userinfo is redacted to xxxxxx, and
// the parser quotes the single character before the unencoded "/" back as the
// offending port. One character cannot be blanked everywhere — it appears in
// "parse", "port" and "host" — so it is redacted by position instead, and the
// assertion is that this specific quoted position no longer holds it.
func TestScrubRedactsAPasswordFragmentQuotedAsAPort(t *testing.T) {
	const connectionString = "postgres://user:p/secret@host:notaport/db"
	raw := "cannot parse `postgres://user:xxxxxx@host:notaport/db`: " +
		`failed to parse as URL (invalid port ":p" after host)`

	scrubbed := sessionbackend.Scrub(errors.New(raw), connectionString).Error()
	require.NotContains(t, scrubbed, `invalid port ":p"`)
	require.Contains(t, scrubbed, `invalid port ":`+redactionMarker+`" after host`)

	// The rest of the message survives intact, which is the whole reason this
	// is a positional rule and not a global one.
	require.Contains(t, scrubbed, "cannot parse")
	require.Contains(t, scrubbed, "failed to parse as URL")
	require.Contains(t, scrubbed, "after host")

	twice := sessionbackend.Scrub(errors.New(scrubbed), connectionString)
	require.Equal(t, scrubbed, twice.Error())
}

// A port that is not part of the password is a diagnostic an operator needs,
// not a secret. Redacting it would be the over-correction that makes the
// positional rule dangerous, so it is pinned here.
func TestScrubKeepsAQuotedPortThatIsNotPartOfThePassword(t *testing.T) {
	original := errors.New(
		"cannot parse `postgres://user:xxxxxx@host:notaport/db`: " +
			`failed to parse as URL (invalid port ":notaport" after host)`)

	scrubbed := sessionbackend.Scrub(original, "postgres://user:s3cret@host:notaport/db")
	require.Equal(t, original, scrubbed, "nothing here leaks, so the error is passed through")
	require.Contains(t, scrubbed.Error(), `invalid port ":notaport"`)
}

// leakyError is the shape that makes the Unwrap rule load bearing: an error
// whose own text is clean while the error it wraps still holds the raw
// connection string, so the rendered message leaks only through the chain.
//
// It is a stand-in for any such driver, not a claim about pgx: the pinned pgx
// unwraps url.Parse's *url.Error to its bare message and does not carry the
// original string. Scrub is exported and used with connection strings other
// than pgx's, so the contract is tested independently of one driver's
// behaviour.
type leakyError struct {
	connectionString string
}

func (e *leakyError) Error() string { return "parse " + e.connectionString }

func TestScrubDropsTheUnwrapChainWhenItRedacts(t *testing.T) {
	const (
		connectionString = "postgres://user:s3cret@host:5432/db"
		secret           = "s3cret"
	)
	sentinel := errors.New("cannot parse connection string")
	leak := &leakyError{connectionString: connectionString}
	original := fmt.Errorf("%w: %w", sentinel, leak)
	require.Contains(t, original.Error(), secret, "the fixture must actually leak")

	scrubbed := sessionbackend.Scrub(original, connectionString)
	require.NotContains(t, scrubbed.Error(), secret)

	// Nothing reachable from the returned error may hand the secret back, by
	// any of the three routes a caller has.
	require.Nil(t, errors.Unwrap(scrubbed))
	require.NotErrorIs(t, scrubbed, sentinel)
	var leaked *leakyError
	require.False(t, errors.As(scrubbed, &leaked))

	// The error the caller passed in is not modified, only the returned one is
	// different: Scrub has no way to edit an error someone else still holds, so
	// a caller that logs the original anyway is not protected by this call.
	require.Contains(t, original.Error(), secret)
}

func TestScrubKeepsErrorsThatCarryNoPassword(t *testing.T) {
	t.Run("nil error stays nil", func(t *testing.T) {
		require.NoError(t, sessionbackend.Scrub(nil, "postgres://u:p@h/db"))
	})

	t.Run("error without the secret keeps its identity and chain", func(t *testing.T) {
		sentinel := errors.New("connection refused")
		original := fmt.Errorf("dial host:5432: %w", sentinel)

		scrubbed := sessionbackend.Scrub(original, "postgres://u:s3cret@h/db")
		require.Equal(t, original, scrubbed)
		require.ErrorIs(t, scrubbed, sentinel)
	})

	t.Run("connection string without a password", func(t *testing.T) {
		original := errors.New("host=h user=u: connection refused")
		require.Equal(t, original, sessionbackend.Scrub(original, "host=h user=u"))
	})

	t.Run("empty connection string", func(t *testing.T) {
		original := errors.New("in-memory backends have nothing to redact")
		require.Equal(t, original, sessionbackend.Scrub(original, ""))
	})
}
