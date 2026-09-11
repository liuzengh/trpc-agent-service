package execution

import (
	"strconv"
)

// The framework's session.Service is keyed by three strings — app name, user
// id, session id — and the platform's own identity is richer than that: it
// carries a tenant, an app, a binding, a single-or-group actor key, and a
// generation. None of those may be lost when a session crosses the boundary
// into framework code, because a collision there is a cross-tenant or
// cross-session mix-up that no amount of MySQL-side scoping would catch:
// Redis would already be holding the wrong conversation.
//
// The encoding is length-prefixed rather than delimiter-joined. A delimiter
// (':', the shape the first batch used in channels.InboundMessage.SessionID)
// is ambiguous the moment any field itself contains it, and a WeCom userid or
// an external_userid is exactly the sort of opaque id that can. Length
// prefixes cannot be ambiguous, and the result is still a plain ASCII string,
// so it needs no secondary encoding to survive a column or a Redis key.

// SessionID is the framework session id for one claim.
func SessionID(c *Claim) string {
	return sessionIDFromColumns(c.TenantID, c.AppID, c.BindingID, c.ActorKey, c.Generation)
}

// sessionIDFromColumns is the same encoding built from raw session columns,
// for code paths (operator disposition, repair) that hold a session row and
// no claim. Keeping it one function is what makes the two call sites agree
// on identity — a second, similar-looking encoder would eventually disagree.
func sessionIDFromColumns(tenantID string, appID, bindingID int64, actorKey string, generation uint32) string {
	return encode(
		tenantID,
		strconv.FormatInt(appID, 10),
		strconv.FormatInt(bindingID, 10),
		actorKey,
		strconv.FormatUint(uint64(generation), 10),
	)
}

// FrameworkKeys returns the (app name, user id) pair the framework's session
// key needs, derived from the same identity SessionID encodes. The user id
// carries the tenant prefix, because the framework's own user-level state
// namespace is keyed by app name plus user id alone.
func FrameworkKeys(c *Claim) (appName, userID string) {
	return encode(c.TenantID), encode(c.TenantID, c.ActorKey)
}

// encode prefixes each segment with its own length, so no segment content can
// imitate a boundary.
func encode(segments ...string) string {
	out := ""
	for _, s := range segments {
		out += strconv.Itoa(len(s)) + ":" + s
	}
	return out
}
