package wecomadapter

import d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"

// FromConnectionResult translates fixed result enums and optional numeric ACK
// evidence, never raw SDK errors. A contradictory ACK is not proof of success
// or non-transmission; it must remain UNKNOWN and must not authorize a retry.
// An old/poisoned Final cannot be retried just because this particular call was
// NOT_SENT; only pre-write temporary capacity/readiness failures may retry.
func FromConnectionResult(certainty, code string, providerCode *int64) d.Result {
	if providerCode != nil && ((certainty == string(d.CertaintyAccepted) && *providerCode != 0) || certainty == string(d.CertaintyNotSent)) {
		return d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}
	}
	result := d.Result{Certainty: d.Certainty(certainty)}
	switch result.Certainty {
	case d.CertaintyAccepted:
		if code != "" {
			return d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}
		}
		return result
	case d.CertaintyRejected:
		result.ErrorClass = d.ErrorPermanent
	case d.CertaintyUnknown:
		result.ErrorClass = d.ErrorPermanent
	case d.CertaintyNotSent:
		switch code {
		case "not_ready", "capacity_exceeded", "quiescing":
			result.ErrorClass = d.ErrorTemporary
		case "stale_origin", "stale_generation":
			result.ErrorClass = d.ErrorStaleOrigin
		case "canceled":
			result.ErrorClass = d.ErrorDeadline
		default:
			result.ErrorClass = d.ErrorPermanent
		}
	default:
		return d.Result{Certainty: d.CertaintyUnknown, ErrorClass: d.ErrorPermanent}
	}
	return result
}
