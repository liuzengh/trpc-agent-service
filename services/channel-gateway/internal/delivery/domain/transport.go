package domain

import "math"

// TransportPosition identifies a broker-owned stream incarnation and position.
// RawDigest covers even invalid wire bytes; business deduplication remains IntentID.
type TransportPosition struct {
	StreamName, StreamID, RawDigest string
	Sequence                        uint64
}
type TransportReceipt struct {
	Position                         TransportPosition
	Outcome, Reason, IntentID, RunID string
}

func (p TransportPosition) Validate() error {
	if p.StreamName != "REPLY_INTENTS_V1" || !opaque(p.StreamID, 128) || p.Sequence < 1 || p.Sequence > math.MaxInt64 || !digestPattern.MatchString(p.RawDigest) {
		return ErrInvalid
	}
	return nil
}
func (r TransportReceipt) Validate() error {
	if r.Position.Validate() != nil {
		return ErrInvalid
	}
	if r.Outcome == "ACCEPTED" && r.Reason == "" && identifier.MatchString(r.IntentID) && identifier.MatchString(r.RunID) {
		return nil
	}
	if r.Outcome == "REJECTED" {
		switch r.Reason {
		case "INVALID_WIRE", "CONFLICT", "UNAUTHORIZED", "EXPIRED", "UNSUPPORTED":
			return nil
		}
	}
	return ErrInvalid
}
