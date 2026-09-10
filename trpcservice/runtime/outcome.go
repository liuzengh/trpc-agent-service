package runtime

type Outcome string

const (
	OutcomePending             Outcome = "pending"
	OutcomeQueued              Outcome = "queued"
	OutcomeRunning             Outcome = "running"
	OutcomeBlocked             Outcome = "blocked"
	OutcomeWaitingConfirmation Outcome = "waiting_confirmation"
	OutcomeSucceeded           Outcome = "succeeded"
	OutcomeDenied              Outcome = "denied"
	OutcomeFailed              Outcome = "failed"
	OutcomeCancelled           Outcome = "cancelled"
	OutcomeConfirmationDenied  Outcome = "confirmation_denied"
	OutcomeConfirmationTimeout Outcome = "confirmation_timeout"
)

func (o Outcome) Terminal() bool {
	switch o {
	case OutcomeSucceeded, OutcomeDenied, OutcomeFailed, OutcomeCancelled,
		OutcomeConfirmationDenied, OutcomeConfirmationTimeout:
		return true
	default:
		return false
	}
}
