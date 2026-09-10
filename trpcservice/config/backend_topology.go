package config

// ValidateBackendTopology enforces cross-domain storage invariants that cannot
// be expressed by a single Backend Profile capability. In particular, summary
// watermark CAS is committed with session state/events, so its body store must
// be pinned to the exact same Profile revision as the session authority.
func ValidateBackendTopology(bindings []BackendBinding) error {
	var session, summary *BackendBinding
	for index := range bindings {
		binding := &bindings[index]
		switch binding.Domain {
		case "session":
			session = binding
		case "summary":
			summary = binding
		}
	}
	if summary == nil {
		return nil
	}
	if session == nil || !requiresBackendCapability(session.Required, "atomic_turn_commit") ||
		!requiresBackendCapability(summary.Required, "summary_cas") ||
		session.BackendProfileID != summary.BackendProfileID || session.BackendVersion != summary.BackendVersion {
		return ErrInvalid
	}
	return nil
}

func requiresBackendCapability(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
