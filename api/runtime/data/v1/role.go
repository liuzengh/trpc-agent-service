package datav1

const (
	SessionIsolation = "tenant-session-v1"
	MemoryIsolation  = "tenant-subject-agent-v1"
)

// ForRole binds isolation to an already authorized data role, not to a database
// type. It does not authorize a caller: the catalogue must check tenant/role use
// first. Physical configuration stays fixed, and existing Session bytes stay equal.
func (s Snapshot) ForRole(role string) (Snapshot, error) {
	if s.Validate() != nil {
		return Snapshot{}, ErrSnapshot
	}
	out := s.Clone()
	switch role {
	case "session", "memory":
		if out.Kind != PostgreSQL && out.Kind != Redis {
			return Snapshot{}, ErrSnapshot
		}
		if role == "session" {
			out.Isolation = SessionIsolation
		} else {
			out.Isolation = MemoryIsolation
		}
	case "knowledge":
		if out.Kind != Qdrant {
			return Snapshot{}, ErrSnapshot
		}
	case "artifact":
		if out.Kind != S3 {
			return Snapshot{}, ErrSnapshot
		}
	default:
		return Snapshot{}, ErrSnapshot
	}
	return out, nil
}

// ValidateForRole is required at a consumer boundary. Validate alone accepts a
// physical backend shape, and must not make a Memory descriptor a Session grant.
func (s Snapshot) ValidateForRole(role string) error {
	expected, err := s.ForRole(role)
	if err != nil || expected.Isolation != s.Isolation {
		return ErrSnapshot
	}
	return nil
}
