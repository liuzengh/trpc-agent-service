package app

import "testing"

func TestChangeEventCloneCopiesRevisionPointers(t *testing.T) {
	previous := int64(1)
	current := int64(2)
	original := ChangeEvent{PreviousRevision: &previous, CurrentRevision: &current}
	clone := original.Clone()
	*clone.PreviousRevision = 10
	*clone.CurrentRevision = 20
	if *original.PreviousRevision != 1 || *original.CurrentRevision != 2 {
		t.Fatal("change event clone leaked revision pointer mutations")
	}

	empty := (ChangeEvent{}).Clone()
	if empty.PreviousRevision != nil || empty.CurrentRevision != nil {
		t.Fatal("nil revision pointers must remain nil")
	}
}
