package state

import (
	"os"
	"testing"
)

func TestStateString(t *testing.T) {
	defer func() {
		_ = os.RemoveAll(stateTestDir)
	}()
	// Test save
	s, err := newState(stateTestPath)
	if err != nil {
		t.Fatal(err)
	}

	expected := "expected"
	ss, err := NewStateString(&s, "key")
	if err != nil {
		t.Fatal(err)
	}

	err = ss.SaveState(expected)
	if err != nil {
		t.Fatal(err)
	}

	if expected != ss.Get() {
		t.Fatalf("expected state not saved, expected: %s, got: %s", expected, ss.Get())
	}

	// Test reading from file
	s, err = newState(stateTestPath)
	if err != nil {
		t.Fatal(err)
	}

	ss, err = NewStateString(&s, "key")
	if err != nil {
		t.Fatal(err)
	}

	if expected != ss.Get() {
		t.Fatalf("expected state not on file, expected: %s, got: %s", expected, ss.Get())
	}
}
