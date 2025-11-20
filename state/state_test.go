package state

import (
	"encoding/json"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"
)

const (
	stateTestDir  = "./state_test"
	stateTestPath = stateTestDir + "/state.json"
)

func Test_saveState(t *testing.T) {
	s, err := NewState(stateTestPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.RemoveAll(stateTestDir)
	}()

	err = s.saveState("key", "value")
	if err != nil {
		t.Fatal(err)
	}

	// Test if State is persisted
	f, err := s.ensureStateFile()
	if err != nil {
		t.Fatal(err)
	}

	actual, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	expected := "{\"key\":\"value\"}"
	if string(actual) != expected {
		t.Fatalf("state not persisted, expected: %s, got: %s", expected, string(actual))
	}

	// Save second State
	err = s.saveState("key2", "value2")
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return
	}

	actual, err = io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	expected = "{\"key\":\"value\",\"key2\":\"value2\"}"
	if string(actual) != expected {
		t.Fatalf("second state not persisted, expected: %s, got: %s", expected, string(actual))
	}

	// Overwrite State
	err = s.saveState("key", "value3")
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return
	}

	actual, err = io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	expected = "{\"key\":\"value3\",\"key2\":\"value2\"}"
	if string(actual) != expected {
		t.Fatalf("failed to overwrite state, expected: %s, got: %s", expected, string(actual))
	}
}

func Test_state_initFromFile(t *testing.T) {
	tests := []struct {
		name     string
		expected map[string]interface{}
	}{
		{
			name:     "No file",
			expected: make(map[string]interface{}),
		},
		{
			name:     "File with state",
			expected: map[string]interface{}{"key": "value", "key2": map[string]interface{}{"key3": "value3"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				_ = os.RemoveAll(stateTestDir)
			}()
			s := &State{
				filePath: stateTestPath,
				mutex:    &sync.Mutex{},
				state:    nil,
			}

			if tt.expected != nil {
				f, err := s.ensureStateFile()
				if err != nil {
					t.Fatal(err)
				}

				expectedBytes, err := json.Marshal(tt.expected)
				if err != nil {
					t.Fatal(err)
				}

				_, err = f.Write(expectedBytes)
				if err != nil {
					t.Fatal(err)
				}
			}

			err := s.initFromFile()
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(tt.expected, s.state) {
				t.Fatalf("expected state not on file, expected: %+v, got: %+v", tt.expected, s.state)
			}
		})
	}
}
