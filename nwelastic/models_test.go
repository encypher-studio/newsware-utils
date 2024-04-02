package nwelastic

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestNews_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected News
	}{
		{
			"id number",
			`{"id":1,"headline":"headline"}`,
			News{Id: "1", Headline: "headline"},
		},
		{
			"id string",
			`{"id":"string","headline":"headline"}`,
			News{Id: "string", Headline: "headline"},
		},
		{
			"id is nil",
			`{"headline":"headline"}`,
			News{Id: "", Headline: "headline"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actual News
			err := json.Unmarshal([]byte(tt.json), &actual)
			if err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}

			if !reflect.DeepEqual(tt.expected, actual) {
				t.Errorf("expected: %v, actual: %v", tt.expected, actual)
			}
		})
	}
}
