package rss

import (
	"testing"
	"time"
)

func Test_parseRetryAfter(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected time.Duration
	}{
		{"empty falls back to default", "", defaultRetryAfter},
		{"seconds", "5", 5 * time.Second},
		{"seconds with whitespace", " 12 ", 12 * time.Second},
		{"zero seconds falls back to default", "0", defaultRetryAfter},
		{"negative seconds falls back to default", "-3", defaultRetryAfter},
		{"garbage falls back to default", "soon", defaultRetryAfter},
		{"past http date falls back to default", "Mon, 02 Jan 2006 15:04:05 GMT", defaultRetryAfter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseRetryAfter(tt.value); got != tt.expected {
				t.Fatalf("parseRetryAfter(%q) = %s, want %s", tt.value, got, tt.expected)
			}
		})
	}
}

func Test_RateLimitError_Error(t *testing.T) {
	err := &RateLimitError{RetryAfter: 30 * time.Second}
	if got, want := err.Error(), "rate limited (429), retry after 30s"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}
