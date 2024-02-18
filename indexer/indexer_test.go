package indexer

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/response"
)

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
}

func TestIndexer_Index(t *testing.T) {
	tests := []struct {
		name         string
		responseCode int
		response     interface{}
		expectedErr  error
	}{
		{
			"success",
			200,
			response.Response[*int, *int]{},
			nil,
		},
		{
			"error",
			500,
			response.Response[*int, *int]{
				Error: &response.ResponseError[*int]{
					Code:    "test_code",
					Message: "test",
				},
			},
			errors.New("test"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.responseCode)
				w.Write(marshalUnsafe(tt.response))
			}))

			i := new(Config{
				Host:   server.URL,
				ApiKey: "",
			})
			err := i.Index(&nwelastic.News{})
			if err != nil {
				if err.Error() != tt.expectedErr.Error() {
					t.Fatalf("error is not as expected, got '%s', expected '%s'", err, tt.expectedErr)
				}
			}
		})
	}
}

func TestIndexer_Index_Load(t *testing.T) {
	tests := []struct {
		name          string
		numberOfCalls int
		expectedErr   error
	}{
		{
			"100",
			100,
			nil,
		},
		{
			"1000",
			1000,
			errors.New("test"),
		},
		{
			"1000",
			100000,
			errors.New("test"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write(marshalUnsafe(response.Response[*int, *int]{}))
				r.Body.Close()
			}))

			i := new(Config{
				Host:   server.URL,
				ApiKey: "",
			})
			for range tt.numberOfCalls {
				err := i.Index(&nwelastic.News{})
				if err != nil {
					if tt.expectedErr == nil {
						t.Fatalf("unexpected error: %s", err)
					}
					if err.Error() != tt.expectedErr.Error() {
						t.Fatalf("error is not as expected, got '%s', expected '%s'", err, tt.expectedErr)
					}
				}
			}
		})
	}
}

func TestIndexer_New(t *testing.T) {
	tests := []struct {
		name         string
		responseCode int
		response     interface{}
		expectedErr  error
	}{
		{
			"success",
			200,
			response.Response[*int, *int]{},
			nil,
		},
		{
			"error",
			500,
			response.Response[*int, *int]{
				Error: &response.ResponseError[*int]{
					Code:    "test_code",
					Message: "test",
				},
			},
			errors.New("test"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.responseCode)
				w.Write(marshalUnsafe(tt.response))
			}))

			_, err := New(Config{
				Host:   server.URL,
				ApiKey: "",
			})
			if err != nil || tt.expectedErr != nil {
				if tt.expectedErr == nil || err.Error() != tt.expectedErr.Error() {
					t.Fatalf("error is not as expected, got '%s', expected '%s'", err, tt.expectedErr)
				}
			}
		})
	}
}

func marshalUnsafe(value interface{}) []byte {
	bytes, _ := json.Marshal(value)
	return bytes
}
