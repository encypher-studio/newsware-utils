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

func TestIndexer_IndexBatch(t *testing.T) {
	tests := []struct {
		name                 string
		responseCode         int
		response             interface{}
		expectedTotalIndexed int
		expectedLastIndex    int
		expectedErr          error
	}{
		{
			"success",
			200,
			response.Response[UploadBatchData, *int]{
				Data: UploadBatchData{
					TotalIndexed: 10,
					LastIndex:    20,
				},
			},
			10,
			20,
			nil,
		},
		{
			"error",
			500,
			response.Response[*int, UploadBatchData]{
				Error: &response.ResponseError[UploadBatchData]{
					Code:    "test_code",
					Message: "test",
					Data: UploadBatchData{
						TotalIndexed: 100,
						LastIndex:    200,
					},
				},
			},
			100,
			200,
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
			actualTotalIndexed, actualLastIndexed, err := i.IndexBatch([]*nwelastic.News{})
			if err != nil {
				if err.Error() != tt.expectedErr.Error() {
					t.Fatalf("error is not as expected, got '%s', expected '%s'", err, tt.expectedErr)
				}
			}

			if actualTotalIndexed != tt.expectedTotalIndexed {
				t.Fatalf("totalIndexed is not as expected, got %d, expected %d", actualTotalIndexed, tt.expectedTotalIndexed)
			}

			if actualLastIndexed != tt.expectedLastIndex {
				t.Fatalf("totalIndexed is not as expected, got %d, expected %d", actualLastIndexed, tt.expectedLastIndex)
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
