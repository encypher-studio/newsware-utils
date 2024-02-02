package indexer

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/response"
	"github.com/pkg/errors"
)

// Indexer helps sending news to the indexer service: https://github.com/encypher-studio/newsware-indexer
type Indexer struct {
	// The host where the indexer service is reachable (must include port if applies)
	host        string
	pathPrefix  string
	contentType string
	apiKey      string
}

func New(config Config) (Indexer, error) {
	i := new(config)
	return i, i.Ping()
}

func new(config Config) Indexer {
	return Indexer{
		host:        config.Host,
		pathPrefix:  "/api/v1",
		contentType: "application/json",
		apiKey:      config.ApiKey,
	}
}

func (i Indexer) Index(news *nwelastic.News) error {
	newsJson, err := json.Marshal(news)
	if err != nil {
		return errors.Wrap(err, "marshaling news item")
	}
	resp, err := http.Post(i.urlWithAuth("/upload"), i.contentType, bytes.NewReader(newsJson))
	if err != nil {
		return errors.Wrap(err, "calling /upload")
	}

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}

	respApi, err := handleErrorResponse[*int](resp)
	if err != nil {
		return errors.Wrap(err, "handling error response")
	}

	return errors.New(respApi.Error.Message)
}

// IndexBatch calls /upload/batch and returns the total amount of indexed records, the last indexed id, and an error if exists
func (i Indexer) IndexBatch(news []*nwelastic.News) (int, int, error) {
	newsJson, err := json.Marshal(news)
	if err != nil {
		return -1, -1, errors.Wrap(err, "marshaling news item")
	}
	resp, err := http.Post(i.urlWithAuth("/upload/batch"), i.contentType, bytes.NewReader(newsJson))
	if err != nil {
		return -1, -1, errors.Wrap(err, "calling /upload/batch")
	}

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		respApi, err := handleResponse[UploadBatchData](resp)
		if err != nil {
			return -1, -1, errors.Wrap(err, "handling response")
		}
		return respApi.Data.TotalIndexed, respApi.Data.LastIndex, nil
	}

	respApi, err := handleErrorResponse[UploadBatchData](resp)
	if err != nil {
		return -1, -1, errors.Wrap(err, "handling error response")
	}

	return respApi.Error.Data.TotalIndexed, respApi.Error.Data.LastIndex, errors.New(respApi.Error.Message)
}

func (i Indexer) Ping() error {
	resp, err := http.Post(i.urlWithAuth("/ping"), i.contentType, nil)
	if err != nil {
		return errors.Wrap(err, "calling /ping")
	}

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}

	respApi, err := handleErrorResponse[*int](resp)
	if err != nil {
		return errors.Wrap(err, "handling error response")
	}

	return errors.New(respApi.Error.Message)
}

func handleResponse[T any](resp *http.Response) (response.Response[T, *int], error) {
	var respApi response.Response[T, *int]
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return response.Response[T, *int]{}, errors.Wrap(err, "reading response body")
	}

	err = json.Unmarshal(respBytes, &respApi)
	if err != nil {
		return response.Response[T, *int]{}, errors.Wrap(err, "unmarshaling error response")
	}

	return respApi, nil
}

func handleErrorResponse[T any](resp *http.Response) (response.Response[*int, T], error) {
	var respApi response.Response[*int, T]
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return response.Response[*int, T]{}, errors.Wrap(err, "reading response body")
	}

	err = json.Unmarshal(respBytes, &respApi)
	if err != nil {
		return response.Response[*int, T]{}, errors.Wrap(err, "unmarshaling error response")
	}

	return respApi, nil
}

func (i Indexer) urlWithAuth(endpoint string) string {
	return i.generateUrl(endpoint) + "?apiKey=" + i.apiKey
}

func (i Indexer) generateUrl(endpoint string) string {
	return i.host + i.pathPrefix + endpoint
}
