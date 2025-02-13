package nwelastic

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/get"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/pkg/errors"
)

var (
	ErrFailedToPingElastic = errors.New("failed to ping elastic cluster")
)

type Elastic struct {
	Config      ElasticConfig
	TypedClient *elasticsearch.TypedClient
	client      *elasticsearch.Client
}

func NewElastic(config ElasticConfig) Elastic {
	if config.NewsIndex == "" {
		config.NewsIndex = "news"
	}
	elastic := Elastic{Config: config}
	return elastic
}

func (e *Elastic) StartClient() (err error) {
	if e.client != nil {
		return nil
	}

	e.client, err = elasticsearch.NewClient(e.elasticClientConfig())
	if err != nil {
		return errors.Wrap(err, "creating elastic client")
	}

	res, err := e.client.Ping()
	if err != nil {
		return errors.Wrap(err, ErrFailedToPingElastic.Error())
	}

	if res.IsError() {
		return ErrFailedToPingElastic
	}

	return nil
}

func (e *Elastic) StartTypedClient() (err error) {
	if e.TypedClient != nil {
		return nil
	}

	e.TypedClient, err = elasticsearch.NewTypedClient(e.elasticClientConfig())
	if err != nil {
		return errors.Wrap(err, "creating elastic client")
	}

	ok, err := e.TypedClient.Ping().Do(context.Background())
	if err != nil {
		return errors.Wrap(err, ErrFailedToPingElastic.Error())
	}

	if !ok {
		return ErrFailedToPingElastic
	}

	return nil
}

func (e *Elastic) bulkIndexer(index string) (esutil.BulkIndexer, error) {
	if e.client == nil {
		return nil, errors.New("call StartClient() before calling bulkIndexer()")
	}
	bulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: e.client,
		Index:  index,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating elastic bulk indexer")
	}

	return bulkIndexer, nil
}

func (e *Elastic) Search() *search.Search {
	return e.TypedClient.Search()
}

func (e *Elastic) Get(index string, documentId string) *get.Get {
	return e.TypedClient.Get(index, documentId)
}

func (e *Elastic) elasticClientConfig() elasticsearch.Config {
	elasticTransport := http.DefaultTransport.(*http.Transport)
	elasticTransport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}

	elasticConfig := elasticsearch.Config{
		Addresses: e.Config.Addresses,
		Username:  e.Config.Username,
		Password:  e.Config.Password,
		Transport: elasticTransport,
	}

	if e.Config.LogRequests {
		elasticConfig.Logger = &elastictransport.ColorLogger{
			Output:             os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		}
	}

	return elasticConfig
}
