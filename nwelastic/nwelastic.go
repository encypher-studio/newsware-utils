package nwelastic

import (
	"context"
	"crypto/tls"
	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/pkg/errors"
	"net/http"
	"os"
)

var (
	ErrFailedToPingElastic = errors.New("failed to ping elastic cluster")
)

type Elastic struct {
	Config      ElasticConfig
	typedClient *elasticsearch.TypedClient
	client      *elasticsearch.Client
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
	if e.typedClient != nil {
		return nil
	}

	e.typedClient, err = elasticsearch.NewTypedClient(e.elasticClientConfig())
	if err != nil {
		return errors.Wrap(err, "creating elastic client")
	}

	ok, err := e.typedClient.Ping().Do(context.Background())
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
	return e.typedClient.Search()
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
