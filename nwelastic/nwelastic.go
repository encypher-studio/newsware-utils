package nwelastic

import (
	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/pkg/errors"
	"os"
)

type Elastic struct {
	typedClient *elasticsearch.TypedClient
	client      *elasticsearch.Client
	config      ElasticConfig
}

func (e *Elastic) StartClient() error {
	if e.client != nil {
		return nil
	}

	cert, err := os.ReadFile(e.config.CertPath)
	if err != nil {
		return errors.Wrap(err, "reading elastic cert")
	}

	elasticConfig := elasticsearch.Config{
		Addresses: e.config.Addresses,
		Username:  e.config.Username,
		Password:  e.config.Password,
		CACert:    cert,
	}

	if e.config.LogRequests {
		elasticConfig.Logger = &elastictransport.ColorLogger{
			Output:             os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		}
	}

	e.client, err = elasticsearch.NewClient(elasticConfig)
	if err != nil {
		return errors.Wrap(err, "creating elastic client")
	}

	return nil
}

func (e *Elastic) StartTypedClient() error {
	if e.typedClient != nil {
		return nil
	}

	cert, err := os.ReadFile(e.config.CertPath)
	if err != nil {
		return errors.Wrap(err, "reading elastic cert")
	}

	elasticConfig := elasticsearch.Config{
		Addresses: e.config.Addresses,
		Username:  e.config.Username,
		Password:  e.config.Password,
		CACert:    cert,
	}

	if e.config.LogRequests {
		elasticConfig.Logger = &elastictransport.ColorLogger{
			Output:             os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		}
	}

	e.typedClient, err = elasticsearch.NewTypedClient(elasticConfig)
	if err != nil {
		return errors.Wrap(err, "creating elastic client")
	}

	return nil
}

func (e *Elastic) BulkIndexer(index string) (esutil.BulkIndexer, error) {
	if e.client == nil {
		return nil, errors.New("call StartClient() before calling BulkIndexer()")
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
