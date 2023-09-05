package nwelastic

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/pkg/errors"
	"os"
	"time"
)

var (
	// maxQuerySize is set to 90MB, the maximum HTTP request size is 100MB in ElasticSearch
	maxQuerySize int = 90e6
	index            = "news"
)

type Repository interface {
	InsertNews(News) error
}

type NewsRepository struct {
	client      *elasticsearch.Client
	typedClient *elasticsearch.TypedClient
}

func (b *NewsRepository) Init(config ElasticConfig) error {
	if flag.Lookup("test.v") != nil && index == "news" {
		return errors.New("can't use index 'news' for tests")
	}

	cert, err := os.ReadFile(config.CertPath)
	if err != nil {
		return errors.Wrap(err, "reading elastic cert")
	}

	elasticConfig := elasticsearch.Config{
		Addresses: config.Addresses,
		Username:  config.Username,
		Password:  config.Password,
		CACert:    cert,
	}

	if config.LogRequests {
		elasticConfig.Logger = &elastictransport.ColorLogger{
			Output:             os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		}
	}

	b.client, err = elasticsearch.NewClient(elasticConfig)
	if err != nil {
		return errors.Wrap(err, "creating elastic client")
	}

	b.typedClient, err = elasticsearch.NewTypedClient(elasticConfig)
	if err != nil {
		return errors.Wrap(err, "creating elastic client")
	}

	return nil
}

func (b *NewsRepository) InsertNews(news []*News, insertedCallback func(totalIndexed int, lastIndex int)) error {
	shouldBreak := false
	fromIndex := 0

	for !shouldBreak {
		toIndex := fromIndex
		bodySizes := 0
		creationTime := time.Now()

		for _, newsItem := range news[fromIndex:] {
			toIndex += 1
			newsItem.CreationTime = creationTime
			if len(newsItem.Body) > maxQuerySize {
				newsItem.Body = ""
			}

			bodySizes += len(newsItem.Body)
			if bodySizes >= maxQuerySize {
				break
			}
		}

		bulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Client: b.client,
			Index:  index,
		})
		if err != nil {
			return errors.Wrap(err, "creating elastic bulk indexer")
		}

		for i, newsItem := range news[fromIndex:toIndex] {
			newsItemBytes, err := json.Marshal(newsItem)
			if err != nil {
				return err
			}
			err = bulkIndexer.Add(context.Background(), esutil.BulkIndexerItem{
				Index:  index,
				Action: "index",
				Body:   bytes.NewReader(newsItemBytes),
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					news[fromIndex+i].Id = res.DocumentID
				},
			})
			if err != nil {
				return err
			}
		}

		if err := bulkIndexer.Close(context.Background()); err != nil {
			return err
		}

		insertedCallback(toIndex-fromIndex, toIndex-1)

		fromIndex = toIndex
		if fromIndex >= len(news) {
			shouldBreak = true
		}
	}

	return nil
}
