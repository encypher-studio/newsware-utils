package nwelastic

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/pkg/errors"
)

var (
	// maxQuerySize is set to 90MB, the maximum HTTP request size is 100MB in ElasticSearch
	maxQuerySize int = 90e6
)

type Repository interface {
	InsertNews(News) error
}

type NewsRepository struct {
	elastic  Elastic
	Index    string // Defaults to "news"
	sequence Sequence
}

// NewNewsRepository creates a NewsRepository, if the context is a test, an index other than "news" must be passed otherwise it will fail.
func NewNewsRepository(elastic Elastic, sequenceIndex ...string) (NewsRepository, error) {
	if flag.Lookup("test.v") != nil && elastic.Config.NewsIndex == "news" {
		return NewsRepository{}, errors.New("can't use index 'news' for tests")
	}

	err := elastic.StartClient()
	if err != nil {
		return NewsRepository{}, err
	}

	err = elastic.StartTypedClient()
	if err != nil {
		return NewsRepository{}, err
	}

	sequence, err := NewSequence(elastic, elastic.Config.NewsIndex, sequenceIndex...)
	if err != nil {
		return NewsRepository{}, err
	}

	return NewsRepository{
		Index:    elastic.Config.NewsIndex,
		elastic:  elastic,
		sequence: sequence,
	}, nil
}

// InsertBatch inserts a batch of news, if the batch is too big, it is uploaded in sub-batches.
// news must be ordered from [oldest... newest]. The insertedCallback is called after a sub-batch is inserted
// it sends as arguments the total amount of news in the sub-batch and the batch index of the last item in the
// sub-batch.
func (b NewsRepository) InsertBatch(news []*News, insertedCallback func(totalIndexed int, lastIndex int)) error {
	if len(news) == 0 {
		return nil
	}

	fromIndex := 0
	for {
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

		bulkIndexer, err := b.elastic.bulkIndexer(b.Index)
		if err != nil {
			return err
		}

		ids, err := b.sequence.GenerateUniqueIds(toIndex - fromIndex)
		if err != nil {
			return err
		}

		for i, newsItem := range news[fromIndex:toIndex] {
			// Assign the highest id to the newest record
			newsItem.Id = ids[i]
			newsItemBytes, err := json.Marshal(newsItem)
			if err != nil {
				return err
			}
			err = bulkIndexer.Add(context.Background(), esutil.BulkIndexerItem{
				Index:      b.Index,
				DocumentID: strconv.FormatInt(newsItem.Id, 10),
				Action:     "index",
				Body:       bytes.NewReader(newsItemBytes),
			})
			if err != nil {
				return err
			}
		}

		if err := bulkIndexer.Close(context.Background()); err != nil {
			return err
		}

		insertedCallback(toIndex-fromIndex, toIndex-1)

		if toIndex >= len(news) {
			break
		}
		fromIndex = toIndex
	}

	return nil
}

func (b NewsRepository) Insert(news *News) error {
	news.CreationTime = time.Now()
	if len(news.Body) > maxQuerySize {
		news.Body = ""
	}

	ids, err := b.sequence.GenerateUniqueIds(1)
	if err != nil {
		return err
	}
	news.Id = ids[0]

	_, err = b.elastic.typedClient.Index(b.Index).Request(news).Id(strconv.FormatInt(news.Id, 10)).Do(context.Background())
	if err != nil {
		return errors.Wrap(err, "failed to insert news")
	}

	return nil
}
