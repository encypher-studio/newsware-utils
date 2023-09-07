package nwelastic

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/pkg/errors"
	"strconv"
	"time"
)

var (
	// maxQuerySize is set to 90MB, the maximum HTTP request size is 100MB in ElasticSearch
	maxQuerySize int = 90e6
)

type Repository interface {
	InsertNews(News) error
}

type NewsRepository struct {
	elastic  *Elastic
	Index    string // Defaults to "news"
	sequence Sequence
}

func (b *NewsRepository) Init(elastic *Elastic) error {
	if b.Index == "" {
		b.Index = "news"
	}

	if flag.Lookup("test.v") != nil && b.Index == "news" {
		return errors.New("can't use index 'news' for tests")
	}

	b.elastic = elastic

	err := b.elastic.StartClient()
	if err != nil {
		return err
	}

	err = b.elastic.StartTypedClient()
	if err != nil {
		return err
	}

	b.sequence = Sequence{
		elastic: elastic,
		index:   b.Index,
	}

	return nil
}

func (b *NewsRepository) InsertBatch(news []*News, insertedCallback func(totalIndexed int, lastIndex int)) error {
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

		bulkIndexer, err := b.elastic.bulkIndexer(b.Index)
		if err != nil {
			return err
		}

		ids, err := b.sequence.GenerateUniqueIds(toIndex - fromIndex)
		if err != nil {
			return err
		}

		for i, newsItem := range news[fromIndex:toIndex] {
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

		fromIndex = toIndex
		if fromIndex >= len(news) {
			shouldBreak = true
		}
	}

	return nil
}

func (b *NewsRepository) Insert(news *News) error {
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
