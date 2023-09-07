package nwelastic

import (
	"bytes"
	"context"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"sort"
	"sync"
)

// Sequence helps retrieve unique and sequential ids from ElasticSearch using the method:
// https://blogs.perl.org/users/clinton_gormley/2011/10/elasticsearchsequence---a-blazing-fast-ticket-server.html
type Sequence struct {
	elastic *Elastic
	index   string
}

// Init initializes Sequence. The index argument is the index for which a sequence will be generated.
func (s *Sequence) Init(elastic *Elastic, index string) error {
	s.index = index
	s.elastic = elastic

	err := s.elastic.StartClient()
	if err != nil {
		return err
	}

	return nil
}

// GenerateUniqueIds requests unique ids to the "sequence" index, the ids are generated for s.index
func (s *Sequence) GenerateUniqueIds(amount int) ([]int64, error) {
	bulkIndexer, err := s.elastic.bulkIndexer(s.index)
	if err != nil {
		return nil, err
	}

	var ids []int64
	muIds := &sync.Mutex{}
	for i := 0; i < amount; i++ {
		err = bulkIndexer.Add(context.Background(), esutil.BulkIndexerItem{
			Index:      "sequence",
			Action:     "index",
			DocumentID: s.index,
			Body:       bytes.NewReader([]byte("{}")),
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				muIds.Lock()
				defer muIds.Unlock()
				ids = append(ids, res.Version)
			},
		})
		if err != nil {
			return nil, err
		}
	}

	if err = bulkIndexer.Close(context.Background()); err != nil {
		return nil, err
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	return ids, nil
}
