package nwelastic

import (
	"bytes"
	"context"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/pkg/errors"
	"sort"
	"sync"
)

// Sequence helps retrieve unique and sequential ids from ElasticSearch using the method:
// https://blogs.perl.org/users/clinton_gormley/2011/10/elasticsearchsequence---a-blazing-fast-ticket-server.html
type Sequence struct {
	elastic       *Elastic
	sequenceIndex string
	index         string
}

// Init initializes Sequence. The index argument is the index for which a sequence will be generated.
func (s *Sequence) Init(elastic *Elastic, index string) error {
	s.index = index
	s.elastic = elastic

	err := s.elastic.StartClient()
	if err != nil {
		return err
	}

	err = s.elastic.StartTypedClient()
	if err != nil {
		return err
	}

	if s.sequenceIndex == "" {
		s.sequenceIndex = "sequence"
	}

	return nil
}

// GenerateUniqueIds requests unique ids to Sequence.sequenceIndex, the ids are generated for s.index
func (s *Sequence) GenerateUniqueIds(amount int) ([]int64, error) {
	bulkIndexer, err := s.elastic.bulkIndexer(s.index)
	if err != nil {
		return nil, err
	}

	var failures []error
	var ids []int64
	muIds := &sync.Mutex{}
	muFailures := &sync.Mutex{}
	for i := 0; i < amount; i++ {
		err = bulkIndexer.Add(context.Background(), esutil.BulkIndexerItem{
			Index:      s.sequenceIndex,
			Action:     "index",
			DocumentID: s.index,
			Body:       bytes.NewReader([]byte("{}")),
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				muIds.Lock()
				defer muIds.Unlock()
				ids = append(ids, res.Version)
			},
			OnFailure: func(_ context.Context, _ esutil.BulkIndexerItem, _ esutil.BulkIndexerResponseItem, err error) {
				muFailures.Lock()
				defer muFailures.Unlock()
				failures = append(failures, err)
			},
		})
		if err != nil {
			return nil, err
		}
	}

	if err = bulkIndexer.Close(context.Background()); err != nil {
		return nil, err
	}

	if len(failures) > 0 {
		return nil, errors.Wrap(failures[0], "failed to insert ids")
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	return ids, nil
}

func (s *Sequence) GetLastId() (int64, error) {
	resp, err := s.elastic.Search().Index(s.sequenceIndex).Request(&search.Request{
		Query: &types.Query{
			Term: map[string]types.TermQuery{
				"_id": {
					Value: s.index,
				},
			},
		},
		Version: esapi.BoolPtr(true),
	}).Do(context.Background())
	if err != nil {
		return 0, err
	}

	if len(resp.Hits.Hits) == 0 {
		return 0, nil
	}

	return *resp.Hits.Hits[0].Version_, nil
}
