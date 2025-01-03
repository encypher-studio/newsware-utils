package nwelastic

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/stretchr/testify/suite"
)

var (
	TestElasticConfig = ElasticConfig{
		Addresses:   []string{"https://localhost:9200"},
		Username:    "elastic",
		Password:    "changeme",
		NewsIndex:   "nwelastic_tests",
		LogRequests: false,
	}
)

type publicationTimeSort struct {
	PublicationTime sortOrder `json:"publicationTime"`
}

type sortOrder struct {
	Order string `json:"order"`
}

// newsRepositorySuite performs integration tests, they are run unless the test -short flag is set
type newsRepositorySuite struct {
	suite.Suite
	config         ElasticConfig
	newsRepository NewsRepository
}

func (r *newsRepositorySuite) SetupSuite() {
	var err error
	elastic := NewElastic(TestElasticConfig)
	r.newsRepository, err = NewNewsRepository(elastic)
	if err != nil {
		r.FailNow(err.Error())
	}

	_, _ = r.newsRepository.elastic.TypedClient.Indices.Delete(r.newsRepository.Index).Do(nil)
}

func (r *newsRepositorySuite) SetupSubTest() {
	_, err := r.newsRepository.elastic.TypedClient.Indices.Create(r.newsRepository.Index).Do(nil)
	if err != nil {
		r.FailNow(err.Error())
	}
}

func (r *newsRepositorySuite) TearDownSubTest() {
	_, err := r.newsRepository.elastic.TypedClient.Indices.Delete(r.newsRepository.Index).Do(nil)
	if err != nil {
		r.FailNow(err.Error())
	}
}

func (r *newsRepositorySuite) TestNewsRepository_InsertBatch() {
	maxQuerySize = 1000
	defaultTime := time.Now()

	tests := []struct {
		name         string
		insertNews   []*News
		expectedNews []*News
	}{
		{
			"insert news",
			[]*News{
				{
					Id:              "1",
					Headline:        "headline",
					Body:            "body",
					Tickers:         []string{"ticker"},
					Ciks:            []int{1, 2, 3},
					Link:            "link",
					Source:          "SOURCE",
					CategoryCodes:   []string{"categoryCode1, categoryCode11"},
					IndustryCodes:   []string{"industryCode1, industryCode11"},
					RegionCodes:     []string{"regionCode1, regionCode11"},
					PublicationTime: defaultTime.Add(time.Minute),
					ReceivedTime:    defaultTime.Add(time.Minute),
				},
				{
					Id:              "2",
					Headline:        "headline2",
					Body:            "body2",
					Tickers:         []string{"ticker2"},
					Ciks:            []int{4, 5, 6},
					Link:            "link2",
					Source:          "SOURCE",
					CategoryCodes:   []string{"categoryCode2, categoryCode22"},
					IndustryCodes:   []string{"industryCode2, industryCode22"},
					RegionCodes:     []string{"regionCode1, regionCode11"},
					PublicationTime: defaultTime,
					ReceivedTime:    defaultTime,
				},
			},
			nil,
		},
		{
			"limit query size",
			[]*News{
				{
					Id:              "1",
					Headline:        "1",
					Body:            generateBody(maxQuerySize),
					PublicationTime: defaultTime.Add(time.Minute),
					ReceivedTime:    defaultTime.Add(time.Minute),
				},
				{
					Id:              "2",
					Headline:        generateBody(maxQuerySize),
					PublicationTime: defaultTime,
				},
			},
			nil,
		},
		{
			"body bigger than max query size",
			[]*News{
				{
					Id:              "1",
					Headline:        "1",
					Body:            generateBody(maxQuerySize * 2),
					PublicationTime: defaultTime.Add(time.Minute),
					ReceivedTime:    defaultTime.Add(time.Minute),
				},
				{
					Id:              "2",
					Headline:        "2",
					Body:            generateBody(maxQuerySize * 2),
					PublicationTime: defaultTime,
				},
			},
			[]*News{
				{
					Id:              "1",
					Headline:        "1",
					Body:            "",
					PublicationTime: defaultTime.Add(time.Minute),
					ReceivedTime:    defaultTime.Add(time.Minute),
				},
				{
					Id:              "2",
					Headline:        "2",
					Body:            "",
					PublicationTime: defaultTime,
				},
			},
		},
		{
			"multiple batches",
			[]*News{
				{
					Id:              "1",
					Headline:        "1",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime.Add(time.Minute * 60),
					ReceivedTime:    defaultTime.Add(time.Minute * 60),
				},
				{
					Id:              "2",
					Headline:        "2",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime.Add(time.Minute * 59),
					ReceivedTime:    defaultTime.Add(time.Minute * 59),
				},
				{
					Id:              "3",
					Headline:        "3",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime.Add(time.Minute * 58),
					ReceivedTime:    defaultTime.Add(time.Minute * 58),
				},
				{
					Id:              "4",
					Headline:        "4",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime.Add(time.Minute * 57),
					ReceivedTime:    defaultTime.Add(time.Minute * 57),
				},
				{
					Id:              "5",
					Headline:        "5",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime.Add(time.Minute * 56),
					ReceivedTime:    defaultTime.Add(time.Minute * 56),
				},
				{
					Id:              "6",
					Headline:        "6",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime,
					ReceivedTime:    defaultTime,
				},
			},
			nil,
		},
	}
	for _, tt := range tests {
		r.Run(tt.name, func() {
			err := r.newsRepository.InsertBatch(tt.insertNews, func(int, int) {})
			if !r.NoError(err) {
				r.FailNow("")
			}

			_, err = r.newsRepository.elastic.TypedClient.Indices.Refresh().Do(context.Background())
			if !r.NoError(err) {
				r.FailNow("")
			}

			actualNews := make([]*News, 0)
			resp, err := r.newsRepository.elastic.TypedClient.
				Search().
				Index(r.newsRepository.Index).
				Request(&search.Request{
					Query: &types.Query{
						MatchAll: &types.MatchAllQuery{
							Boost: float32Ptr(1.2),
						},
					},
					Sort: []types.SortCombinations{publicationTimeSort{sortOrder{Order: "desc"}}},
				}).
				Do(context.Background())
			r.NoError(err)

			for _, hit := range resp.Hits.Hits {
				actualNewsItem := &News{}
				err = json.Unmarshal(hit.Source_, actualNewsItem)
				if err != nil {
					r.FailNow("unmarshalling elastic search response")
				}
				actualNews = append(actualNews, actualNewsItem)
			}

			// If tt.expectedNews is nil, we use the insert args as expected result
			if tt.expectedNews == nil {
				tt.expectedNews = tt.insertNews
			}

			if !r.Equal(len(tt.expectedNews), len(actualNews), "returned wrong number of news") {
				r.FailNow("expected and actuals news lengths don't match")
			}
			for i, actualNewsItem := range actualNews {
				r.Equal(tt.expectedNews[i].Id, resp.Hits.Hits[i].Id_, "wrong document _id")
				r.assertNewsEqual(tt.expectedNews[i], actualNewsItem)
			}
		})
	}
}

func (r *newsRepositorySuite) TestNewsRepository_Insert() {
	maxQuerySize = 1000
	defaultTime := time.Now()

	tests := []struct {
		name         string
		news         *News
		expectedNews *News
	}{
		{
			"insert news",
			&News{
				Id:              "100",
				Headline:        "headline",
				Body:            "body",
				Tickers:         []string{"ticker"},
				Ciks:            []int{1, 2, 3},
				Link:            "link",
				Source:          "SOURCE",
				CategoryCodes:   []string{"categoryCode1, categoryCode2"},
				IndustryCodes:   []string{"industryCode1, industryCode2"},
				RegionCodes:     []string{"regionCode1, regionCode2"},
				PublicationTime: defaultTime.Add(time.Minute),
				ReceivedTime:    defaultTime.Add(time.Minute),
			},
			nil,
		},
	}
	for _, tt := range tests {
		r.Run(tt.name, func() {
			err := r.newsRepository.Insert(tt.news)
			if !r.NoError(err) {
				r.FailNow("")
			}

			_, err = r.newsRepository.elastic.TypedClient.Indices.Refresh().Do(context.Background())
			if !r.NoError(err) {
				r.FailNow("")
			}

			resp, err := r.newsRepository.elastic.TypedClient.
				Search().
				Index(r.newsRepository.Index).
				Request(&search.Request{
					Query: &types.Query{
						MatchAll: &types.MatchAllQuery{
							Boost: float32Ptr(1.2),
						},
					},
					Sort: []types.SortCombinations{publicationTimeSort{sortOrder{Order: "desc"}}},
				}).
				Do(context.Background())
			r.NoError(err)

			if !r.Len(resp.Hits.Hits, 1, "returned wrong number of news") {
				r.FailNow("expected and actuals news lengths don't match")
			}

			actualNews := &News{}
			err = json.Unmarshal(resp.Hits.Hits[0].Source_, actualNews)
			if err != nil {
				r.FailNow("unmarshalling elastic search response")
			}

			// If tt.expectedNews is nil, we use the insert args as expected result
			if tt.expectedNews == nil {
				tt.expectedNews = tt.news
			}

			r.Equal(tt.expectedNews.Id, resp.Hits.Hits[0].Id_, "wrong document _id")

			r.assertNewsEqual(tt.expectedNews, actualNews)
		})
	}
}

func TestNewsRepositorySuite(t *testing.T) {
	integration := os.Getenv("INTEGRATION")
	if integration == "" {
		t.Skip("skipping: set INTEGRATION env to run this test")
	}

	suite.Run(t, new(newsRepositorySuite))
}

func generateBody(size int) string {
	body := ""

	for i := 0; i < size; i++ {
		body += "1"
	}

	return body
}

func float32Ptr(v float32) *float32 { return &v }

func (r *newsRepositorySuite) assertNewsEqual(expected *News, actual *News) {
	r.Equal(expected.Id, actual.Id)
	r.Equal(expected.Headline, actual.Headline)
	r.Equal(expected.Body, actual.Body)
	r.Equal(expected.Tickers, actual.Tickers)
	r.Equal(expected.Ciks, actual.Ciks)
	r.Equal(expected.Link, actual.Link)
	r.Equal(expected.Source, actual.Source)
	r.Equal(expected.CategoryCodes, actual.CategoryCodes)

	// Time is tested within a delta of 1 millisecond since rethinkdb has millisecond precision, and Go microsecond
	r.WithinDuration(expected.PublicationTime, actual.PublicationTime, time.Millisecond)
	r.WithinDuration(expected.ReceivedTime, actual.ReceivedTime, time.Millisecond)
	r.WithinDuration(time.Now(), actual.CreationTime, time.Second*10)
}
