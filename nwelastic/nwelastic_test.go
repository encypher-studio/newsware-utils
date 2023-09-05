package nwelastic

import (
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

var (
	TestElasticConfig = ElasticConfig{
		Addresses:   []string{"https://localhost:9200"},
		Username:    "elastic",
		Password:    "changeme",
		CertPath:    "./ca.crt",
		LogRequests: false,
	}
)

type publicationTimeSort struct {
	PublicationTime sort `json:"publicationTime"`
}

type sort struct {
	Order string `json:"order"`
}

// newsRepositorySuite performs integration tests on database {config.RethinkDb.Database}_integration_test. Database must not exist
// beforehand.
type newsRepositorySuite struct {
	suite.Suite
	config         ElasticConfig
	newsRepository NewsRepository
}

func (r *newsRepositorySuite) SetupSuite() {
	index = "nwelastic_tests"

	err := r.newsRepository.Init(TestElasticConfig)
	if err != nil {
		r.FailNow(err.Error())
	}

	_, _ = r.newsRepository.typedClient.Indices.Delete(index).Do(nil)
}

func (r *newsRepositorySuite) SetupSubTest() {
	_, err := r.newsRepository.typedClient.Indices.Create(index).Do(nil)
	if err != nil {
		r.FailNow(err.Error())
	}
}

func (r *newsRepositorySuite) TearDownSubTest() {
	_, err := r.newsRepository.typedClient.Indices.Delete(index).Do(nil)
	if err != nil {
		r.FailNow(err.Error())
	}
}

func (r *newsRepositorySuite) TestNewsRepository_InsertNews() {
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
					Headline:        "headline",
					Body:            "body",
					Tickers:         []string{"ticker"},
					Ciks:            []int{1, 2, 3},
					Link:            "link",
					Source:          "SOURCE",
					PublicationTime: defaultTime.Add(time.Minute),
					ReceivedTime:    defaultTime.Add(time.Minute),
				},
				{
					Headline:        "headline2",
					Body:            "body2",
					Tickers:         []string{"ticker2"},
					Ciks:            []int{4, 5, 6},
					Link:            "link2",
					Source:          "SOURCE",
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
					Headline:        "1",
					Body:            generateBody(maxQuerySize),
					PublicationTime: defaultTime.Add(time.Minute),
					ReceivedTime:    defaultTime.Add(time.Minute),
				},
				{
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
					Headline:        "1",
					Body:            generateBody(maxQuerySize * 2),
					PublicationTime: defaultTime.Add(time.Minute),
					ReceivedTime:    defaultTime.Add(time.Minute),
				},
				{
					Headline:        "2",
					Body:            generateBody(maxQuerySize * 2),
					PublicationTime: defaultTime,
				},
			},
			[]*News{
				{
					Headline:        "1",
					Body:            "",
					PublicationTime: defaultTime.Add(time.Minute),
					ReceivedTime:    defaultTime.Add(time.Minute),
				},
				{
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
					Headline:        "1",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime.Add(time.Minute * 60),
					ReceivedTime:    defaultTime.Add(time.Minute * 60),
				},
				{
					Headline:        "2",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime.Add(time.Minute * 59),
					ReceivedTime:    defaultTime.Add(time.Minute * 59),
				},
				{
					Headline:        "3",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime.Add(time.Minute * 58),
					ReceivedTime:    defaultTime.Add(time.Minute * 58),
				},
				{
					Headline:        "4",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime.Add(time.Minute * 57),
					ReceivedTime:    defaultTime.Add(time.Minute * 57),
				},
				{
					Headline:        "5",
					Body:            generateBody(maxQuerySize / 2),
					PublicationTime: defaultTime.Add(time.Minute * 56),
					ReceivedTime:    defaultTime.Add(time.Minute * 56),
				},
				{
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
			insertArgs := make([]*News, len(tt.insertNews))
			for i, news := range tt.insertNews {
				v := *news
				insertArgs[i] = &v
			}
			err := r.newsRepository.InsertNews(insertArgs, func(int, int) {})
			if !r.NoError(err) {
				r.FailNow("")
			}

			_, err = r.newsRepository.typedClient.Indices.Refresh().Do(context.Background())
			if !r.NoError(err) {
				r.FailNow("")
			}

			actualNews := make([]*News, 0)
			resp, err := r.newsRepository.typedClient.
				Search().
				Index(index).
				Request(&search.Request{
					Query: &types.Query{
						MatchAll: &types.MatchAllQuery{
							Boost: float32Ptr(1.2),
						},
					},
					Sort: []types.SortCombinations{publicationTimeSort{sort{Order: "desc"}}},
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
				r.Equal(tt.expectedNews[i].Headline, actualNewsItem.Headline)
				r.Equal(tt.expectedNews[i].Body, actualNewsItem.Body)
				r.Equal(tt.expectedNews[i].Tickers, actualNewsItem.Tickers)
				r.Equal(tt.expectedNews[i].Ciks, actualNewsItem.Ciks)
				r.Equal(tt.expectedNews[i].Link, actualNewsItem.Link)
				r.Equal(tt.expectedNews[i].Source, actualNewsItem.Source)

				// Time is tested within a delta of 1 millisecond since rethinkdb has millisecond precision, and Go microsecond
				r.WithinDuration(tt.expectedNews[i].PublicationTime, actualNewsItem.PublicationTime, time.Millisecond)
				r.WithinDuration(tt.expectedNews[i].ReceivedTime, actualNewsItem.ReceivedTime, time.Millisecond)
				r.WithinDuration(time.Now(), actualNewsItem.CreationTime, time.Second*10)
			}
		})
	}
}

func TestNewsRepositorySuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skip test for news repository")
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
