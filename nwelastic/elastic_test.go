package nwelastic

import (
	"encoding/json"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var (
	defaultTime = time.Now()
)

func TestElastic_StartClient_failPing(t *testing.T) {
	conf := TestElasticConfig
	conf.Addresses = []string{"http://localhost:80"}
	e := &Elastic{
		Config: conf,
	}

	err := e.StartClient()
	assert.Contains(t, err.Error(), ErrFailedToPingElastic.Error())
}

func TestElastic_StartTypedClient_failPing(t *testing.T) {
	conf := TestElasticConfig
	conf.Addresses = []string{"http://localhost:80"}
	e := &Elastic{
		Config: conf,
	}

	err := e.StartTypedClient()
	assert.Contains(t, err.Error(), ErrFailedToPingElastic.Error())
}

// nwElasticSuite performs integration tests, they are run unless the test -short flag is set
type nwElasticSuite struct {
	suite.Suite
	config         ElasticConfig
	newsRepository NewsRepository
	elastic        Elastic
}

func (n *nwElasticSuite) SetupSuite() {
	var err error
	elasticConfig := TestElasticConfig
	elasticConfig.NewsIndex = strconv.Itoa(rand.Int())
	n.elastic = NewElastic(elasticConfig)
	n.elastic.StartTypedClient()
	n.newsRepository, err = NewNewsRepository(n.elastic, "sequence_test")
	if err != nil {
		n.FailNow(err.Error())
	}

	_, _ = n.newsRepository.elastic.typedClient.Indices.Delete(n.newsRepository.Index).Do(nil)
}

func (n *nwElasticSuite) SetupSubTest() {
	_, err := n.newsRepository.elastic.typedClient.Indices.Create(n.newsRepository.Index).Do(nil)
	if err != nil {
		n.FailNow(err.Error())
	}
}

func (n *nwElasticSuite) TearDownSubTest() {
	_, err := n.newsRepository.elastic.typedClient.Indices.Delete(n.newsRepository.Index).Do(nil)
	if err != nil {
		n.FailNow(err.Error())
	}
}

func (n *nwElasticSuite) TestGet() {
	expectedHeadline := "headline"
	news := News{
		Id:       "1",
		Headline: expectedHeadline,
	}

	err := n.newsRepository.Insert(&news)
	if !n.NoError(err) {
		n.FailNow("")
	}

	res, err := n.elastic.Get(n.newsRepository.Index, news.Id).Do(nil)
	if !n.NoError(err) {
		n.FailNow("")
	}

	var actualNews News
	err = json.Unmarshal(res.Source_, &actualNews)
	if !n.NoError(err) {
		n.FailNow("")
	}

	n.Equal(expectedHeadline, actualNews.Headline)
}

func TestNwElasticSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skip integration tests for nwelastic")
	}
	suite.Run(t, new(nwElasticSuite))
}
