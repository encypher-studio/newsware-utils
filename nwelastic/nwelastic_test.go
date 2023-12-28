package nwelastic

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"strconv"
	"testing"
	"time"
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
	elastic        *Elastic
}

func (n *nwElasticSuite) SetupSuite() {
	testIndex := strconv.Itoa(rand.Int())
	n.newsRepository.Index = testIndex
	err := n.newsRepository.Init(&Elastic{Config: TestElasticConfig})
	if err != nil {
		n.FailNow(err.Error())
	}

	n.elastic = n.newsRepository.elastic

	_, _ = n.newsRepository.elastic.typedClient.Indices.Delete(n.newsRepository.Index).Do(nil)
	_, _ = n.newsRepository.elastic.typedClient.Delete(n.newsRepository.sequence.sequenceIndex, n.newsRepository.Index).Do(nil)
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
	_, err = n.newsRepository.elastic.typedClient.Delete(n.newsRepository.sequence.sequenceIndex, n.newsRepository.Index).Do(nil)
	if err != nil {
		n.FailNow(err.Error())
	}
}

func (n *nwElasticSuite) TestGet() {
	expectedHeadline := "headline"
	news := News{
		Headline: expectedHeadline,
	}

	err := n.newsRepository.Insert(&news)
	if !n.NoError(err) {
		n.FailNow("")
	}

	res, err := n.elastic.Get(n.newsRepository.Index, strconv.FormatInt(news.Id, 10)).Do(nil)
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
