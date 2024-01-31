package nwelastic

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

// newsRepositorySuite performs integration tests, they are run unless the test -short flag is set
type sequenceSuite struct {
	suite.Suite
	config   ElasticConfig
	sequence Sequence
}

func (s *sequenceSuite) SetupSuite() {
	var err error
	elastic := NewElastic(TestElasticConfig)
	s.sequence, err = NewSequence(elastic, "index_test", "sequence_index_test")

	err = s.sequence.elastic.StartTypedClient()
	if err != nil {
		s.FailNow(err.Error())
	}

	_, _ = s.sequence.elastic.typedClient.Indices.Delete(s.sequence.sequenceIndex).Do(nil)
}

func (s *sequenceSuite) BeforeTest(_, _ string) {
	_, err := s.sequence.elastic.typedClient.Indices.Create(s.sequence.sequenceIndex).Do(nil)
	if err != nil {
		s.FailNow(err.Error())
	}
}

func (s *sequenceSuite) AfterTest(_, _ string) {
	_, err := s.sequence.elastic.typedClient.Indices.Delete(s.sequence.sequenceIndex).Do(nil)
	if err != nil {
		s.FailNow(err.Error())
	}
}

func (s *sequenceSuite) TestSequence_GenerateUniqueIds() {
	ids, err := s.sequence.GenerateUniqueIds(100)
	if err != nil {
		s.FailNow(err.Error())
	}

	s.Equal(100, len(ids), "length of ids is not as expected")
	for i := int64(1); i <= 100; i++ {
		if !s.Equal(i, ids[i-1]) {
			s.FailNow("returned ids are not as expected")
		}
	}

	ids, err = s.sequence.GenerateUniqueIds(500)
	if err != nil {
		s.FailNow(err.Error())
	}

	s.Equal(500, len(ids), "length of ids is not as expected")
	for i := int64(101); i <= 600; i++ {
		if !s.Equal(i, ids[i-101]) {
			s.FailNow("returned ids are not as expected")
		}
	}
}

func (s *sequenceSuite) TestSequence_GetLastId() {
	_, err := s.sequence.GenerateUniqueIds(100)
	if err != nil {
		s.FailNow(err.Error())
	}

	_, err = s.sequence.elastic.typedClient.Indices.Refresh().Index(s.sequence.sequenceIndex).Do(context.Background())
	if err != nil {
		s.FailNow(err.Error())
	}

	lastId, err := s.sequence.GetLastId()
	if err != nil {
		s.FailNow(err.Error())
	}

	s.Equal(int64(100), lastId)

	_, err = s.sequence.GenerateUniqueIds(500)
	if err != nil {
		s.FailNow(err.Error())
	}

	_, err = s.sequence.elastic.typedClient.Indices.Refresh().Index(s.sequence.sequenceIndex).Do(context.Background())
	if err != nil {
		s.FailNow(err.Error())
	}

	lastId, err = s.sequence.GetLastId()
	if err != nil {
		s.FailNow(err.Error())
	}

	s.Equal(int64(600), lastId)
}

func TestSequenceSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skip test for Sequence")
	}
	suite.Run(t, new(sequenceSuite))
}
