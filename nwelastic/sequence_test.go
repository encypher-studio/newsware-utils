package nwelastic

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

// newsRepositorySuite performs integration tests, they are run unless the test -short flag is set
type sequenceSuite struct {
	suite.Suite
	config   ElasticConfig
	sequence Sequence
}

func (s *sequenceSuite) SetupSuite() {
	err := s.sequence.Init(&Elastic{Config: TestElasticConfig}, "index_to_sequence")
	if err != nil {
		s.FailNow(err.Error())
	}

	err = s.sequence.elastic.StartTypedClient()
	if err != nil {
		s.FailNow(err.Error())
	}

	_, _ = s.sequence.elastic.typedClient.Indices.Delete("sequence").Do(nil)
}

func (s *sequenceSuite) SetupSubTest() {
	_, err := s.sequence.elastic.typedClient.Indices.Create("sequence").Do(nil)
	if err != nil {
		s.FailNow(err.Error())
	}
}

func (s *sequenceSuite) TearDownSubTest() {
	_, err := s.sequence.elastic.typedClient.Indices.Delete("sequence").Do(nil)
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

func TestSequenceSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skip test for Sequence")
	}
	suite.Run(t, new(sequenceSuite))
}
