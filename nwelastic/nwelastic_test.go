package nwelastic

import (
	"github.com/stretchr/testify/assert"
	"testing"
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

	err := e.StartClient()
	assert.Contains(t, err.Error(), ErrFailedToPingElastic.Error())
}
