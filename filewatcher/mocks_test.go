package filewatcher

import (
	"context"

	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/nwfs"
)

type mockFs struct {
	sendChanFiles chan nwfs.NewFile
	sendChanErr   chan error
	moveCalls     int
	deleteCalls   int
}

func NewMockFs() *mockFs {
	return &mockFs{
		sendChanFiles: make(chan nwfs.NewFile),
		sendChanErr:   make(chan error),
	}
}

func (m *mockFs) Watch(ctx context.Context, chanFiles chan nwfs.NewFile) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case file := <-m.sendChanFiles:
			chanFiles <- file
		case err := <-m.sendChanErr:
			return err
		}

	}
}
func (m *mockFs) Delete(file nwfs.NewFile) error {
	m.deleteCalls++
	return nil
}
func (m *mockFs) Unprocessable(file nwfs.NewFile) error {
	m.moveCalls++
	return nil
}

type mockIndexer struct {
	indexCalls   int
	rets         []error
	argIndexNews *nwelastic.News
}

func (m *mockIndexer) Index(news *nwelastic.News) error {
	m.argIndexNews = news
	m.indexCalls++
	if m.indexCalls > len(m.rets) {
		return nil
	}
	return m.rets[m.indexCalls-1]
}
