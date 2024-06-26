package filewatcher

import (
	"errors"
	"testing"
	"time"

	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/nwfs"
)

func TestFileWatcher_Run(t *testing.T) {
	type runRets struct {
		sendWatchErr bool
		retParse     error
		retIndex     error
	}
	tests := []struct {
		name        string
		rets        []runRets
		parseCalls  int
		moveCalls   int
		deleteCalls int
		indexCalls  int
	}{
		{
			name: "should index",
			rets: []runRets{
				{},
				{},
			},
			parseCalls:  2,
			indexCalls:  2,
			deleteCalls: 2,
		},
		{
			name: "should index and move",
			rets: []runRets{
				{},
				{retParse: errors.New("error")},
			},
			parseCalls:  2,
			moveCalls:   1,
			indexCalls:  1,
			deleteCalls: 1,
		},
		{
			name: "should index and fail watch",
			rets: []runRets{
				{},
				{sendWatchErr: true},
			},
			parseCalls:  1,
			indexCalls:  1,
			deleteCalls: 1,
		},
		{
			name: "should index and fail index",
			rets: []runRets{
				{},
				{retIndex: errors.New("error")},
			},
			parseCalls:  3,
			indexCalls:  3,
			deleteCalls: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseCalls := 0

			f := FileWatcher{
				fs: NewMockFs(),
				indexer: &mockIndexer{
					rets: make([]error, len(tt.rets)),
				},
				logger: mockLogger{},
			}

			go f.Run()

			for i, ret := range tt.rets {
				f.indexer.(*mockIndexer).rets[i] = ret.retIndex
				chanParseCalled := make(chan struct{}, 100)
				f.parseFunc = func(nwfs.NewFile) (nwelastic.News, error) {
					parseCalls++
					chanParseCalled <- struct{}{}
					return nwelastic.News{}, ret.retParse
				}

				if ret.sendWatchErr {
					f.fs.(*mockFs).sendChanErr <- errors.New("error")
				} else {
					f.fs.(*mockFs).sendChanFiles <- nwfs.NewFile{}
					for {
						select {
						case <-chanParseCalled:
							goto breakFor
						case <-time.After(200 * time.Millisecond):
							t.Fatal("timeout waiting for parse to be called")
						}
					}
				}
			breakFor:
			}

			// Wait for the goroutine to finish
			time.Sleep(50 * time.Millisecond)

			if f.fs.(*mockFs).moveCalls != tt.moveCalls {
				t.Fatalf("Run() moveCalls = %v, expected %v", f.fs.(*mockFs).moveCalls, tt.moveCalls)
			}
			if f.fs.(*mockFs).deleteCalls != tt.deleteCalls {
				t.Fatalf("Run() deleteCalls = %v, expected %v", f.fs.(*mockFs).deleteCalls, tt.deleteCalls)
			}
			if f.indexer.(*mockIndexer).indexCalls != tt.indexCalls {
				t.Fatalf("Run() indexCalls = %v, expected %v", f.indexer.(*mockIndexer).indexCalls, tt.indexCalls)
			}
			if parseCalls != tt.parseCalls {
				t.Fatalf("Run() parseCalls = %v, expected %v", parseCalls, tt.parseCalls)
			}
		})
	}
}
