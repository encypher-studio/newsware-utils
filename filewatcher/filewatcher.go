package filewatcher

import (
	"context"
	"errors"
	"fmt"

	"github.com/encypher-studio/newsware-utils/indexer"
	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/nwfs"
	"github.com/rs/zerolog"
)

var (
	ErrIgnorableNews = fmt.Errorf("ignorable news")
)

type ParseFunc func(newFile nwfs.NewFile) (nwelastic.News, error)

type IIndexer interface {
	Index(news *nwelastic.News) error
}

// FileWatcher watches for new files in a directory, parses them using parseFunc and indexes them using indexer. If PreIndexProcessor is set, it is called before indexing.
type FileWatcher struct {
	fs        nwfs.IFs
	indexer   IIndexer
	logger    zerolog.Logger
	parseFunc ParseFunc
}

// New creates a new Fly instance.
func New(fsConfig nwfs.Config, indexer indexer.Indexer, parseFunc ParseFunc, logger zerolog.Logger) (FileWatcher, error) {
	fs, err := nwfs.NewFs(fsConfig, logger)
	if err != nil {
		return FileWatcher{}, err
	}
	return FileWatcher{fs: fs, indexer: indexer, logger: logger, parseFunc: parseFunc}, nil
}

// Run starts the FileWatcher instance.
func (f *FileWatcher) Run() {
	chanFiles := make(chan nwfs.NewFile, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := f.fs.Watch(ctx, chanFiles)
		if err != nil {
			f.logger.Error().Err(err).Msg("watching for new files")
			cancel()
		}
	}()

	for {
		select {
		case newFile := <-chanFiles:
			f.logger.Info().Str("path", newFile.Path).Msg("file received for processing")
			f.logger.Debug().Str("path", newFile.Path).Str("data", string(newFile.Bytes)).Msg("file received")
			// Process asynchronously
			go func() {
				news, err := f.parseFunc(newFile)
				if err != nil {
					if errors.Is(err, ErrIgnorableNews) {
						f.logger.Info().Str("path", newFile.Path).Msg("ignorable news")
						err = f.fs.Delete(newFile)
						if err != nil {
							f.logger.Error().Err(err).Str("path", newFile.Path).Msg("deleting ignorable file")
						} else {
							f.logger.Info().Str("path", newFile.Path).Msg("file deleted")
						}
						return
					}

					// Move file to unprocessable directory
					f.logger.Error().Err(err).Str("path", newFile.Path).Msg("parsing news")
					err = f.fs.Unprocessable(newFile)
					if err != nil {
						f.logger.Error().Err(err).Str("path", newFile.Path).Msg("moving file to unprocessable directory")
					}
					return
				}

				news.ReceivedTime = newFile.ReceivedTime

				err = f.indexer.Index(&news)
				if err != nil {
					// Send file again to the channel, so it can be processed again
					f.logger.Error().Err(err).Str("path", newFile.Path).Msg("indexing news")
					chanFiles <- newFile
					return
				}

				f.logger.Info().Str("path", newFile.Path).Msg("file indexed")

				err = f.fs.Delete(newFile)
				if err != nil {
					f.logger.Error().Err(err).Str("path", newFile.Path).Msg("deleting indexed file")
				} else {
					f.logger.Info().Str("path", newFile.Path).Msg("file deleted")
				}
			}()
		case <-ctx.Done():
			return
		}
	}
}
