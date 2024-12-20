package filewatcher

import (
	"context"
	"errors"
	"fmt"

	"github.com/encypher-studio/newsware-utils/ecslogger"
	"github.com/encypher-studio/newsware-utils/indexer"
	"github.com/encypher-studio/newsware-utils/indexmetrics"
	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/nwfs"
	"go.uber.org/zap"
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
	logger    ecslogger.ILogger
	parseFunc ParseFunc
}

// New creates a new Fly instance.
func New(fsConfig nwfs.Config, indexer indexer.Indexer, parseFunc ParseFunc, logger ecslogger.ILogger) (FileWatcher, error) {
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
			f.logger.Error("watching for new files", err)
			cancel()
		}
	}()

	for {
		select {
		case newFile := <-chanFiles:
			f.logger.Info("file received for processing", zap.String("path", newFile.Path))
			f.logger.Debug("file received", zap.String("path", newFile.Path), zap.String("data", string(newFile.Bytes)))
			// Process asynchronously
			go func() {
				news, err := f.parseFunc(newFile)
				if err != nil {
					if errors.Is(err, ErrIgnorableNews) {
						f.logger.Info("ignorable news", zap.String("path", newFile.Path))
						err = f.fs.Delete(newFile)
						if err != nil {
							f.logger.Error("deleting ignorable file", err, zap.String("path", newFile.Path))
						} else {
							f.logger.Info("file deleted", zap.String("path", newFile.Path))
						}
						return
					}

					// Move file to unprocessable directory
					f.logger.Error("parsing news", err, zap.String("path", newFile.Path))
					err = f.fs.Unprocessable(newFile)
					if err != nil {
						f.logger.Error("moving file to unprocessable directory", err, zap.String("path", newFile.Path))
					}
					return
				}

				news.ReceivedTime = newFile.ReceivedTime

				err = f.indexer.Index(&news)
				if err != nil {
					// Send file again to the channel, so it can be processed again
					f.logger.Error("indexing news", err, zap.String("path", newFile.Path))
					chanFiles <- newFile
					return
				}

				f.logger.Info("file indexed", zap.String("path", newFile.Path))

				err = f.fs.Delete(newFile)
				if err != nil {
					f.logger.Error("deleting indexed file", err, zap.String("path", newFile.Path))
				} else {
					f.logger.Info("file deleted", zap.String("path", newFile.Path))
				}

				indexmetrics.MetricDocumentsIndexed.WithLabelValues().Inc()
			}()
		case <-ctx.Done():
			return
		}
	}
}
