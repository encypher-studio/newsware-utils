package filewatcher

import (
	"context"

	"github.com/encypher-studio/newsware-utils/ecslogger"
	"github.com/encypher-studio/newsware-utils/indexer"
	"github.com/encypher-studio/newsware-utils/indexmetrics"
	"github.com/encypher-studio/newsware-utils/nwelastic"
	"github.com/encypher-studio/newsware-utils/nwfs"
	"go.uber.org/zap"
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
func New(watchDir string, indexer indexer.Indexer, parseFunc ParseFunc, logger ecslogger.ILogger) FileWatcher {
	fs := nwfs.NewFs(watchDir, logger)
	return FileWatcher{fs: fs, indexer: indexer, logger: logger, parseFunc: parseFunc}
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
			f.logger.Info("file received for processing", zap.String("file", newFile.Name))
			f.logger.Debug("file received", zap.String("file", newFile.Name), zap.String("data", string(newFile.Bytes)))
			// Process asynchronously
			go func() {
				news, err := f.parseFunc(newFile)
				if err != nil {
					// Move file to unprocessable directory
					f.logger.Error("parsing news", err, zap.String("file", newFile.Name))
					err = f.fs.Unprocessable(newFile.Name)
					if err != nil {
						f.logger.Error("moving file to unprocessable directory", err, zap.String("file", newFile.Name))
					}
					return
				}

				news.ReceivedTime = newFile.ReceivedTime

				err = f.indexer.Index(&news)
				if err != nil {
					// Send file again to the channel, so it can be processed again
					f.logger.Error("indexing news", err, zap.String("file", newFile.Name))
					chanFiles <- newFile
					return
				}

				f.logger.Info("file indexed", zap.String("file", newFile.Name))

				err = f.fs.Delete(newFile.Name)
				if err != nil {
					f.logger.Error("deleting indexed file", err, zap.String("file", newFile.Name))
				} else {
					f.logger.Info("file deleted", zap.String("file", newFile.Name))
				}

				indexmetrics.MetricDocumentsIndexed.WithLabelValues().Inc()
			}()
		case <-ctx.Done():
			return
		}
	}
}
