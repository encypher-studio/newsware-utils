package flyonthewall

import (
	"context"
	"os"
	"path"
	"time"

	"github.com/encypher-studio/newsware-utils/ecslogger"
	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

type NewFile struct {
	Name         string
	Bytes        []byte
	ReceivedTime time.Time
}

type iFs interface {
	Watch(ctx context.Context, chanFiles chan NewFile) error
	Delete(file string) error
	Move(file string) error
}

type Fs struct {
	dir    string
	logger ecslogger.ILogger
}

// NewFs creates a new Fs instance.
func NewFs(dir string, logger ecslogger.ILogger) Fs {
	return Fs{dir: dir, logger: logger}
}

// Watch watches the directory for new files and sends them to the channel
func (f Fs) Watch(ctx context.Context, chanFiles chan NewFile) error {
	// Find any existing files
	files, err := os.ReadDir(f.dir)
	if err != nil {
		return err
	}

	f.logger.Info("existing files", zap.Int("count", len(files)))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		f.logger.Info("new file found", zap.String("file", file.Name()))
		bytes, err := os.ReadFile(path.Join(f.dir, file.Name()))
		if err != nil {
			return err
		}

		info, err := file.Info()
		if err != nil {
			return err
		}

		chanFiles <- NewFile{Name: file.Name(), Bytes: bytes, ReceivedTime: info.ModTime().UTC()}
	}

	// Watch for new files
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer fsWatcher.Close()

	err = fsWatcher.Add(f.dir)
	if err != nil {
		return err
	}

	for {
		select {
		case event := <-fsWatcher.Events:
			if event.Has(fsnotify.Create) {
				filename := path.Base(event.Name)
				f.logger.Info("new file detected", zap.String("file", filename))
				bytes, err := os.ReadFile(path.Join(f.dir, filename))
				if err != nil {
					return err
				}

				chanFiles <- NewFile{Name: filename, Bytes: bytes, ReceivedTime: time.Now().UTC()}
			}
		case err, ok := <-fsWatcher.Errors:
			if !ok {
				return err
			}
			return err
		case <-ctx.Done():
			return nil
		}
	}
}

// Delete deletes a file from the directory
func (f Fs) Delete(file string) error {
	return os.Remove(path.Join(f.dir, file))
}

// Unprocessable moves a file to unprocessable directory
func (f Fs) Unprocessable(file string) error {
	return os.Rename(path.Join(f.dir, file), path.Join(f.dir, "unprocessable", file))
}
