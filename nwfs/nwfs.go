package nwfs

import (
	"context"
	"math"
	"os"
	"path"
	"sync"
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

type IFs interface {
	Watch(ctx context.Context, chanFiles chan NewFile) error
	Delete(file string) error
	Unprocessable(file string) error
}

type Fs struct {
	dir    string
	logger ecslogger.ILogger
}

// NewFs creates a new Fs instance.
func NewFs(dir string, logger ecslogger.ILogger) Fs {
	return Fs{dir: dir, logger: logger}
}

// Watch watches the directory for new files and sends them to the channel, it also processes existing files.
// Files are uploaded after 100ms without a WRITE or CREATE event.
func (f Fs) Watch(ctx context.Context, chanFiles chan NewFile) error {
	// Process existing files
	err := f.processExistingFiles(chanFiles)
	if err != nil {
		return err
	}

	// Watch for new files
	var mu sync.Mutex
	timers := make(map[string]*time.Timer)

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
		case event, ok := <-fsWatcher.Events:
			if !ok {
				return nil
			}

			if !event.Has(fsnotify.Create) && !event.Has(fsnotify.Write) {
				continue
			}

			// Get timer for this file
			mu.Lock()
			t, ok := timers[event.Name]
			mu.Unlock()

			// Create timer if doesn't exist
			if !ok {
				t = time.AfterFunc(math.MaxInt64, func() {
					defer func() {
						mu.Lock()
						delete(timers, event.Name)
						mu.Unlock()
					}()

					filename := path.Base(event.Name)
					f.logger.Info("new file detected", zap.String("file", filename))
					bytes, err := os.ReadFile(path.Join(f.dir, filename))
					if err != nil {
						fsWatcher.Events <- event
						return
					}

					chanFiles <- NewFile{Name: filename, Bytes: bytes, ReceivedTime: time.Now().UTC()}
				})
				t.Stop()

				mu.Lock()
				timers[event.Name] = t
				mu.Unlock()
			}

			// Start timer
			t.Reset(time.Millisecond * 200)
		case err, ok := <-fsWatcher.Errors:
			if !ok {
				return nil
			}
			return err
		}
	}
}

func (f Fs) processExistingFiles(chanFiles chan NewFile) error {
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

	return nil
}

// Delete deletes a file from the directory
func (f Fs) Delete(file string) error {
	return os.Remove(path.Join(f.dir, file))
}

// Unprocessable moves a file to unprocessable directory
func (f Fs) Unprocessable(file string) error {
	return os.Rename(path.Join(f.dir, file), path.Join(f.dir, "unprocessable", file))
}
