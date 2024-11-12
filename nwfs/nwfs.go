package nwfs

import (
	"context"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/encypher-studio/newsware-utils/ecslogger"
	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

type NewFile struct {
	Name         string
	Path         string
	RelativePath string
	Bytes        []byte
	ReceivedTime time.Time
}

type IFs interface {
	Watch(ctx context.Context, chanFiles chan NewFile) error
	Delete(file NewFile) error
	Unprocessable(file NewFile) error
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
	err := f.processExistingFiles(f.dir, chanFiles)
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

	f.logger.Info("watching directory", zap.String("dir", f.dir))
	err = fsWatcher.Add(f.dir)
	if err != nil {
		return err
	}

	for {
		select {
		case event, ok := <-fsWatcher.Events:
			if !ok {
				f.logger.Info("fsWatcher.Events channel closed")
				return nil
			}

			f.logger.Info("fsWatcher event", zap.String("event", event.String()), zap.String("file", event.Name))

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

					info, err := os.Stat(event.Name)
					if err != nil {
						f.logger.Error("getting file info", err, zap.String("file", event.Name))
						fsWatcher.Events <- event
						return
					}

					if info.IsDir() {
						f.logger.Info("directory detected, skipping", zap.String("name", event.Name))
						return
					}

					f.logger.Info("new file detected", zap.String("file", event.Name))

					bytes, err := os.ReadFile(event.Name)
					if err != nil {
						fsWatcher.Events <- event
						return
					}

					chanFiles <- NewFile{
						Name:         filepath.Base(event.Name),
						Path:         event.Name,
						RelativePath: strings.Trim(event.Name, f.dir),
						Bytes:        bytes,
						ReceivedTime: info.ModTime().UTC(),
					}
				})
				t.Stop()

				mu.Lock()
				timers[event.Name] = t
				mu.Unlock()
			}

			// Start timer
			t.Reset(time.Millisecond * 500)
		case err, ok := <-fsWatcher.Errors:
			if !ok {
				return nil
			}
			return err
		}
	}
}

func (f Fs) processExistingFiles(path string, chanFiles chan NewFile) error {
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	for _, file := range files {
		filePath := filepath.Join(path, file.Name())
		relativePath := strings.Trim(filePath, f.dir)

		if file.IsDir() {
			f.processExistingFiles(filePath, chanFiles)
		} else {
			f.logger.Info("new file found", zap.String("file", filePath))

			bytes, err := os.ReadFile(filePath)
			if err != nil {

				return err
			}

			info, err := file.Info()
			if err != nil {
				return err
			}

			chanFiles <- NewFile{
				Name:         file.Name(),
				Path:         filePath,
				RelativePath: relativePath,
				Bytes:        bytes,
				ReceivedTime: info.ModTime().UTC(),
			}
		}
	}

	return nil
}

// Delete deletes a file from the directory
func (f Fs) Delete(file NewFile) error {
	return os.Remove(file.Path)
}

// Unprocessable moves a file to unprocessable directory
func (f Fs) Unprocessable(file NewFile) error {
	return os.Rename(file.Path, path.Join(f.dir, "unprocessable", file.RelativePath))
}
