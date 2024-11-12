package nwfs

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
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
	dir        string
	logger     ecslogger.ILogger
	ignoreDirs []string
}

// NewFs creates a new Fs instance.
func NewFs(dir string, ignoreDirs []string, logger ecslogger.ILogger) Fs {
	ignoreDirs = append(ignoreDirs, "unprocessable")
	ignoreDirs = append(ignoreDirs, "redirect")

	for i, dir := range ignoreDirs {
		ignoreDirs[i] = filepath.Clean(dir)
	}

	return Fs{dir: dir, ignoreDirs: ignoreDirs, logger: logger}
}

// Watch watches the directory for top and nested new files and sends them to the channel, it also processes existing files.
// Files are uploaded after 100ms without a WRITE or CREATE event.
func (f Fs) Watch(ctx context.Context, chanFiles chan NewFile) error {
	// Process existing files
	dirs, err := findValidDirs(f.dir, f.ignoreDirs)
	if err != nil {
		return err
	}

	err = f.processExistingFiles(dirs, chanFiles)
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

	for _, dir := range dirs {
		err = fsWatcher.Add(dir)
		if err != nil {
			return fmt.Errorf("adding directory to watch list: %w", err)
		}
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
						f.logger.Info("directory detected, adding to watch list", zap.String("name", event.Name))
						err := fsWatcher.Add(event.Name)
						if err != nil {
							f.logger.Error("adding directory to watch list", err, zap.String("name", event.Name))
							fsWatcher.Events <- event
						}

						// Add nested directories created after the parent directory
						// If the directory is the root directory, ignore all directories in the ignoreDirs list
						ignoreDirs := []string{}
						if filepath.Dir(event.Name) == filepath.Clean(f.dir) {
							ignoreDirs = f.ignoreDirs
						}
						dirs, err := findValidDirs(event.Name, ignoreDirs)
						if err != nil {
							f.logger.Error("finding valid directories", err, zap.String("name", event.Name))
							fsWatcher.Events <- event
							return
						}

						for _, dir := range dirs {
							err = fsWatcher.Add(dir)
							if err != nil {
								f.logger.Error("adding directory to watch list", err, zap.String("name", dir))
								fsWatcher.Events <- event
							}
						}

						// Process any files uploaded while the directory was being added
						f.processExistingFiles(dirs, chanFiles)
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

func (f Fs) processExistingFiles(dirs []string, chanFiles chan NewFile) error {
	for _, dir := range dirs {
		f.logger.Info("looking for files", zap.String("dir", dir))
		files, err := os.ReadDir(dir)
		if err != nil {
			return err
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			filePath := filepath.Join(dir, file.Name())
			relativePath := strings.Trim(filePath, f.dir)

			f.logger.Info("file found", zap.String("file", filePath))

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

func findValidDirs(path string, ignoreDirs []string) ([]string, error) {
	var dirs []string
	firstRun := true
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		defer func() {
			firstRun = false
		}()

		if !info.IsDir() {
			return nil
		}

		if slices.Contains(ignoreDirs, filepath.Base(path)) {
			return nil
		}

		// First run is always the path itself
		if firstRun {
			dirs = append(dirs, path)
			return nil
		}

		if err != nil {
			return err
		}

		nestedDirs, err := findValidDirs(path, []string{})
		if err != nil {
			return err
		}

		dirs = append(dirs, nestedDirs...)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return dirs, nil
}

// Delete deletes a file from the directory
func (f Fs) Delete(file NewFile) error {
	return os.Remove(file.Path)
}

// Unprocessable moves a file to unprocessable directory
func (f Fs) Unprocessable(file NewFile) error {
	return os.Rename(file.Path, path.Join(f.dir, "unprocessable", file.RelativePath))
}
