package nwfs

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/encypher-studio/newsware-utils/ecslogger"
	"github.com/said1296/fsnotify"
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
	Config
	logger       ecslogger.ILogger
	eventRetries map[string]int
}

type Config struct {
	Dir                string
	IgnoreDirs         []string
	SkipReadingContent bool
}

// NewFs creates a new Fs instance.
func NewFs(config Config, logger ecslogger.ILogger) Fs {
	config.IgnoreDirs = append(config.IgnoreDirs, "unprocessable")
	config.IgnoreDirs = append(config.IgnoreDirs, "redirect")

	for i, dir := range config.IgnoreDirs {
		config.IgnoreDirs[i] = filepath.Clean(dir)
	}

	return Fs{
		Config:       config,
		logger:       logger,
		eventRetries: make(map[string]int),
	}
}

// Watch watches the directory for top and nested new files and sends them to the channel, it also processes existing files.
// Files are uploaded after 100ms without a WRITE or CREATE event.
func (f Fs) Watch(ctx context.Context, chanFiles chan NewFile) error {
	// Process existing files
	dirs, err := findValidDirs(f.Dir, f.IgnoreDirs)
	if err != nil {
		return err
	}

	err = f.processExistingFiles(dirs, chanFiles)
	if err != nil {
		return err
	}

	// Watch for new files
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer fsWatcher.Close()

	for _, dir := range dirs {
		err = fsWatcher.AddWith(dir, fsnotify.WithOps(fsnotify.UnportableCloseWrite|fsnotify.Create))
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

			f.eventRetries[event.Name]++
			if f.eventRetries[event.Name] > 10 {
				f.logger.Error("event retry limit reached", nil, zap.String("name", event.Name))
				continue
			}

			f.logger.Debug("event received", zap.String("name", event.Name), zap.String("event", event.String()))

			info, err := os.Stat(event.Name)
			if err != nil {
				f.logger.Error("getting file info", err, zap.String("name", event.Name))
				fsWatcher.Events <- event
				continue
			}

			if event.Op == fsnotify.Create {
				if !info.IsDir() {
					continue
				}

				f.logger.Info("new directory detected", zap.String("name", event.Name))

				// Add nested directories created after the parent directory
				// If the directory is the root directory, ignore all directories in the ignoreDirs list
				ignoreDirs := []string{}
				if filepath.Dir(event.Name) == filepath.Clean(f.Dir) {
					ignoreDirs = f.IgnoreDirs
				}
				dirs, err := findValidDirs(event.Name, ignoreDirs)
				if err != nil {
					f.logger.Error("finding valid directories", err, zap.String("name", event.Name))
					fsWatcher.Events <- event
					continue
				}

				for _, dir := range dirs {
					f.logger.Info("adding directory to watch list", zap.String("name", dir))
					err = fsWatcher.Add(dir)
					if err != nil {
						f.logger.Error("adding directory to watch list", err, zap.String("name", dir))
						fsWatcher.Events <- event
					}
				}

				// Process any files uploaded while the directory was being added
				err = f.processExistingFiles(dirs, chanFiles)
				if err != nil {
					f.logger.Error("processing existing files in new directory", err, zap.String("name", event.Name))
				}

				continue
			}

			f.logger.Info("new file detected", zap.String("file", event.Name))

			var bytes []byte
			if !f.SkipReadingContent {
				bytes, err = os.ReadFile(event.Name)
				if err != nil {
					f.logger.Error("reading file content", err, zap.String("name", event.Name))
					fsWatcher.Events <- event
					continue
				}
			}

			chanFiles <- NewFile{
				Name:         filepath.Base(event.Name),
				Path:         event.Name,
				RelativePath: strings.Trim(event.Name, f.Dir),
				Bytes:        bytes,
				ReceivedTime: info.ModTime().UTC(),
			}
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
			relativePath := strings.Trim(filePath, f.Dir)

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
	return os.Rename(file.Path, path.Join(f.Dir, "unprocessable", file.RelativePath))
}
