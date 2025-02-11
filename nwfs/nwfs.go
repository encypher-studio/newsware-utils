package nwfs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/encypher-studio/newsware-utils/ecslogger"
	"github.com/said1296/fsnotify"
	"go.uber.org/zap"
)

var (
	ErrWatchDirMissing = fmt.Errorf("watch directory missing in config")
)

const (
	opsFilter fsnotify.Op = fsnotify.UnportableCloseWrite | fsnotify.Create | fsnotify.Rename | fsnotify.Write
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
	fileModificationTimeout time.Duration
	logger                  ecslogger.ILogger
	eventRetries            map[string]int
	ignoreFiles             []*regexp.Regexp
	fileModificationTimers  map[string]*time.Timer
	fileModificationMutex   *sync.RWMutex
}

type Config struct {
	Dir                string   `yaml:"dir"`
	IgnoreDirs         []string `yaml:"ignoreDirs"`
	SkipReadingContent bool     `yaml:"skipReadingContent"`
	IgnoreFiles        []string `yaml:"ignoreFiles"`
}

func (c Config) validate() error {
	if c.Dir == "" {
		return ErrWatchDirMissing
	}

	return nil
}

// NewFs creates a new Fs instance.
func NewFs(config Config, logger ecslogger.ILogger) (Fs, error) {
	err := config.validate()
	if err != nil {
		return Fs{}, err
	}

	config.IgnoreDirs = append(config.IgnoreDirs, "unprocessable")
	config.IgnoreDirs = append(config.IgnoreDirs, "redirect")

	for i, dir := range config.IgnoreDirs {
		config.IgnoreDirs[i] = filepath.Clean(dir)
	}

	config.Dir = filepath.Clean(config.Dir)

	var ignoreFiles []*regexp.Regexp
	for _, ignoreFile := range config.IgnoreFiles {
		re, err := regexp.Compile(ignoreFile)
		if err != nil {
			return Fs{}, fmt.Errorf("compiling ignore file regex: %w", err)
		}
		ignoreFiles = append(ignoreFiles, re)
	}

	return Fs{
		Config:                  config,
		fileModificationTimeout: 3 * time.Second,
		logger:                  logger,
		eventRetries:            make(map[string]int),
		ignoreFiles:             ignoreFiles,
		fileModificationTimers:  make(map[string]*time.Timer),
		fileModificationMutex:   &sync.RWMutex{},
	}, nil
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
	fsWatcher, err := fsnotify.NewBufferedWatcher(100)
	if err != nil {
		return err
	}
	defer fsWatcher.Close()

	for _, dir := range dirs {
		err = fsWatcher.AddWith(dir, fsnotify.WithOps(opsFilter))
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

			if event.Op == fsnotify.Rename {
				continue
			}

			f.logger.Debug("event received", zap.String("name", event.Name), zap.String("event", event.String()))

			f.eventRetries[event.Name]++
			if f.eventRetries[event.Name] > 10 {
				f.logger.Error("event retry limit reached", nil, zap.String("name", event.Name))
				continue
			}

			info, err := os.Stat(event.Name)
			if err != nil {
				if os.IsNotExist(err) {
					f.logger.Error("file not found", err, zap.String("name", event.Name))
					continue
				}
				f.logger.Error("getting file info", err, zap.String("name", event.Name))
				fsWatcher.Events <- event
				continue
			}

			switch event.Op {
			case fsnotify.Write:
				f.handleFileModification(event, chanFiles, info, fsWatcher)
			case fsnotify.Create:
				if !info.IsDir() {
					f.handleFileModification(event, chanFiles, info, fsWatcher)
					continue
				}

				f.logger.Info("new directory detected", zap.String("name", event.Name))

				// Add nested directories created after the parent directory
				// If the directory is the root directory, ignore all directories in the ignoreDirs list
				ignoreDirs := []string{}
				if filepath.Dir(event.Name) == f.Dir {
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
					err = fsWatcher.AddWith(dir, fsnotify.WithOps(opsFilter))
					if err != nil {
						f.logger.Error("adding directory to watch list", err, zap.String("name", dir))
						fsWatcher.Events <- event
						continue
					}
				}

				// Process any files uploaded while the directory was being added
				err = f.processExistingFiles(dirs, chanFiles)
				if err != nil {
					f.logger.Error("processing existing files in new directory", err, zap.String("name", event.Name))
					fsWatcher.Events <- event
					continue
				}
			case fsnotify.UnportableCloseWrite:
				f.fileModificationMutex.RLock()
				timer := f.fileModificationTimers[event.Name]
				f.fileModificationMutex.RUnlock()
				if timer != nil {
					timer.Stop()
					f.fileModificationMutex.Lock()
					delete(f.fileModificationTimers, event.Name)
					f.fileModificationMutex.Unlock()
				}
				err := f.processNewFile(event.Name, chanFiles, info)
				if err != nil {
					f.logger.Error("processing finished event", err, zap.String("name", event.Name))
					fsWatcher.Events <- event
					continue
				}
			}
		case err, ok := <-fsWatcher.Errors:
			if !ok {
				return nil
			}
			return err
		}
	}
}

// handleFileModification processes a file if no WRITE event is received before the timeout
func (f Fs) handleFileModification(event fsnotify.Event, chanFiles chan NewFile, info os.FileInfo, fsWatcher *fsnotify.Watcher) {
	f.fileModificationMutex.RLock()
	_, ok := f.fileModificationTimers[event.Name]
	f.fileModificationMutex.RUnlock()
	if !ok {
		f.fileModificationMutex.Lock()
		f.fileModificationTimers[event.Name] = time.AfterFunc(math.MaxInt64, func() {
			err := f.processNewFile(event.Name, chanFiles, info)
			if err != nil {
				f.logger.Error("processing finished event", err, zap.String("name", event.Name))
				fsWatcher.Events <- event
				return
			}
			f.fileModificationMutex.Lock()
			delete(f.fileModificationTimers, event.Name)
			f.fileModificationMutex.Unlock()
		})
		f.fileModificationMutex.Unlock()
	}
	f.fileModificationTimers[event.Name].Reset(f.fileModificationTimeout)
}

func (f Fs) processNewFile(path string, chanFiles chan NewFile, info os.FileInfo) error {
	var err error

	f.logger.Info("new file detected", zap.String("path", path))
	filename := filepath.Base(path)

	if !f.isValidFile(filename) {
		f.logger.Info("file ignored", zap.String("path", path))
		return nil
	}

	var bytes []byte
	if !f.SkipReadingContent {
		bytes, err = os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading file content: %w", err)
		}
	}

	chanFiles <- NewFile{
		Name:         filename,
		Path:         path,
		RelativePath: strings.TrimPrefix(path, f.Dir+"/"),
		Bytes:        bytes,
		ReceivedTime: info.ModTime().UTC(),
	}

	return nil
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

			if !f.isValidFile(file.Name()) {
				f.logger.Info("file ignored", zap.String("path", file.Name()))
				continue
			}

			filePath := filepath.Join(dir, file.Name())

			info, err := file.Info()
			if err != nil {
				return err
			}

			err = f.processNewFile(filePath, chanFiles, info)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (f Fs) isValidFile(filename string) bool {
	for _, ignoreFile := range f.ignoreFiles {
		if ignoreFile.MatchString(filename) {
			return false
		}
	}

	return true
}

func findValidDirs(path string, ignoreDirs []string) ([]string, error) {
	var dirs []string
	firstRun := true
	err := filepath.WalkDir(path, func(path string, dirEntry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		defer func() {
			firstRun = false
		}()

		if !dirEntry.IsDir() {
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
	targetPath := path.Join(f.Dir, "unprocessable", file.RelativePath)
	err := os.Rename(file.Path, targetPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err := os.MkdirAll(filepath.Dir(targetPath), os.ModePerm)
			if err != nil {
				return fmt.Errorf("creating unprocessable directory: %w", err)
			}

			return os.Rename(file.Path, targetPath)
		}

		return err
	}

	return nil
}
