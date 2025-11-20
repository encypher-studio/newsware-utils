package state

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

type State struct {
	filePath string
	mutex    *sync.Mutex
	state    map[string]interface{}
}

func NewState(filePath string) (State, error) {
	s := State{
		mutex:    &sync.Mutex{},
		filePath: filePath,
	}

	err := s.initFromFile()
	if err != nil {
		return State{}, err
	}

	return s, nil
}

// saveState stores the watcher state in i.stateDir/state.json
func (s *State) saveState(key string, state interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.state[key] = state

	stateFile, err := s.ensureStateFile()
	if err != nil {
		return errors.Wrap(err, "failed to ensure state file")
	}

	stateBytes, err := json.Marshal(s.state)
	if err != nil {
		return errors.Wrap(err, "failed to marshall state struct")
	}

	err = stateFile.Truncate(0)
	if err != nil {
		return err
	}

	_, err = stateFile.Write(stateBytes)
	if err != nil {
		return errors.Wrap(err, "failed to write state file")
	}

	return nil
}

func (s *State) ensureStateFile() (*os.File, error) {
	_, err := os.Stat(filepath.Dir(s.filePath))
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(filepath.Dir(s.filePath), os.ModePerm)
			if err != nil {
				return nil, errors.Wrap(err, "creating state directory")
			}
		}
	}

	f, err := os.OpenFile(s.filePath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "opening state file")
	}

	return f, nil
}

func (s *State) get(key string) (interface{}, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.state[key], nil
}

func (s *State) initFromFile() error {
	s.state = make(map[string]interface{})

	stateFile, err := s.ensureStateFile()
	if err != nil {
		return errors.Wrap(err, "failed to ensure state file")
	}

	stateBytes, err := io.ReadAll(stateFile)
	if err != nil {
		return errors.Wrap(err, "initializing from persisted state: reading state file")
	}

	if len(stateBytes) == 0 {
		return nil
	}

	err = stateFile.Close()
	if err != nil {
		return err
	}

	err = json.Unmarshal(stateBytes, &s.state)
	if err != nil {
		return errors.Wrap(err, "initializing from persisted state: unmarshalling state file")
	}

	return nil
}
