package state

import "fmt"

type StateString struct {
	state *State
	key   string
	value string
}

func NewStateString(state *State, key string) (StateString, error) {
	valueInterface, err := state.get(key)
	if err != nil {
		return StateString{}, err
	}

	var value string
	if valueInterface == nil {
		value = ""
	} else {
		var ok bool
		value, ok = valueInterface.(string)
		if !ok {
			return StateString{}, fmt.Errorf("state value is not a string")
		}
	}

	return StateString{
		state: state,
		key:   key,
		value: value,
	}, nil
}

func (s *StateString) Get() string {
	return fmt.Sprintf("%v", s.value)
}

func (s *StateString) SaveState(state string) error {
	s.value = state
	return s.state.saveState(s.key, state)
}
