package ecslogger

import (
	"go.uber.org/zap/zapcore"
)

// ecsService describes a service as specified in the ECS standard
type ecsService struct {
	id   string
	name string
}

func (s ecsService) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("id", s.id)
	enc.AddString("name", s.name)
	return nil
}
