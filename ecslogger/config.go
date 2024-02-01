package ecslogger

import "go.uber.org/zap/zapcore"

type Config struct {
	Service ServiceConfig
	Console bool
	Path    string
	Level   zapcore.Level
}

type ServiceConfig struct {
	Id   string
	Name string
}
