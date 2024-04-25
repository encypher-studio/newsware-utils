package flyonthewall

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type mockLogger struct{}

func (m mockLogger) Fatal(string, error, ...zap.Field)                {}
func (m mockLogger) Error(msg string, err error, fields ...zap.Field) {}
func (m mockLogger) Info(string, ...zap.Field)                        {}
func (m mockLogger) Log(zapcore.Level, string, ...zap.Field)          {}
func (m mockLogger) Println(...interface{})                           {}
func (m mockLogger) Debug(string, ...zap.Field)                       {}
