package ecslogger

import (
	"os"

	"go.elastic.co/ecszap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	defaultLogRotation = lumberjack.Logger{
		MaxSize:    100,
		MaxAge:     10,
		MaxBackups: 10,
		LocalTime:  false,
		Compress:   false,
	}
)

type ILogger interface {
	Fatal(msg string, err error, fields ...zap.Field)
	Error(msg string, err error, fields ...zap.Field)
	Info(msg string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
	Log(level zapcore.Level, msg string, fields ...zap.Field)
	Println(args ...interface{})
}

// Logger logs to a file following ElasticSearch ECS standard. It is compatible with pkg/errors and eris
// error libraries. It can also print to console depending on the values passed to Init
type Logger struct {
	logger  *zap.Logger
	service ecsService
}

// New returns a configured Logger.
//
// serviceId must distinctly identify the service that is using the logger. If the service runs
// in multiple nodes, the serviceId should be the same for all. For example an indexer for SEC filings running on 10
// nodes, can have "indexer_sec" as id across all instances.
//
// serviceName is just a readable name for the service that will be added to logs.
//
// Logs to logPath. If logPath is empty or logToConsole is true, then it logs to console.
func New(config Config) (Logger, error) {
	var cores []zapcore.Core

	if config.Path != "" {
		cores = append(cores, NewCore(
			ecszap.NewDefaultEncoderConfig(),
			zapcore.AddSync(&lumberjack.Logger{
				Filename:   config.Path,
				MaxSize:    defaultLogRotation.MaxSize,
				MaxAge:     defaultLogRotation.MaxAge,
				MaxBackups: defaultLogRotation.MaxBackups,
				LocalTime:  defaultLogRotation.LocalTime,
				Compress:   defaultLogRotation.Compress,
			}),
			zapcore.ErrorLevel),
		)
	}

	if config.Console || len(cores) == 0 {
		consoleEncoder := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
		cores = append(cores, zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), config.Level))
	}

	core := zapcore.NewTee(cores...)

	zapLogger := zap.New(core)

	return Logger{
		logger: zapLogger,
		service: ecsService{
			id:   config.Service.Id,
			name: config.Service.Name,
		},
	}, nil
}

func (l Logger) Fatal(msg string, err error, fields ...zap.Field) {
	fields = append(fields, zap.Error(err))
	l.Log(zapcore.FatalLevel, msg, fields...)
}

func (l Logger) Error(msg string, err error, fields ...zap.Field) {
	fields = append(fields, zap.Error(err))
	l.Log(zapcore.ErrorLevel, msg, fields...)
}

func (l Logger) Info(msg string, fields ...zap.Field) {
	l.Log(zapcore.InfoLevel, msg, fields...)
}

func (l Logger) Debug(msg string, fields ...zap.Field) {
	l.Log(zapcore.DebugLevel, msg, fields...)
}

func (l Logger) Log(level zapcore.Level, msg string, fields ...zap.Field) {
	fields = append(fields, zapcore.Field{Key: "service",
		Type:      zapcore.ObjectMarshalerType,
		Interface: l.service,
	})
	l.logger.Log(level, msg, fields...)
}

func (l Logger) Println(args ...interface{}) {
}
