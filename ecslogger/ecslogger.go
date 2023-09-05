package ecslogger

import (
	"go.elastic.co/ecszap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path/filepath"
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

// Logger logs to a file following ElasticSearch ECS standard. It is compatible with pkg/errors and eris
// error libraries. It can also print to console depending on the values passed to Init
type Logger struct {
	logger  *zap.Logger
	service ecsService
}

// Init sets up Logger.
//
// serviceId must distinctly identify the service that is using the logger. If the service runs
// in multiple nodes, the serviceId should be the same for all. For example an indexer for SEC filings running on 10
// nodes, can have "indexer_sec" as id across all instances.
//
// serviceName is just a readable name for the service that will be added to logs.
func (l *Logger) Init(serviceId string, serviceName string, level zapcore.Level, logPath string, logToConsole bool) (err error) {
	l.logger, err = get(level, logPath, logToConsole)
	if err != nil {
		return err
	}
	l.service = ecsService{
		id:   serviceId,
		name: serviceName,
	}
	return nil
}

func (l *Logger) Fatal(msg string, err error, fields ...zap.Field) {
	fields = append(fields, zap.Error(err))
	l.Log(zapcore.FatalLevel, msg, fields...)
}

func (l *Logger) Error(msg string, err error, fields ...zap.Field) {
	fields = append(fields, zap.Error(err))
	l.Log(zapcore.ErrorLevel, msg, fields...)
}

func (l *Logger) Info(msg string, fields ...zap.Field) {
	l.Log(zapcore.InfoLevel, msg, fields...)
}

func (l *Logger) Log(level zapcore.Level, msg string, fields ...zap.Field) {
	fields = append(fields, zapcore.Field{Key: "service",
		Type:      zapcore.ObjectMarshalerType,
		Interface: l.service,
	})
	l.logger.Log(level, msg, fields...)
}

func (l *Logger) Println(args ...interface{}) {
	l.Println(args)
}

// get returns a new *zap.Logger that outputs logs to logPath. If logPath is empty or logToConsole is true, then it
// logs to console. The logs are stored to file following ElasticSearch ECS specification. For files, only errors are
// logged
func get(level zapcore.Level, logPath string, logToConsole bool) (*zap.Logger, error) {
	var cores []zapcore.Core

	if logPath != "" {
		cores = append(cores, NewCore(
			ecszap.NewDefaultEncoderConfig(),
			zapcore.AddSync(&lumberjack.Logger{
				Filename:   logPath,
				MaxSize:    defaultLogRotation.MaxSize,
				MaxAge:     defaultLogRotation.MaxAge,
				MaxBackups: defaultLogRotation.MaxBackups,
				LocalTime:  defaultLogRotation.LocalTime,
				Compress:   defaultLogRotation.Compress,
			}),
			zapcore.ErrorLevel),
		)
	}

	if logToConsole || len(cores) == 0 {
		consoleEncoder := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
		cores = append(cores, zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level))
	}

	core := zapcore.NewTee(cores...)

	l := zap.New(core)

	return l, nil
}

func getLogFile(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dir, os.ModePerm)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return f, nil
}
