package ecslogger

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

type logError struct {
	Message    string `json:"message"`
	StackTrace string `json:"stack_trace"`
}
type logService struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}
type logOutput struct {
	LogLevel   string     `json:"log.level"`
	Message    string     `json:"message"`
	EcsVersion string     `json:"ecs.version"`
	Error      logError   `json:"error"`
	Service    logService `json:"service"`
}

func dummyErrorFunction() error {
	return errors.New("dummy error")
}

func TestGet(t *testing.T) {
	logger, err := New(Config{
		Service: ServiceConfig{
			Id:   "test_id",
			Name: "test_name",
		},
		Level: zapcore.InfoLevel,
		Path:  "./test/log.log",
	})
	assert.NoError(t, err)

	expectedLogs := []logOutput{
		{
			LogLevel:   "error",
			Message:    "error message",
			EcsVersion: "1.6.0",
			Error: logError{
				Message:    "dummy error",
				StackTrace: "\ngithub.com/encypher-studio/newsware-utils/ecslogger.dummyErrorFunction",
			},
			Service: logService{
				Id:   "test_id",
				Name: "test_name",
			},
		},
		{
			LogLevel:   "info",
			Message:    "info message",
			EcsVersion: "1.6.0",
			Service: logService{
				Id:   "test_id",
				Name: "test_name",
			},
		},
	}

	testErr := dummyErrorFunction()
	logger.Error(expectedLogs[0].Message, testErr)
	logger.Info(expectedLogs[1].Message)

	readLogFile, err := os.OpenFile("./test/log.log", os.O_RDONLY, os.ModePerm)
	assert.NoError(t, err)
	defer readLogFile.Close()

	i := 0
	scanner := bufio.NewScanner(readLogFile)
	for scanner.Scan() {
		text := scanner.Text()
		actualLog := logOutput{}
		err = json.Unmarshal([]byte(text), &actualLog)
		assert.NoError(t, err)
		assert.Contains(t, actualLog.Error.StackTrace, expectedLogs[i].Error.StackTrace)
		expectedLogs[i].Error.StackTrace = actualLog.Error.StackTrace
		assert.Equal(t, expectedLogs[i], actualLog)
		i++
	}

	os.RemoveAll("./test")
}

func TestRotation(t *testing.T) {
	logDir := "./test"
	logPath := logDir + "/log.log"

	err := os.Mkdir(logDir, os.ModePerm)
	if !assert.NoError(t, err) {
		assert.FailNow(t, "")
	}

	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if !assert.NoError(t, err) {
		assert.FailNow(t, "")
	}

	bytesToWrite := make([]byte, 2e6)
	for i := range bytesToWrite {
		bytesToWrite[i] = []byte("1")[0]
	}

	if _, err = file.Write(bytesToWrite); !assert.NoError(t, err) {
		assert.FailNow(t, "")
	}

	defaultLogRotation.MaxSize = 1
	logger, err := New(Config{
		Service: ServiceConfig{
			Id:   "test_id",
			Name: "test_name",
		},
		Level: zapcore.InfoLevel,
		Path:  logPath,
	})
	if !assert.NoError(t, err) {
		assert.FailNow(t, "")
	}
	logger.Error("dummy error", errors.New("error"))

	count := 0
	err = filepath.Walk(logDir, func(path string, info os.FileInfo, err error) error {
		if strings.HasPrefix(info.Name(), "log") && strings.HasSuffix(info.Name(), ".log") {
			count++
		}
		return nil
	})

	assert.Equal(t, 2, count, "number of log files is not correct")

	os.RemoveAll(logDir)
}

func TestLogger_Fatal(t *testing.T) {
	defer os.RemoveAll("./test")

	logger, err := New(Config{
		Service: ServiceConfig{
			Id:   "test_id",
			Name: "test_name",
		},
		Level: zapcore.InfoLevel,
		Path:  "./test/log.log",
	})
	if !assert.NoError(t, err) {
		assert.FailNow(t, "")
	}

	if os.Getenv("BE_CRASHER") == "1" {
		logger.Fatal("error", errors.New("error"))
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestLogger_Fatal")
	cmd.Env = append(os.Environ(), "BE_CRASHER=1")
	err = cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

func TestLogger_Error_shouldTruncateLargeErrorMessage(t *testing.T) {
	defer os.RemoveAll("./test")
	logger, err := New(Config{
		Service: ServiceConfig{
			Id:   "test_id",
			Name: "test_name",
		},
		Level: zapcore.InfoLevel,
		Path:  "./test/log.log",
	})
	assert.NoError(t, err)

	errBytes := make([]byte, errMsgMaxLength+1)
	for i := range errBytes {
		errBytes[i] = []byte("1")[0]
	}
	logger.Error("error", errors.New(string(errBytes)))

	readLogFile, err := os.OpenFile("./test/log.log", os.O_RDONLY, os.ModePerm)
	assert.NoError(t, err)
	defer readLogFile.Close()

	logFileBytes, err := io.ReadAll(readLogFile)
	if err != nil {
		return
	}

	actualLog := logOutput{}
	err = json.Unmarshal(logFileBytes, &actualLog)
	if !assert.NoError(t, err) {
		assert.FailNow(t, "")
	}

	assert.Len(t, actualLog.Error.Message, errMsgMaxLength+len(truncateSuffix))
	assert.True(t, strings.HasSuffix(actualLog.Error.Message, truncateSuffix), "error message has no truncate suffix")
}

func TestLogger_conforms_to_interface(t *testing.T) {
	type wrapper struct {
		logger ILogger
	}

	logger, err := New(Config{
		Service: ServiceConfig{
			Id:   "test_id",
			Name: "test_name",
		},
		Level: zapcore.InfoLevel,
		Path:  "./test/log.log",
	})
	assert.NoError(t, err)

	_ = wrapper{logger: logger}
}
