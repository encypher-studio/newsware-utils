package retrier

import (
	"testing"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestRetrier_RetryFunc(t *testing.T) {
	var onRetryCount uint
	retrier := Retrier{
		MaxRetries: 0,
		MaxDelay:   time.Nanosecond,
		OnRetry: func(n uint, err error, message string) {
			onRetryCount = n
		},
	}

	var retryCallCount uint
	retrier.RetryFunc(
		func() error {
			if retryCallCount == 10 {
				return retry.Unrecoverable(errors.New(""))
			}
			retryCallCount += 1
			return errors.New("")
		}, "",
	)

	assert.Equal(t, onRetryCount, uint(10))
	assert.Equal(t, retryCallCount, uint(10))
}
