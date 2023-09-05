package retry

import (
	"github.com/avast/retry-go/v4"
	"time"
)

type Retrier struct {
	MaxRetries uint
	MaxDelay   time.Duration
	OnRetry    func(n uint, err error, message string)
}

// RetryFunc receives a function to retry, a message that will be passed to the OnRetry hook,
// and additional github.com/avast/retry-go options
func (r *Retrier) RetryFunc(f retry.RetryableFunc, message string, opts ...retry.Option) error {
	opts = append([]retry.Option{
		retry.Attempts(r.MaxRetries),
		retry.MaxDelay(r.MaxDelay),
		retry.LastErrorOnly(true),
		retry.OnRetry(func(n uint, err error) {
			r.OnRetry(n, err, message)
		}),
	}, opts...)

	return retry.Do(
		f,
		opts...,
	)
}
