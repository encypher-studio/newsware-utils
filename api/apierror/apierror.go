package apierror

import "github.com/encypher-studio/newsware-utils/api/response"

type ApiError[T any] struct {
	inner      error
	code       string
	message    string
	statusCode int
	data       T
}

func New(code string, message string, statusCode int) ApiError[*int] {
	return ApiError[*int]{
		code:       code,
		message:    message,
		statusCode: statusCode,
	}
}

func NewWithData[T any](code string, message string, statusCode int) ApiError[T] {
	return ApiError[T]{
		code:       code,
		message:    message,
		statusCode: statusCode,
	}
}

func (a ApiError[T]) SetData(data T) ApiError[T] {
	a.data = data
	return a
}

func (a ApiError[T]) Response() interface{} {
	return response.ErrorWithData(a, a.data)
}

func (a ApiError[T]) Error() string {
	if a.inner != nil {
		if a.message != "" {
			a.message += ": "
		}
		a.message += a.inner.Error()
	}
	return a.message
}

func (a ApiError[T]) Unwrap() error {
	return a.inner
}

func (a ApiError[T]) With(err error) ApiError[T] {
	a.inner = err
	return a
}

func (a ApiError[T]) Code() string {
	return a.code
}

func (a ApiError[T]) StatusCode() int {
	return a.statusCode
}
