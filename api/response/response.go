package response

type IError interface {
	error
	Code() string
}

type Response[T any, E any] struct {
	Error *ResponseError[E] `json:"error,omitempty"`
	Data  T                 `json:"data,omitempty"`
}

type ResponseError[E any] struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    E      `json:"data,omitempty"`
}

func SuccessWithData[T any](data T) Response[T, *int] {
	return Response[T, *int]{
		Data: data,
	}
}

func Success() Response[*int, *int] {
	return Response[*int, *int]{}
}

func Error(err IError) Response[*int, *int] {
	return ErrorExplicit[*int](err.Code(), err.Error(), nil)
}

func ErrorWithData[E any](err IError, data E) Response[*int, E] {
	return ErrorExplicit[E](err.Code(), err.Error(), data)
}

func ErrorExplicit[E any](code string, message string, data E) Response[*int, E] {
	return Response[*int, E]{
		Error: &ResponseError[E]{
			Code:    code,
			Message: message,
			Data:    data,
		},
	}
}
