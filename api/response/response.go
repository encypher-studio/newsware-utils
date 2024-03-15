package response

type IError interface {
	error
	Code() string
}

type Response[T any, E any] struct {
	Error      *ResponseError[E] `json:"error,omitempty"`
	Data       *T                `json:"data,omitempty"`
	Pagination *Pagination       `json:"pagination,omitempty"`
}

type ResponseError[E any] struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    E      `json:"data,omitempty"`
}

// Pagination contains data related to pagination for a request
type Pagination struct {
	Cursor interface{} `json:"cursor,omitempty"`
	Total  *int64      `json:"total,omitempty"`
}

func SuccessWithData[T any](data T, pagination ...Pagination) Response[T, *int] {
	res := Response[T, *int]{
		Data: &data,
	}

	if len(pagination) > 0 {
		res.Pagination = &pagination[0]
	}

	return res
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
