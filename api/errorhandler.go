package api

import (
	"errors"
	"strconv"

	"github.com/encypher-studio/newsware-utils/api/apierror"
	"github.com/encypher-studio/newsware-utils/api/response"
	"github.com/gofiber/fiber/v3"
	"github.com/rs/zerolog"
)

type IError interface {
	response.IError
	StatusCode() int
	Response() interface{}
}

var ErrorHandler = func(l zerolog.Logger) fiber.ErrorHandler {
	return func(c fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError
		var resp interface{}

		if e, ok := errors.AsType[*fiber.Error](err); ok {
			code = e.Code
		} else if apiErr, ok := err.(IError); ok {
			resp = apiErr.Response()
			code = apiErr.StatusCode()
		}

		if resp == nil {
			resp = response.Error(apierror.New(strconv.Itoa(code), err.Error(), code))
		}

		l.Error().Err(err).Str("path", c.Path()).Msg("request error")
		return c.Status(code).JSON(resp)
	}
}
