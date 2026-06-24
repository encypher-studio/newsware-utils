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
	LogLevel() zerolog.Level
}

var ErrorHandler = func(l zerolog.Logger) fiber.ErrorHandler {
	return func(c fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError
		var resp interface{}
		logLevel := zerolog.NoLevel

		if e, ok := errors.AsType[*fiber.Error](err); ok {
			code = e.Code
		} else if apiErr, ok := err.(IError); ok {
			resp = apiErr.Response()
			code = apiErr.StatusCode()
			logLevel = apiErr.LogLevel()
		}

		if resp == nil {
			resp = response.Error(apierror.New(strconv.Itoa(code), err.Error(), code))
		}

		if logLevel == zerolog.NoLevel {
			if code >= 500 {
				logLevel = zerolog.ErrorLevel
			} else {
				logLevel = zerolog.WarnLevel
			}
		}
		l.WithLevel(logLevel).Err(err).Str("path", c.Path()).Send()
		return c.Status(code).JSON(resp)
	}
}
