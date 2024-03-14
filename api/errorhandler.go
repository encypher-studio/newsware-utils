package api

import (
	"errors"
	"strconv"

	"github.com/encypher-studio/newsware-utils/api/apierror"
	"github.com/encypher-studio/newsware-utils/api/response"
	"github.com/encypher-studio/newsware-utils/ecslogger"
	"github.com/gofiber/fiber/v2"
)

type IError interface {
	response.IError
	StatusCode() int
	Response() interface{}
}

var ErrorHandler = func(l ecslogger.ILogger) fiber.ErrorHandler {
	return func(c *fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError
		var resp interface{}

		var e *fiber.Error
		if errors.As(err, &e) {
			code = e.Code
		} else if apiErr, ok := err.(IError); ok {
			resp = apiErr.Response()
			code = apiErr.StatusCode()
		}

		if resp == nil {
			resp = response.Error(apierror.New(strconv.Itoa(code), err.Error(), code))
		}

		l.Error(c.Path(), err)
		return c.Status(code).JSON(resp)
	}
}
