package session

import (
	"github.com/gorilla/sessions"
	"github.com/labstack/echo"
	"github.com/labstack/echo-contrib/session"
)

func SessUserID(c echo.Context) int64 {
	sess, _ := session.Get("session", c)
	var userID int64
	if x, ok := sess.Values["user_id"]; ok {
		userID, _ = x.(int64)
	}
	return userID
}

func SessSetUserID(c echo.Context, id int64) {
	sess, _ := session.Get("session", c)
	sess.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   3600,
		HttpOnly: true,
	}
	sess.Values["user_id"] = id
	sess.Save(c.Request(), c.Response())
}

func SessDeleteUserID(c echo.Context) {
	sess, _ := session.Get("session", c)
	sess.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   3600,
		HttpOnly: true,
	}
	delete(sess.Values, "user_id")
	sess.Save(c.Request(), c.Response())
}

func SessAdministratorID(c echo.Context) int64 {
	sess, _ := session.Get("session", c)
	var administratorID int64
	if x, ok := sess.Values["administrator_id"]; ok {
		administratorID, _ = x.(int64)
	}
	return administratorID
}

func SessSetAdministratorID(c echo.Context, id int64) {
	sess, _ := session.Get("session", c)
	sess.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   3600,
		HttpOnly: true,
	}
	sess.Values["administrator_id"] = id
	sess.Save(c.Request(), c.Response())
}

func SessDeleteAdministratorID(c echo.Context) {
	sess, _ := session.Get("session", c)
	sess.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   3600,
		HttpOnly: true,
	}
	delete(sess.Values, "administrator_id")
	sess.Save(c.Request(), c.Response())
}
