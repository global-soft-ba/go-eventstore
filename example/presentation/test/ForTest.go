package test

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"net/url"
)

func GetTestContext(rawQuery string, routeParams ...gin.Param) *gin.Context {
	ctx, _ := GetTestContextWithHeaderAndWriter(rawQuery, routeParams...)
	return ctx
}

func GetTestContextWithHeaderAndWriter(rawQuery string, routeParams ...gin.Param) (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request = &http.Request{
		URL: &url.URL{RawQuery: rawQuery},
	}
	ctx.Params = routeParams
	return ctx, w
}
