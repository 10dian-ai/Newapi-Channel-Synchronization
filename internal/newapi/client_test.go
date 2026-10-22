package newapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	var gotAuth, gotUser, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotUser = r.Header.Get("Authorization"), r.Header.Get("New-Api-User")
		switch r.Method + " " + r.URL.Path {
		case "GET /api/status":
			_, _ = w.Write([]byte(`{"success":true,"data":{"version":"v1.2.3"}}`))
		case "POST /api/channel/fix":
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"success":7,"fails":1}}`))
		case "PUT /api/option/":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			gotBody = body["key"] + "=" + body["value"]
			if body["key"] == "Bad" {
				_, _ = w.Write([]byte(`{"success":false,"message":"invalid value"}`))
				return
			}
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"success":false,"message":"no","code":"ACCESS_TOKEN_SCOPE_DENIED"}`))
		}
	}))
	defer srv.Close()

	c := New(srv.URL+"/", "nap_token", 1, time.Second)
	ctx := context.Background()

	if v, err := c.Version(ctx); err != nil || v != "v1.2.3" {
		t.Fatalf("Version = %q, %v", v, err)
	}
	ok, fails, err := c.FixAbilities(ctx)
	if err != nil || ok != 7 || fails != 1 {
		t.Fatalf("FixAbilities = %d,%d,%v", ok, fails, err)
	}
	if gotAuth != "Bearer nap_token" || gotUser != "1" {
		t.Fatalf("鉴权请求头不对：%q %q", gotAuth, gotUser)
	}
	if err := c.UpdateOption(ctx, "GroupRatio", `{"a":1}`); err != nil || gotBody != `GroupRatio={"a":1}` {
		t.Fatalf("UpdateOption：%v %q", err, gotBody)
	}
	if err := c.UpdateOption(ctx, "Bad", "x"); err == nil || !strings.Contains(err.Error(), "invalid value") {
		t.Fatalf("业务失败应返回错误：%v", err)
	}

	bad := New(srv.URL, "t", 0, time.Second)
	bad.baseURL = srv.URL + "/nope"
	_, _, err = bad.FixAbilities(ctx)
	if !IsAuthError(err) || !strings.Contains(err.Error(), "权限不足") {
		t.Fatalf("403 应识别为权限问题：%v", err)
	}
}
