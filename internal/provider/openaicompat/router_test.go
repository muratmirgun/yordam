package openaicompat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/muratmirgun/yordam/internal/domain"
)

func TestRouterSelectsProfileAndRequestedModel(t *testing.T) {
	cloudCount, localCount, localModel := 0, 0, ""
	cloudServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cloudCount++
		fmt.Fprintln(w, "data: [DONE]")
	}))
	defer cloudServer.Close()
	localServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		localCount++
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		localModel = body.Model
		fmt.Fprintln(w, "data: [DONE]")
	}))
	defer localServer.Close()
	router, err := NewRouter(map[string]*Client{
		"cloud": New(ClientOptions{HTTPClient: cloudServer.Client(), BaseURL: cloudServer.URL, Model: "cloud-default"}),
		"local": New(ClientOptions{HTTPClient: localServer.Client(), BaseURL: localServer.URL, Model: "local-default"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := router.Stream(context.Background(), domain.ModelRequest{Selection: domain.ModelSelection{Profile: "local", Model: "code-model"}})
	if err != nil {
		t.Fatal(err)
	}
	for event := range events {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	}
	if cloudCount != 0 || localCount != 1 || localModel != "code-model" {
		t.Fatalf("cloud=%d local=%d model=%q", cloudCount, localCount, localModel)
	}
	if _, err := router.Stream(context.Background(), domain.ModelRequest{Selection: domain.ModelSelection{Profile: "missing"}}); err == nil || !strings.Contains(err.Error(), "unknown provider profile") {
		t.Fatalf("error=%v", err)
	}
}

func TestNewRouterRejectsInvalidProfiles(t *testing.T) {
	if _, err := NewRouter(nil); err == nil {
		t.Fatal("expected empty router error")
	}
	if _, err := NewRouter(map[string]*Client{"": New(ClientOptions{})}); err == nil {
		t.Fatal("expected empty profile error")
	}
	if _, err := NewRouter(map[string]*Client{"cloud": nil}); err == nil {
		t.Fatal("expected nil client error")
	}
}
