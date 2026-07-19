package authorization_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/muratmirgun/yordam/internal/authorization"
)

func TestAuthorizationCommittedTokenIsOpaqueAndZeroInvalid(t *testing.T) {
	zero := authorization.CommittedToken{}
	if zero.ValidFor("activity", "call", authDigest('a'), authDigest('b'), authDigest('c'), "generation", "nonce") {
		t.Fatal("zero token is valid")
	}
	if _, err := json.Marshal(zero); err == nil {
		t.Fatal("committed token has a JSON representation")
	}
	if err := authorization.NewService(nil).Dispatch(context.Background(), zero, authorization.DispatchBinding{}, func(context.Context) error { return nil }); !errors.Is(err, authorization.ErrInvalidCommittedToken) {
		t.Fatalf("zero dispatch err=%v", err)
	}
}
