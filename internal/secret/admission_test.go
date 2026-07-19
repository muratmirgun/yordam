package secret_test

import (
	"encoding/base64"
	"encoding/hex"
	"testing"

	"github.com/muratmirgun/yordam/internal/secret"
)

func TestAdmissionScannerDetectsRawAndExactEncodedVariants(t *testing.T) {
	raw := []byte("synthetic-secret\xff")
	scanner := secret.NewAdmissionScanner(raw)
	for name, value := range admissionVariants(raw) {
		t.Run(name, func(t *testing.T) {
			if !scanner.Scan(append([]byte("prefix:"), append(value, []byte(":suffix")...)...)) {
				t.Fatalf("variant was not detected: %x", value)
			}
		})
	}
	if scanner.Scan([]byte("synthetic-secre")) {
		t.Fatal("scanner matched a non-secret prefix")
	}
}

func TestAdmissionStreamDetectsSecretAcrossChunkBoundary(t *testing.T) {
	scanner := secret.NewAdmissionScanner([]byte("boundary-secret"))
	stream := scanner.Stream()
	if stream.Write([]byte("before-boundary-se")) {
		t.Fatal("stream reported a secret before the complete variant arrived")
	}
	if !stream.Write([]byte("cret-after")) {
		t.Fatal("stream missed a secret split across writes")
	}
	if !stream.Close() {
		t.Fatal("closed stream forgot its detected state")
	}
}

func admissionVariants(raw []byte) map[string][]byte {
	return map[string][]byte{
		"raw":              append([]byte(nil), raw...),
		"base64-padded":    []byte(base64.StdEncoding.EncodeToString(raw)),
		"base64-unpadded":  []byte(base64.RawStdEncoding.EncodeToString(raw)),
		"base64url-padded": []byte(base64.URLEncoding.EncodeToString(raw)),
		"base64url-raw":    []byte(base64.RawURLEncoding.EncodeToString(raw)),
		"lowercase-hex":    []byte(hex.EncodeToString(raw)),
		"uppercase-hex":    []byte(stringUpper(hex.EncodeToString(raw))),
	}
}

func stringUpper(value string) string {
	buffer := []byte(value)
	for index, character := range buffer {
		if character >= 'a' && character <= 'f' {
			buffer[index] = character - ('a' - 'A')
		}
	}
	return string(buffer)
}
