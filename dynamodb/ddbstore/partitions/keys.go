package buckets

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// base64 encode the pk in order to be able to use SplitN reliably on the deliminator when decoding.
func encodeKey(partitionKey string, sortKey string) []byte {
	encoded := base64.StdEncoding.EncodeToString([]byte(partitionKey)) + "|" + sortKey
	return []byte(encoded)
}

func decodeKey(encodedKey []byte) (string, string, error) {
	// Split the decoded composite key on the separator "|"
	parts := strings.SplitN(string(encodedKey), "|", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid key format")
	}
	pkEncoded := parts[0]
	sk := parts[1]

	pk, err := base64.StdEncoding.DecodeString(string(pkEncoded))
	if err != nil {
		return "", "", err
	}

	return string(pk), sk, nil
}
