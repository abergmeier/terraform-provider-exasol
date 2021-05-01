package websocket

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

func pathFromRequest(id string, request interface{}) (string, error) {
	jreq, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("cannot marshal request\n%#v:\n%s", request, err)
	}
	hash := sha256.Sum256(jreq)
	return fmt.Sprintf("v1/request_by_id/%s/%X", id, hash), nil
}
