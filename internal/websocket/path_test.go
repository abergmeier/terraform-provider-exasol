package websocket

import "testing"

func TestPath(t *testing.T) {
	t.Parallel()
	s, err := pathFromRequest("2", map[string]interface{}{
		"foo": 4,
	})
	if err != nil {
		t.Fatal("pathFromRequest failed:", err)
	}
	if s != "v1/request_by_id/2/1F7267F4FC9FF9D36857D71FDB0B6A14783AF188E7679BA702D7846B97C5350A" {
		t.Errorf("Expected path v1/request_by_id/2/1F7267F4FC9FF9D36857D71FDB0B6A14783AF188E7679BA702D7846B97C5350A. Got: %s", s)
	}
}
