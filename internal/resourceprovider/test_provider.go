package resourceprovider

import (
	"testing"
)

func TestProvider(t *testing.T) {
	err := Provider().InternalValidate()
	if err != nil {
		t.Fatal(err)
	}
}
