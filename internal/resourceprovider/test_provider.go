package resourceprovider

import (
	"testing"
)

func TestProvider(t *testing.T) {
	t.Parallel()

	err := Provider().InternalValidate()
	if err != nil {
		t.Fatal(err)
	}
}
