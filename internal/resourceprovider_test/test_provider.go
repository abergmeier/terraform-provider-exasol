package resourceprovider

import (
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/resourceprovider"
)

func TestProvider(t *testing.T) {
	t.Parallel()

	err := resourceprovider.Provider().InternalValidate()
	if err != nil {
		t.Fatal(err)
	}
}
