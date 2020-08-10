package resourceprovider

import (
	"testing"

	"github.com/hashicorp/terraform/helper/schema"
)

func TestProvider(t *testing.T) {
	err := Provider().(*schema.Provider).InternalValidate()
	if err != nil {
		t.Fatal(err)
	}
}
