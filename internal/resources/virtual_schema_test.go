package resources

import (
	"strings"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
)

func TestCreateVirtualSchema(t *testing.T) {
	name := t.Name()

	delete := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name":           name,
			"adapter_script": "foo.bar",
			"properties": map[string]string{
				"foo": "bar",
			},
		},
	}

	deleteVirtualSchemaData(delete, exaClient)

	err := createVirtualSchemaData(create, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	defer deleteVirtualSchemaData(create, exaClient)

	if create.Id() != strings.ToUpper(name) {
		t.Fatalf("Expected name %s: %s", strings.ToUpper(name), create.Id())
	}
}
