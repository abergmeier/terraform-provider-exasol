package schema

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/test"
)

func TestReadPhysicalSchema(t *testing.T) {
	t.Parallel()

	conn := test.OpenManualConnectionInTest(t, exaClient)
	defer conn.Close()
	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	stmt := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", name)
	_, err := conn.Conn.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	read := &test.Data{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	diags := readPhysicalSchemaData(read, conn.Conn)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	n := read.Get("name")
	readName, _ := n.(string)
	if readName != name {
		t.Fatalf("Expected name %s: %#v", name, n)
	}

	readID := read.Id()
	if readID != strings.ToUpper(name) {
		t.Fatalf("Expected name %s: %s", strings.ToUpper(name), readID)
	}
}
