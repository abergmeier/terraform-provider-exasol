package datasources

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
)

func TestReadPhysicalSchema(t *testing.T) {
	name := t.Name()

	stmt := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", name)
	_, err := exaClient.Conn.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	defer exaClient.Conn.Execute(fmt.Sprintf("DROP SCHEMA %s", name))

	read := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	err = readPhysicalSchemaData(read, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
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
