package datasources

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
)

func TestReadPhysicalSchema(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()
	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	stmt := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", name)
	_, err := locked.Conn.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	read := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	err = readPhysicalSchemaData(read, locked.Conn)
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
