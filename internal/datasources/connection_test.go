package datasources

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
)

func TestReadConnection(t *testing.T) {
	name := t.Name()

	stmt := fmt.Sprintf("CREATE CONNECTION %s TO 'foo'", name)
	_, err := exaClient.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	defer exaClient.Execute(fmt.Sprintf("DROP CONNECTION %s", name))

	read := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	err = readConnectionData(read, exaClient)
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
