package connection

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
)

func TestReadConnection(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()
	name := t.Name()

	stmt := fmt.Sprintf("CREATE CONNECTION %s TO 'foo'", name)
	_, err := locked.Conn.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	read := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	err = readData(read, locked.Conn)
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
