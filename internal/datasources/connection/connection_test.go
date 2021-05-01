package connection

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/test"
)

func TestReadConnection(t *testing.T) {
	t.Parallel()

	conn := test.OpenManualConnection(exaClient)
	defer conn.Close()
	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	stmt := fmt.Sprintf("CREATE CONNECTION %s TO 'foo'", name)
	_, err := conn.Conn.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	read := &test.Data{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	diags := readData(read, conn.Conn)
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
