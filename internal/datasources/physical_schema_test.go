package datasources

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
)

func TestReadPhysicalSchema(t *testing.T) {
	t.Parallel()

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()
	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	stmt := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", name)
	_, err := locked.Tx.Exec(stmt)
	if err != nil {
		t.Fatal(err)
	}

	read := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	diags := readPhysicalSchemaData(context.TODO(), read, locked.Tx)
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
