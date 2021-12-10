package view

import (
	"context"
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/andreyvit/diff"
	"github.com/google/go-cmp/cmp"
)

func TestViewDatasourceRead(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()

	_, err := locked.Tx.Exec(fmt.Sprintf("CREATE OR REPLACE TABLE %s.%s_TABLE (A CHAR(10), B VARCHAR(20), C INT)", schemaName, name))
	if err != nil {
		t.Fatal(err)
	}
	_, err = locked.Tx.Exec(fmt.Sprintf("CREATE OR REPLACE VIEW %s.%s_VIEW (VA COMMENT IS 'FOO', VB, VC) AS SELECT A, B, C FROM %s.%s_TABLE AS T", schemaName, name, schemaName, name))
	if err != nil {
		t.Fatal(err)
	}

	d := &internal.TestData{
		Values: map[string]interface{}{},
	}

	diags := readData(context.TODO(), d, locked.Tx, argument.RequiredArguments{
		Schema: schemaName,
		Name:   fmt.Sprintf("%s_VIEW", name),
	})
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	comp := d.Get("column").([]interface{})
	expectedComposite := []interface{}{
		map[string]interface{}{
			"name":    "VA",
			"comment": "FOO",
		},
		map[string]interface{}{
			"name": "VB",
		},
		map[string]interface{}{
			"name": "VC",
		},
	}

	if cmp.Diff(comp, expectedComposite) != "" {
		t.Fatalf("Unexpected columns:\n%s", cmp.Diff(comp, expectedComposite))
	}

	sq := d.Get("subquery").(string)
	expectedSubquery := fmt.Sprintf("SELECT A, B, C FROM %s.%s_TABLE AS T", schemaName, name)
	if sq != expectedSubquery {
		lines := diff.LineDiff(sq, expectedSubquery)
		t.Fatalf("Unexpected subquery:\n%s", lines)
	}
}
