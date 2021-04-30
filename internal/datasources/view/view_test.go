package view

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/andreyvit/diff"
)

func TestRead(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	_, err := locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s_TABLE (A CHAR(10), B VARCHAR(20), C INT)", name), nil, schemaName)
	if err != nil {
		t.Fatal(err)
	}
	_, err = locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE VIEW %s_VIEW (VA COMMENT IS 'FOO', VB, VC) AS SELECT A, B, C FROM %s_TABLE AS T", name, name), nil, schemaName)
	if err != nil {
		t.Fatal(err)
	}

	d := &internal.TestData{
		Values: map[string]interface{}{
			"name":   fmt.Sprintf("%s_VIEW", name),
			"schema": schemaName,
		},
	}

	diags := readData(d, locked.Conn)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	comp := d.Get("composite").(string)
	expectedComposite := `VA COMMENT IS 'FOO',
VB,
VC,
`
	if comp != expectedComposite {
		lines := diff.LineDiff(comp, expectedComposite)
		t.Fatalf("Unexpected composite:\n%s", lines)
	}

	sq := d.Get("subquery").(string)
	expectedSubquery := fmt.Sprintf("SELECT A, B, C FROM %s_TABLE AS T", name)
	if sq != expectedSubquery {
		lines := diff.LineDiff(sq, expectedSubquery)
		t.Fatalf("Unexpected subquery:\n%s", lines)
	}
}
