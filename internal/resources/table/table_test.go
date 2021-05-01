package table

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/test"
	"github.com/abergmeier/terraform-provider-exasol/pkg/resource"
	"github.com/andreyvit/diff"
)

func TestCreate(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	createErr := &test.Data{
		Values: map[string]interface{}{
			"name":   name,
			"schema": schemaName,
		},
	}

	conn := test.OpenManualConnection(exaClient)
	defer conn.Close()
	conn.Conn.Execute(fmt.Sprintf("DROP TABLE %s", name), nil, schemaName)

	err := createData(createErr, conn.Conn, false)
	if err == nil {
		t.Fatal("Expected error when createData")
	}

	create := &test.Data{
		Values: map[string]interface{}{
			"name":      name,
			"schema":    schemaName,
			"composite": "A VARCHAR(20),",
		},
	}
	err = createData(create, conn.Conn, false)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	conn := test.OpenManualConnection(exaClient)
	defer conn.Close()

	conn.Conn.Execute(fmt.Sprintf("DROP TABLE %s", name), nil, schemaName)

	delete := &test.Data{
		Values: map[string]interface{}{
			"name":   name,
			"schema": schemaName,
		},
	}
	err := deleteData(delete, conn.Conn)
	if err == nil {
		t.Fatal("Expected error")
	}

	conn.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (A VARCHAR(40))", name), nil, schemaName)

	err = deleteData(delete, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestExists(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	conn := test.OpenManualConnection(exaClient)
	defer conn.Close()

	conn.Conn.Execute(fmt.Sprintf("DROP TABLE %s", name), nil, schemaName)

	exists := &test.Data{
		Values: map[string]interface{}{
			"name":   name,
			"schema": schemaName,
		},
	}
	e, err := existsData(exists, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if e {
		t.Fatal("Expected false")
	}

	conn.Conn.Execute(fmt.Sprintf("CREATE TABLE %s (A VARCHAR(40))", name), nil, schemaName)

	e, err = existsData(exists, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if !e {
		t.Fatal("Expected true")
	}
}

func TestComment(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	conn := test.OpenManualConnection(exaClient)
	defer conn.Close()

	conn.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (B VARCHAR(5), C VARCHAR(6) NOT NULL)", name), nil, schemaName)

	upd := &test.Data{
		Values: map[string]interface{}{
			"name:":   name,
			"schema":  schemaName,
			"comment": "Foo",
		},
	}

	err := updateData(upd, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestImport(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	conn := test.OpenManualConnection(exaClient)
	defer conn.Close()

	conn.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (B VARCHAR(5), C VARCHAR(6) NOT NULL)", name), nil, schemaName)

	imp := &test.Data{
		Values: map[string]interface{}{
			"name:":  name,
			"schema": schemaName,
		},
	}
	imp.SetId(resource.NewID(schemaName, name))

	err := importData(imp, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	imp = &test.Data{
		Values: map[string]interface{}{
			"name:":     name,
			"schema":    schemaName,
			"composite": "B VARCHAR(5)",
		},
	}
	imp.SetId(name)

	err = importData(imp, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	composite := imp.Get("composite").(string)

	expectedComposite := `B VARCHAR(5) UTF8 NULL,
C VARCHAR(6) UTF8 NOT NULL,
`
	if composite != expectedComposite {
		ld := diff.LineDiff(composite, expectedComposite)

		t.Fatalf("Unexpected composite value:\n%s", ld)
	}
}

func TestRename(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	conn := test.OpenManualConnection(exaClient)
	defer conn.Close()

	conn.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (A VARCHAR(10) COMMENT IS 'Foo')", name), nil, schemaName)

	newName := name + "_SHINY"
	rename := &test.Data{
		Values: map[string]interface{}{
			"name":      name,
			"schema":    schemaName,
			"composite": "A VARCHAR(10) COMMENT IS 'Foo'",
		},
		NewValues: map[string]interface{}{
			"name":      newName,
			"schema":    schemaName,
			"composite": "A VARCHAR(10) COMMENT IS 'Foo'",
		},
	}

	err := updateData(rename, conn.Conn)
	if err != nil {
		t.Fatal("Unknown error:", err)
	}

	read := &test.Data{
		Values: map[string]interface{}{
			"name":      newName,
			"schema":    schemaName,
			"composite": "Dummy",
		},
	}

	diags := readData(read, conn.Conn)
	if diags.HasError() {
		t.Fatal("Unknown error:", err)
	}

	composite := read.Get("composite").(string)
	expectedComposite := `A VARCHAR(10) UTF8 NULL COMMENT IS 'Foo',
`
	if composite != expectedComposite {
		t.Fatalf("Unexpected composite:\n%s", diff.LineDiff(expectedComposite, composite))
	}
}

func TestImportConstraint(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	conn := test.OpenManualConnection(exaClient)
	defer conn.Close()

	conn.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (A VARCHAR(10), B VARCHAR(20), CONSTRAINT PK PRIMARY KEY (B), DISTRIBUTE BY A)", name), nil, schemaName)

	imp := &test.Data{
		Values: map[string]interface{}{
			"name:":     name,
			"schema":    schemaName,
			"composite": "A", // dummy value to trigger composite refresh
		},
	}
	imp.SetId(resource.NewID(schemaName, name))

	err := importData(imp, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	composite := imp.Get("composite").(string)
	expectedComposite := `A VARCHAR(10) UTF8 NULL,
B VARCHAR(20) UTF8 NOT NULL,
CONSTRAINT PRIMARY KEY (B),
DISTRIBUTE BY A,
`
	if composite != expectedComposite {
		t.Fatalf("Unexpected composite:\n%s", diff.LineDiff(composite, expectedComposite))
	}
}
