package table

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/resource"
	"github.com/andreyvit/diff"
)

func TestCreate(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	createErr := &internal.TestData{
		Values: map[string]interface{}{},
	}

	locked := exaClient.Lock()
	defer locked.Unlock()
	locked.Conn.Execute(fmt.Sprintf("DROP TABLE %s", name), nil, schemaName)

	err := createData(createErr, locked.Conn, argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	}, false)
	if err == nil {
		t.Fatal("Expected error when createData")
	}

	create := &internal.TestData{
		Values: map[string]interface{}{
			"composite": "A VARCHAR(20),",
		},
	}
	err = createData(create, locked.Conn, argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	}, false)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("DROP TABLE %s", name), nil, schemaName)

	delete := &internal.TestData{
		Values: map[string]interface{}{},
	}
	err := deleteData(delete, locked.Conn, argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	})
	if err == nil {
		t.Fatal("Expected error")
	}

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (A VARCHAR(40))", name), nil, schemaName)

	err = deleteData(delete, locked.Conn, argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	})
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestComment(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (B VARCHAR(5), C VARCHAR(6) NOT NULL)", name), nil, schemaName)

	upd := &internal.TestData{
		Values: map[string]interface{}{
			"name:":   name,
			"schema":  schemaName,
			"comment": "Foo",
		},
	}

	err := updateData(upd, locked.Conn, argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	})
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestImport(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (B VARCHAR(5), C VARCHAR(6) NOT NULL)", name), nil, schemaName)

	imp := &internal.TestData{
		Values: map[string]interface{}{
			"name:":  name,
			"schema": schemaName,
		},
	}
	imp.SetId(resource.NewID(schemaName, name))

	err := importData(imp, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	imp = &internal.TestData{
		Values: map[string]interface{}{
			"name:":     name,
			"schema":    schemaName,
			"composite": "B VARCHAR(5)",
		},
	}
	imp.SetId(name)

	err = importData(imp, locked.Conn)
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

	oldName := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (A VARCHAR(10) COMMENT IS 'Foo')", oldName), nil, schemaName)

	newName := oldName + "_SHINY"
	rename := &internal.TestData{
		Values: map[string]interface{}{
			"name":      oldName,
			"schema":    schemaName,
			"composite": "A VARCHAR(10) COMMENT IS 'Foo'",
		},
		NewValues: map[string]interface{}{
			"name":      newName,
			"schema":    schemaName,
			"composite": "A VARCHAR(10) COMMENT IS 'Foo'",
		},
	}

	err := updateData(rename, locked.Conn, argument.RequiredArguments{
		Schema: schemaName,
		Name:   newName,
	})
	if err != nil {
		t.Fatal("Unknown error:", err)
	}

	read := &internal.TestData{
		Values: map[string]interface{}{
			"composite": "A VARCHAR(10) COMMENT IS 'Foo'",
		},
	}

	diags := readData(read, locked.Conn, argument.RequiredArguments{
		Schema: schemaName,
		Name:   newName,
	})
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

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (A VARCHAR(10), B VARCHAR(20), CONSTRAINT PK PRIMARY KEY (B), DISTRIBUTE BY A)", name), nil, schemaName)

	imp := &internal.TestData{
		Values: map[string]interface{}{
			"name:":     name,
			"schema":    schemaName,
			"composite": "A", // dummy value to trigger composite refresh
		},
	}
	imp.SetId(resource.NewID(schemaName, name))

	err := importData(imp, locked.Conn)
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
