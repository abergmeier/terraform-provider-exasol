package table

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/pkg/resource"
)

func TestCreate(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	createErr := &internal.TestData{
		Values: map[string]interface{}{
			"name":   name,
			"schema": schemaName,
		},
	}

	locked := exaClient.Lock()
	defer locked.Unlock()
	locked.Conn.Execute(fmt.Sprintf("DROP TABLE %s", name), nil, schemaName)

	err := createData(createErr, locked.Conn)
	if err == nil {
		t.Fatal("Expected error when createData")
	}

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name":      name,
			"schema":    schemaName,
			"composite": "A VARCHAR(20)",
		},
	}
	err = createData(create, locked.Conn)
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
		Values: map[string]interface{}{
			"name":   name,
			"schema": schemaName,
		},
	}
	err := deleteData(delete, locked.Conn)
	if err == nil {
		t.Fatal("Expected error")
	}

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (A VARCHAR(40))", name), nil, schemaName)

	err = deleteData(delete, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestExists(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("DROP TABLE %s", name), nil, schemaName)

	exists := &internal.TestData{
		Values: map[string]interface{}{
			"name":   name,
			"schema": schemaName,
		},
	}
	e, err := existsData(exists, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if e {
		t.Fatal("Expected false")
	}

	locked.Conn.Execute(fmt.Sprintf("CREATE TABLE %s (A VARCHAR(40))", name), nil, schemaName)

	e, err = existsData(exists, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if !e {
		t.Fatal("Expected true")
	}
}

func TestImport(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (B VARCHAR(5))", name), nil, schemaName)

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
}

func TestRename(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE TABLE %s (A VARCHAR(10))", name), nil, schemaName)

	newName := name + "_SHINY"
	rename := &internal.TestData{
		Values: map[string]interface{}{
			"name":      name,
			"schema":    schemaName,
			"composite": "A VARCHAR(10)",
		},
		NewValues: map[string]interface{}{
			"name":      newName,
			"schema":    schemaName,
			"composite": "A VARCHAR(10)",
		},
	}

	err := updateData(rename, locked.Conn)
	if err != nil {
		t.Fatal("Unknown error:", err)
	}
}
