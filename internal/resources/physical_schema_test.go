package resources

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
)

func TestCreatePhysicalSchema(t *testing.T) {
	t.Parallel()

	locked := exaClient.Lock()
	defer locked.Unlock()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deletePhysicalSchemaData(create, locked.Conn)

	err := createPhysicalSchemaData(create, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestDeletePhysicalSchema(t *testing.T) {
	t.Parallel()

	locked := exaClient.Lock()
	defer locked.Unlock()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	delete := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	delete.SetId("foo")

	createPhysicalSchemaData(delete, locked.Conn)

	err := deletePhysicalSchemaData(delete, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if delete.Id() != "" {
		t.Fatal("Expected id reset")
	}
}

func TestImportPhysicalSchema(t *testing.T) {
	t.Parallel()

	locked := exaClient.Lock()
	defer locked.Unlock()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	imp := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	imp.SetId("TestImportPhysicalSchemaWithOtherName")

	stmt := "CREATE SCHEMA IF NOT EXISTS TestImportPhysicalSchemaWithOtherName"
	_, err := locked.Conn.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	err = importPhysicalSchemaData(imp, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if imp.Id() != strings.ToUpper("TestImportPhysicalSchemaWithOtherName") {
		t.Fatalf("Expected id %s: %s", strings.ToUpper(name), imp.Id())
	}
	if imp.Get("name").(string) != name {
		t.Fatalf("Expected name %s: %s", name, imp.Get("name").(string))
	}
}

func TestReadPhysicalSchema(t *testing.T) {
	t.Parallel()

	locked := exaClient.Lock()
	defer locked.Unlock()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	read := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	createPhysicalSchemaData(create, locked.Conn)

	err := readPhysicalSchemaData(read, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if read.Id() != strings.ToUpper(name) {
		t.Fatalf("Expected Id to be %s: %s", strings.ToUpper(name), read.Id())
	}
}

func TestRenamePhysicalSchema(t *testing.T) {
	t.Parallel()

	locked := exaClient.Lock()
	defer locked.Unlock()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	createPhysicalSchemaData(create, locked.Conn)

	newName := name + "_SHINY"
	rename := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
		NewValues: map[string]interface{}{
			"name": newName,
		},
	}

	err := updatePhysicalSchemaData(rename, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	name = rename.Get("name").(string)
	if name != newName {
		t.Fatalf("Expected name to be %s: %s", newName, name)
	}
}
