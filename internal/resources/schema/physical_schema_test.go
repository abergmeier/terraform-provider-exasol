package schema

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/test"
)

func TestCreatePhysicalSchema(t *testing.T) {
	t.Parallel()

	conn := test.OpenManualConnectionInTest(t, exaClient)
	defer conn.Close()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	create := &test.Data{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deletePhysicalSchemaData(create, conn.Conn)

	err := createPhysicalSchemaData(create, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestDeletePhysicalSchema(t *testing.T) {
	t.Parallel()

	conn := test.OpenManualConnectionInTest(t, exaClient)
	defer conn.Close()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	delete := &test.Data{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	delete.SetId("foo")

	createPhysicalSchemaData(delete, conn.Conn)

	err := deletePhysicalSchemaData(delete, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if delete.Id() != "" {
		t.Fatal("Expected id reset")
	}
}

func TestExistsPhysicalSchema(t *testing.T) {
	t.Parallel()

	conn := test.OpenManualConnectionInTest(t, exaClient)
	defer conn.Close()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	exists := &test.Data{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deletePhysicalSchemaData(exists, conn.Conn)

	e, err := existsPhysicalSchemaData(exists, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if e {
		t.Fatal("Expected exists to be false")
	}

	createPhysicalSchemaData(exists, conn.Conn)

	e, err = existsPhysicalSchemaData(exists, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if !e {
		t.Fatal("Expected exists to be true")
	}
}

func TestImportPhysicalSchema(t *testing.T) {
	t.Parallel()

	conn := test.OpenManualConnectionInTest(t, exaClient)
	defer conn.Close()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	imp := &test.Data{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	imp.SetId("TestImportPhysicalSchemaWithOtherName")

	stmt := "CREATE SCHEMA IF NOT EXISTS TestImportPhysicalSchemaWithOtherName"
	_, err := conn.Conn.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	err = importPhysicalSchemaData(imp, conn.Conn)
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

	conn := test.OpenManualConnectionInTest(t, exaClient)
	defer conn.Close()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	create := &test.Data{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	read := &test.Data{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	createPhysicalSchemaData(create, conn.Conn)

	err := readPhysicalSchemaData(read, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if read.Id() != strings.ToUpper(name) {
		t.Fatalf("Expected Id to be %s: %s", strings.ToUpper(name), read.Id())
	}
}

func TestRenamePhysicalSchema(t *testing.T) {
	t.Parallel()

	conn := test.OpenManualConnectionInTest(t, exaClient)
	defer conn.Close()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	create := &test.Data{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	createPhysicalSchemaData(create, conn.Conn)

	newName := name + "_SHINY"
	rename := &test.Data{
		Values: map[string]interface{}{
			"name": name,
		},
		NewValues: map[string]interface{}{
			"name": newName,
		},
	}

	err := updatePhysicalSchemaData(rename, conn.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	name = rename.Get("name").(string)
	if name != newName {
		t.Fatalf("Expected name to be %s: %s", newName, name)
	}
}
