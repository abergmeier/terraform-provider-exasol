package resources

import (
	"strings"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
)

func TestCreatePhysicalSchema(t *testing.T) {
	name := t.Name()

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deletePhysicalSchemaData(create, exaClient)

	err := createPhysicalSchemaData(create, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	defer deletePhysicalSchemaData(create, exaClient)
}

func TestDeletePhysicalSchema(t *testing.T) {
	name := t.Name()

	delete := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	delete.SetId("foo")

	createPhysicalSchemaData(delete, exaClient)

	err := deletePhysicalSchemaData(delete, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if delete.Id() != "" {
		t.Fatal("Expected id reset")
	}
}

func TestExistsPhysicalSchema(t *testing.T) {
	name := t.Name()

	exists := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deletePhysicalSchemaData(exists, exaClient)

	e, err := existsPhysicalSchemaData(exists, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if e {
		t.Fatal("Expected exists to be false")
	}

	createPhysicalSchemaData(exists, exaClient)

	defer deletePhysicalSchemaData(exists, exaClient)

	e, err = existsPhysicalSchemaData(exists, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if !e {
		t.Fatal("Expected exists to be true")
	}
}

func TestImportPhysicalSchema(t *testing.T) {
	name := t.Name()

	imp := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	imp.SetId("TestImportPhysicalSchemaWithOtherName")

	stmt := "CREATE SCHEMA IF NOT EXISTS TestImportPhysicalSchemaWithOtherName"
	_, err := exaClient.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	defer deletePhysicalSchemaData(imp, exaClient)

	err = importPhysicalSchemaData(imp, exaClient)
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
	name := t.Name()

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

	createPhysicalSchemaData(create, exaClient)

	defer deletePhysicalSchemaData(create, exaClient)

	err := readPhysicalSchemaData(read, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if read.Id() != strings.ToUpper(name) {
		t.Fatalf("Expected Id to be %s: %s", strings.ToUpper(name), read.Id())
	}
}

func TestRenamePhysicalSchema(t *testing.T) {
	name := t.Name()

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	createPhysicalSchemaData(create, exaClient)

	defer deletePhysicalSchemaData(create, exaClient)

	rename := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
		NewValues: map[string]interface{}{
			"name": "NEWANDSHINY",
		},
	}

	err := updatePhysicalSchemaData(rename, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	delete := &internal.TestData{
		Values: map[string]interface{}{
			"name": "NEWANDSHINY",
		},
	}
	defer deletePhysicalSchemaData(delete, exaClient)
}
