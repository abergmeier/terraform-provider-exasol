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

	DeletePhysicalSchemaData(create, exaClient)

	err := CreatePhysicalSchemaData(create, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	defer DeletePhysicalSchemaData(create, exaClient)
}

func TestDeletePhysicalSchema(t *testing.T) {
	name := t.Name()

	delete := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	delete.SetId("foo")

	CreatePhysicalSchemaData(delete, exaClient)

	err := DeletePhysicalSchemaData(delete, exaClient)
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

	DeletePhysicalSchemaData(exists, exaClient)

	e, err := existsPhysicalSchemaData(exists, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if e {
		t.Fatal("Expected exists to be false")
	}

	CreatePhysicalSchemaData(exists, exaClient)

	defer DeletePhysicalSchemaData(exists, exaClient)

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
	_, err := exaClient.Conn.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	defer DeletePhysicalSchemaData(imp, exaClient)

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

	CreatePhysicalSchemaData(create, exaClient)

	defer DeletePhysicalSchemaData(create, exaClient)

	err := readPhysicalSchemaData(read, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if read.Id() != strings.ToUpper(name) {
		t.Fatalf("Expected Id to be %s: %s", strings.ToUpper(name), read.Id())
	}
}
