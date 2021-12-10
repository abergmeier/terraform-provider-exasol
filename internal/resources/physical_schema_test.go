package resources

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
)

func TestCreatePhysicalSchemaResource(t *testing.T) {
	t.Parallel()

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deletePhysicalSchemaData(create, locked.Tx)

	err := createPhysicalSchemaData(create, locked.Tx)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestDeletePhysicalSchemaResource(t *testing.T) {
	t.Parallel()

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	delete := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	delete.SetId("foo")

	createPhysicalSchemaData(delete, locked.Tx)

	err := deletePhysicalSchemaData(delete, locked.Tx)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if delete.Id() != "" {
		t.Fatal("Expected id reset")
	}
}

func TestImportPhysicalSchemaResource(t *testing.T) {
	t.Parallel()

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	imp := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	imp.SetId("TestImportPhysicalSchemaWithOtherName")

	stmt := "CREATE SCHEMA IF NOT EXISTS TestImportPhysicalSchemaWithOtherName"
	_, err := locked.Tx.Exec(stmt)
	if err != nil {
		t.Fatal(err)
	}

	err = importPhysicalSchemaData(context.TODO(), imp, locked.Tx)
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

func TestReadPhysicalSchemaResource(t *testing.T) {
	t.Parallel()

	locked := exaprovider.TestLock(t, exaClient)
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

	createPhysicalSchemaData(create, locked.Tx)

	err := readPhysicalSchemaTx(context.TODO(), read, locked.Tx)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if read.Id() != strings.ToUpper(name) {
		t.Fatalf("Expected Id to be %s: %s", strings.ToUpper(name), read.Id())
	}
}

func TestRenamePhysicalSchemaResource(t *testing.T) {
	t.Parallel()

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	createPhysicalSchemaData(create, locked.Tx)

	newName := name + "_SHINY"
	rename := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
		NewValues: map[string]interface{}{
			"name": newName,
		},
	}

	err := updatePhysicalSchemaData(rename, locked.Tx)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	name = rename.Get("name").(string)
	if name != newName {
		t.Fatalf("Expected name to be %s: %s", newName, name)
	}
}
