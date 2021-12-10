package view

import (
	"context"
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/statements"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/tx"
	"github.com/andreyvit/diff"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func testResourceData() *schema.ResourceData {

	testResource := &schema.Resource{
		Schema: Resource().Schema,
	}
	return testResource.TestResourceData()
}

func TestViewResourceCreate(t *testing.T) {
	t.Parallel()

	create := testResourceData()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()
	locked.Tx.Exec(fmt.Sprintf("DROP VIEW %s.%s", schemaName, name))

	diags := createData(context.TODO(), create, locked.Tx, RequiredCreateArguments{
		RequiredArguments: argument.RequiredArguments{
			Schema: schemaName,
			Name:   name,
		},
		subquery: "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS",
	}, false)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	res, err := locked.Tx.Query(fmt.Sprintf("SELECT COLUMN_SCHEMA FROM %s.%s", schemaName, name))
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if !res.Next() {
		t.Fatal("Unexpected empty result")
	}
}

func TestViewResourceColumn(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()
	locked.Tx.Exec(fmt.Sprintf("DROP VIEW %s.%s", schemaName, name))

	create := testResourceData()
	create.Set("column", []interface{}{
		map[string]interface{}{
			"name":    "Bar",
			"comment": "This is Baaar",
		},
	})
	diags := createData(context.TODO(), create, locked.Tx, RequiredCreateArguments{
		RequiredArguments: argument.RequiredArguments{
			Schema: schemaName,
			Name:   name,
		},
		subquery: "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS",
	}, false)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	res, err := locked.Tx.Query(fmt.Sprintf("SELECT Bar FROM %s.%s", schemaName, name))
	if err != nil {
		t.Fatal("Unexpected error in View Column test:", err)
	}
	if !res.Next() {
		t.Fatal("Unexpected empty result")
	}
}

func TestViewResourceDelete(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()

	tx.MustExecf(locked.Tx, "CREATE OR REPLACE VIEW %s.%s AS SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS", schemaName, name)

	delete := testResourceData()
	delete.Set("subquery", "REALLYREALLYFUCKEDUP,")
	diags := deleteData(delete, locked.Tx, argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	})
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	_, err := locked.Tx.Exec(fmt.Sprintf("SELECT COLUMN_TYPE FROM %s.%s", schemaName, name))
	if err == nil {
		t.Fatalf("Seems like View %s was not deleted", name)
	}
}

func TestViewResourceImport(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()

	locked.Tx.Exec(fmt.Sprintf("CREATE OR REPLACE VIEW %s.%s AS SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS", schemaName, name))

	imp := testResourceData()
	imp.Set("schema", schemaName)
	imp.Set("subquery", "SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS")
	imp.SetId(name)

	err := importData(context.TODO(), imp, locked.Tx)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	actualSubquery := imp.Get("subquery").(string)
	expectedSubquery := "SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS"

	if actualSubquery != expectedSubquery {
		ld := diff.LineDiff(actualSubquery, expectedSubquery)

		t.Fatalf("Unexpected subquery value:\n%s", ld)
	}
}

func TestViewResourceUpdate(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()
	locked.Tx.Exec(fmt.Sprintf("DROP VIEW %s.%s", schemaName, name))

	create := testResourceData()
	args := argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	}
	diags := createData(context.TODO(), create, locked.Tx, RequiredCreateArguments{
		RequiredArguments: args,
		subquery:          "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS",
	}, false)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	read := testResourceData()
	diags = readData(context.TODO(), read, locked.Tx, args)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	if read.Get("subquery").(string) != "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS" {
		t.Fatal("Unexpected subquery:", read.Get("subquery").(string))
	}

	tx.MustExecf(locked.Tx, "CREATE OR REPLACE VIEW %s.%s AS SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS", schemaName, name)

	res, err := locked.Tx.Query("SELECT VIEW_TEXT FROM SYS.EXA_ALL_VIEWS WHERE UPPER(VIEW_NAME) = UPPER(?)", name)
	if err != nil {
		t.Fatal("Unexpected error in View Update test:", err)
	}
	if !res.Next() {
		t.Fatal("No results found in View Update test")
	}
	var text string
	err = res.Scan(&text)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if text != fmt.Sprintf("CREATE OR REPLACE VIEW %s.%s AS SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS", schemaName, name) {
		t.Fatal("Unexpected View text:", text)
	}

	read = testResourceData()
	diags = readData(context.TODO(), read, locked.Tx, args)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	if read.Get("subquery").(string) != "SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS" {
		t.Fatal("Unexpected subquery:", read.Get("subquery").(string))
	}

	update := testResourceData()
	update.Set("subquery", "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS")
	diags = updateData(context.TODO(), update, locked.Tx, args)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	res, err = locked.Tx.Query("SELECT VIEW_TEXT FROM SYS.EXA_ALL_VIEWS WHERE UPPER(VIEW_NAME) = UPPER(?)", name)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if !res.Next() {
		t.Fatal("No view_text found in View Update test")
	}
	err = res.Scan(&text)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if text == fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS", name) {
		t.Fatal("Unexpected text:", text)
	}
}

func TestViewResourceUpdateEdgeCase(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaprovider.TestLock(t, exaClient)
	defer locked.Unlock()
	locked.Tx.Exec(fmt.Sprintf("DROP VIEW %s.%s", schemaName, name))

	create := Resource().TestResourceData()

	args := argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	}
	diags := createData(context.TODO(), create, locked.Tx, RequiredCreateArguments{
		RequiredArguments: args,
		subquery:          "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS",
	}, false)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	dv := statements.DropView{
		Schema: args.Schema,
		Name:   args.Name,
	}
	err := dv.Execute(locked.Tx)
	if err != nil {
		t.Fatal(err)
	}

	diags = readData(context.TODO(), create, locked.Tx, args)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}
}
