package view

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/andreyvit/diff"
)

func TestCreate(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()
	locked.Conn.Execute(fmt.Sprintf("DROP VIEW %s", name), nil, schemaName)

	create := &internal.TestData{
		Values: map[string]interface{}{},
	}
	diags := createData(create, locked.Conn, RequiredCreateArguments{
		RequiredArguments: argument.RequiredArguments{
			Schema: schemaName,
			Name:   name,
		},
		subquery: "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS",
	}, false)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	res, err := locked.Conn.FetchSlice(fmt.Sprintf("SELECT COLUMN_SCHEMA FROM %s", name), nil, schemaName)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if len(res) == 0 {
		t.Fatal("Unexpected empty result")
	}
}

func TestColumn(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()
	locked.Conn.Execute(fmt.Sprintf("DROP VIEW %s", name), nil, schemaName)

	create := &internal.TestData{
		Values: map[string]interface{}{
			"column": []interface{}{
				map[string]interface{}{
					"name":    "Bar",
					"comment": "This is Baaar",
				},
			},
		},
	}
	diags := createData(create, locked.Conn, RequiredCreateArguments{
		RequiredArguments: argument.RequiredArguments{
			Schema: schemaName,
			Name:   name,
		},
		subquery: "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS",
	}, false)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	res, err := locked.Conn.FetchSlice(fmt.Sprintf("SELECT Bar FROM %s", name), nil, schemaName)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if len(res) == 0 {
		t.Fatal("Unexpected empty result")
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS", name), nil, schemaName)

	delete := &internal.TestData{
		Values: map[string]interface{}{
			"subquery": "REALLYREALLYFUCKEDUP,",
		},
	}
	diags := deleteData(delete, locked.Conn, argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	})
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	_, err := locked.Conn.Execute(fmt.Sprintf("SELECT COLUMN_TYPE FROM %s", name), nil, schemaName)
	if err == nil {
		t.Fatalf("Seems like View %s was not deleted", name)
	}
}

func TestComment(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS", name), nil, schemaName)

	upd := &internal.TestData{
		Values: map[string]interface{}{
			"subquery": "SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS",
		},
		NewValues: map[string]interface{}{
			"comment":  "Foo",
			"subquery": "SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS",
		},
	}

	diags := updateData(upd, locked.Conn, argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	})
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	res, err := locked.Conn.FetchSlice("SELECT VIEW_COMMENT FROM EXA_ALL_VIEWS WHERE UPPER(VIEW_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	actual := res[0][0]
	if actual != "Foo" {
		t.Fatalf("Expected comment Foo: %s", actual)
	}

}

func TestImport(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS", name), nil, schemaName)

	imp := &internal.TestData{
		Values: map[string]interface{}{
			"schema":   schemaName,
			"subquery": "SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS",
		},
	}
	imp.SetId(name)

	err := importData(imp, locked.Conn)
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

func TestUpdate(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()
	locked.Conn.Execute(fmt.Sprintf("DROP VIEW %s", name), nil, schemaName)

	create := &internal.TestData{
		Values: map[string]interface{}{},
	}
	args := argument.RequiredArguments{
		Schema: schemaName,
		Name:   name,
	}
	diags := createData(create, locked.Conn, RequiredCreateArguments{
		RequiredArguments: args,
		subquery:          "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS",
	}, false)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	read := &internal.TestData{
		Values: map[string]interface{}{},
	}
	diags = readData(read, locked.Conn, args)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	if read.Get("subquery").(string) != "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS" {
		t.Fatal("Unexpected subquery:", read.Get("subquery").(string))
	}

	locked.Conn.Execute(fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS", name), nil, schemaName)

	res, err := locked.Conn.FetchSlice("SELECT VIEW_TEXT FROM EXA_ALL_VIEWS WHERE UPPER(VIEW_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	text := res[0][0].(string)

	if text != fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS", name) {
		t.Fatal("Unexpected View text:", text)
	}

	read = &internal.TestData{
		Values: map[string]interface{}{},
	}
	diags = readData(read, locked.Conn, args)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	if read.Get("subquery").(string) != "SELECT COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS" {
		t.Fatal("Unexpected subquery:", read.Get("subquery").(string))
	}

	update := &internal.TestData{
		Values: map[string]interface{}{
			"subquery": "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS",
		},
	}
	diags = updateData(update, locked.Conn, args)
	if diags.HasError() {
		t.Fatal("Unexpected error:", diags)
	}

	res, err = locked.Conn.FetchSlice("SELECT VIEW_TEXT FROM EXA_ALL_VIEWS WHERE UPPER(VIEW_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	text = res[0][0].(string)
	if text == fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS", name) {
		t.Fatal("Unexpected text:", text)
	}
}
