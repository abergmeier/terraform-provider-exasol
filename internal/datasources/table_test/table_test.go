package table_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/datasources/test"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"

	"github.com/grantstreetgroup/go-exasol-client"
)

type tableTest struct {
	stmt string
	test func(*testing.T, internal.Data)
}

type expectedColumns struct {
	name string
	t    string
}

// TestReadTableExasol tests all examples provided by Exasol.
func TestAccExasolTable_basic(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()

	basicSetup(t, locked.Conn)

	resource.Test(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s

data "exasol_physical_schema" "dummy" {
	name = "%s"
}
data "exasol_table" "t1" {
	name = "t1"
	schema = data.exasol_physical_schema.dummy.name
}
`, test.ProviderInHCL(locked), schemaName),
				Check: resource.ComposeTestCheckFunc(
					testT1Columns("data.exasol_table.t1"),
					testT1ForeignKeys("data.exasol_table.t1"),
					testT1PrimaryKeys("data.exasol_table.t1"),
				),
			},
		},
	})
}

func basicSetup(t *testing.T, c *exasol.Conn) {
	createStmts := map[string]tableTest{
		"t1": {
			stmt: `CREATE TABLE t1 (a VARCHAR(20),
	b DECIMAL(24,4) NOT NULL,
	c DECIMAL DEFAULT 122,
	d DOUBLE,
	e TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	f BOOL)`,
		},
		"t2": {
			stmt: `CREATE TABLE t2 AS SELECT a,b,c+1 AS c FROM t1`,
		},
		"t3": {
			stmt: `CREATE TABLE t3 AS SELECT count(*) AS my_count FROM t1 WITH NO DATA`,
		},
		"t4": {
			stmt: `CREATE TABLE t4 LIKE t1`,
		},
		"t5": {
			stmt: `CREATE TABLE t5 (id int IDENTITY PRIMARY KEY,
	LIKE t1 INCLUDING DEFAULTS,
	g DOUBLE,
	DISTRIBUTE BY a,b)`,
		},
		"t6": {
			stmt: `CREATE TABLE t6 (order_id INT,
	order_price DOUBLE,
	order_date DATE,
	country VARCHAR(40),
	PARTITION BY order_date)`,
		},
		"t7": {
			stmt: `SELECT * INTO TABLE t7 FROM t1`,
		},
		"t8": {
			stmt: `CREATE TABLE t8 (ref_id int CONSTRAINT FK_T5 REFERENCES t5 (id) DISABLE, b VARCHAR(20))`,
		},
	}

	stmtsRef := []string{
		"t1",
		"t2",
		"t3",
		"t4",
		"t5",
		"t6",
		"t7",
		"t8",
	}

	for _, ref := range stmtsRef {

		test := createStmts[ref]
		stmt := test.stmt

		tryDropTable(ref, c)

		_, err := c.Execute(stmt, nil, schemaName)
		if err != nil {
			t.Fatal("Unexpected error:", err)
		}
	}
}

func tryDropTable(ref string, c *exasol.Conn) {
	stmt := fmt.Sprintf("DROP TABLE %s", ref)
	c.Execute(stmt, nil, schemaName)
}

func testColumns(state *terraform.State, id string, expected []expectedColumns) error {

	return testWithResource(state, id, func(ds *terraform.ResourceState) error {

		countString, ok := ds.Primary.Attributes["columns.#"]
		if !ok {
			return fmt.Errorf("Column count not found: %s", id)
		}
		count, err := strconv.Atoi(countString)
		if err != nil {
			return err
		}

		if len(expected) != count {
			return fmt.Errorf("Expected %d elements: %d", len(expected), count)
		}

		for i := 0; i != count; i++ {
			e := expected[i]

			name := ds.Primary.Attributes[fmt.Sprintf("columns.%d.name", i)]
			if name != e.name {
				return fmt.Errorf("Name mismatch at %d. Expected %s: %s", i, e.name, name)
			}
			ct := ds.Primary.Attributes[fmt.Sprintf("columns.%d.type", i)]
			if ct != e.t {
				return fmt.Errorf("Type mismatch at %d. Expected %s: %s", i, e.t, ct)
			}
		}
		return nil
	})
}

func testT1Columns(id string) resource.TestCheckFunc {

	return func(state *terraform.State) error {
		expected := []expectedColumns{
			{
				name: "A",
				t:    "VARCHAR(20) UTF8",
			},
			{
				name: "B",
				t:    "DECIMAL(24,4)",
			}, // NOT NULL,
			{
				name: "C",
				t:    "DECIMAL(18,0)",
			}, // DECIMAL DEFAULT 122,
			{
				name: "D",
				t:    "DOUBLE",
			},
			{
				name: "E",
				t:    "TIMESTAMP",
			}, //  DEFAULT CURRENT_TIMESTAMP,
			{
				name: "F",
				t:    "BOOLEAN",
			},
		}

		return testColumns(state, id, expected)
	}
}

func testT1ForeignKeys(id string) resource.TestCheckFunc {
	return func(state *terraform.State) error {
		return testForeignKeys(state, id, nil)
	}
}

func testT1PrimaryKeys(id string) resource.TestCheckFunc {
	return func(state *terraform.State) error {
		return testPrimaryKeys(state, id, nil)
	}
}

/*
func t2Test(t *testing.T, d internal.Data) {

	expected := []expected{
		{
			name: "A",
			t:    "VARCHAR(20) UTF8",
		},
		{
			name: "B",
			t:    "DECIMAL(24,4)",
		}, //  NOT NULL,
		{
			name: "C",
			t:    "DECIMAL(19,0)",
		}, //  DEFAULT 122,
	}
	testColumns(t, d, expected)
	testForeignKeys(t, d, nil)
	testPrimaryKeys(t, d, nil)
}

func t3Test(t *testing.T, d internal.Data) {

	expected := []expected{
		{
			name: "MY_COUNT",
			t:    "DECIMAL(18,0)",
		},
	}
	testColumns(t, d, expected)
	testForeignKeys(t, d, nil)
	testPrimaryKeys(t, d, nil)
}

func t4Test(t *testing.T, d internal.Data) {

	expected := []expected{
		{
			name: "A",
			t:    "VARCHAR(20) UTF8",
		},
		{
			name: "B", //  NOT NULL,
			t:    "DECIMAL(24,4)",
		},
		{
			name: "C", //  DEFAULT 122,
			t:    "DECIMAL(18,0)",
		},
		{
			name: "D",
			t:    "DOUBLE",
		},
		{
			name: "E", //  DEFAULT CURRENT_TIMESTAMP,
			t:    "TIMESTAMP",
		},
		{
			name: "F",
			t:    "BOOLEAN",
		},
	}
	testColumns(t, d, expected)
	testForeignKeys(t, d, nil)
	testPrimaryKeys(t, d, nil)
}

func t5Test(t *testing.T, d internal.Data) {
	expected := []expected{
		{
			name: "ID",
			t:    "DECIMAL(18,0)",
		},
		{
			name: "A",
			t:    "VARCHAR(20) UTF8",
		},
		{
			name: "B",
			t:    "DECIMAL(24,4)",
		},
		{
			name: "C",
			t:    "DECIMAL(18,0)",
		},
		{
			name: "D",
			t:    "DOUBLE",
		},
		{
			name: "E",
			t:    "TIMESTAMP",
		},
		{
			name: "F",
			t:    "BOOLEAN",
		},
		{
			name: "G",
			t:    "DOUBLE",
		},
	}
	testColumns(t, d, expected)
	testForeignKeys(t, d, nil)
	testPrimaryKeys(t, d, map[string]interface{}{
		"id": 0,
	})
}

func t6Test(t *testing.T, d internal.Data) {
	expected := []expected{
		{
			name: "ORDER_ID",
			t:    "DECIMAL(18,0)",
		},
		{
			name: "ORDER_PRICE",
			t:    "DOUBLE",
		},
		{
			name: "ORDER_DATE",
			t:    "DATE",
		},
		{
			name: "COUNTRY",
			t:    "VARCHAR(40) UTF8",
		},
	}
	testColumns(t, d, expected)
	testForeignKeys(t, d, nil)
	testPrimaryKeys(t, d, nil)
}

func t7Test(t *testing.T, d internal.Data) {
	expected := []expected{
		{
			name: "A",
			t:    "VARCHAR(20) UTF8",
		},
		{
			name: "B",
			t:    "DECIMAL(24,4)", // NOT NULL,
		},
		{
			name: "C",
			t:    "DECIMAL(18,0)", // DEFAULT 122,
		},
		{
			name: "D",
			t:    "DOUBLE",
		},
		{
			name: "E",
			t:    "TIMESTAMP", // DEFAULT CURRENT_TIMESTAMP,
		},
		{
			name: "F",
			t:    "BOOLEAN",
		},
	}
	testColumns(t, d, expected)
	testForeignKeys(t, d, nil)
	testPrimaryKeys(t, d, nil)
}

func t8Test(t *testing.T, d internal.Data) {
	expected := []expected{
		{
			name: "REF_ID",
			t:    "DECIMAL(18,0)",
		},
		{
			name: "B",
			t:    "VARCHAR(20) UTF8",
		},
	}
	testColumns(t, d, expected)
	testForeignKeys(t, d, map[string]interface{}{
		"ref_id": 0,
	})
	testPrimaryKeys(t, d, nil)
}
*/

func testWithResource(state *terraform.State, id string, f func(ds *terraform.ResourceState) error) error {

	ds, ok := state.RootModule().Resources[id]
	if !ok {
		return fmt.Errorf("Datasource not found: %s", id)
	}

	return f(ds)
}

func testForeignKeys(state *terraform.State, id string, expected map[string]int) error {

	return testWithResource(state, id, func(ds *terraform.ResourceState) error {

		countString, ok := ds.Primary.Attributes["foreign_key_indices.#"]
		if !ok {
			return fmt.Errorf("No count of foreign_key_indices found: %s", id)
		}

		count, err := strconv.Atoi(countString)
		if err != nil {
			return err
		}

		if count != len(expected) {
			return fmt.Errorf("Expected %d foreign keys: %d", len(expected), count)
		}

		for ek, expectedIndex := range expected {
			actualIndexString, ok := ds.Primary.Attributes[fmt.Sprintf("foreign_key_indices.%s", ek)]
			if !ok {
				return fmt.Errorf("Not found: %s.foreign_key_indices.%s", id, ek)
			}

			actualIndex, err := strconv.Atoi(actualIndexString)
			if err != nil {
				return err
			}

			if expectedIndex != actualIndex {
				return fmt.Errorf("Expected foreign key to have index %d: %d", expectedIndex, actualIndex)
			}
		}
		return nil
	})
}

func testPrimaryKeys(state *terraform.State, id string, expected map[string]int) error {

	return testWithResource(state, id, func(ds *terraform.ResourceState) error {
		countString, ok := ds.Primary.Attributes["primary_key_indices.#"]
		if !ok {
			return fmt.Errorf("primary_key_indices.# not found %s", id)
		}

		count, err := strconv.Atoi(countString)
		if err != nil {
			return err
		}

		if count != len(expected) {
			return fmt.Errorf("Expected %d primary keys: %d", len(expected), count)
		}

		for ek, expectedIndex := range expected {
			actualIndexString, ok := ds.Primary.Attributes[fmt.Sprintf("primary_key_indices.%s", ek)]
			if !ok {
				return fmt.Errorf("Not found: %s.primary_key_indices.%s", id, ek)
			}
			actualIndex, err := strconv.Atoi(actualIndexString)
			if err != nil {
				return err
			}

			if expectedIndex != actualIndex {
				return fmt.Errorf("Expected primary key to have index %d: %d", expectedIndex, actualIndex)
			}
		}
		return nil
	})
}
