package table_test

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
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

// TestAccExasolTable_basic all examples provided by Exasol.
func TestAccExasolTable_basic(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()

	basicSetup(t, locked.Conn)

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
					testColumns("data.exasol_table.t1", expected),
					testPrimaryKeys("data.exasol_table.t1", nil),
					testForeignKeys("data.exasol_table.t1", nil),
				),
			},
		},
	})

	expected = []expectedColumns{
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
	resource.Test(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s

data "exasol_physical_schema" "dummy" {
	name = "%s"
}
data "exasol_table" "t2" {
	name = "t2"
	schema = data.exasol_physical_schema.dummy.name
}
`, test.ProviderInHCL(locked), schemaName),
				Check: resource.ComposeTestCheckFunc(
					testColumns("data.exasol_table.t2", expected),
					testPrimaryKeys("data.exasol_table.t2", nil),
					testForeignKeys("data.exasol_table.t2", nil),
				),
			},
		},
	})

	expected = []expectedColumns{
		{
			name: "MY_COUNT",
			t:    "DECIMAL(18,0)",
		},
	}
	resource.Test(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s

data "exasol_physical_schema" "dummy" {
	name = "%s"
}
data "exasol_table" "t3" {
	name = "t3"
	schema = data.exasol_physical_schema.dummy.name
}
`, test.ProviderInHCL(locked), schemaName),
				Check: resource.ComposeTestCheckFunc(
					testColumns("data.exasol_table.t3", expected),
					testPrimaryKeys("data.exasol_table.t3", nil),
					testForeignKeys("data.exasol_table.t3", nil),
				),
			},
		},
	})

	expected = []expectedColumns{
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
	resource.Test(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s

data "exasol_physical_schema" "dummy" {
	name = "%s"
}
data "exasol_table" "t4" {
	name = "t4"
	schema = data.exasol_physical_schema.dummy.name
}
`, test.ProviderInHCL(locked), schemaName),
				Check: resource.ComposeTestCheckFunc(
					testColumns("data.exasol_table.t4", expected),
					testPrimaryKeys("data.exasol_table.t4", nil),
					testForeignKeys("data.exasol_table.t4", nil),
				),
			},
		},
	})

	expected = []expectedColumns{
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
	resource.Test(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s

data "exasol_physical_schema" "dummy" {
	name = "%s"
}
data "exasol_table" "t5" {
	name = "t5"
	schema = data.exasol_physical_schema.dummy.name
}
`, test.ProviderInHCL(locked), schemaName),
				Check: resource.ComposeTestCheckFunc(
					testColumns("data.exasol_table.t5", expected),
					testPrimaryKeys("data.exasol_table.t5", map[string]int{
						"id": 0,
					}),
					testForeignKeys("data.exasol_table.t5", nil),
				),
			},
		},
	})

	expected = []expectedColumns{
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
	resource.Test(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s

data "exasol_physical_schema" "dummy" {
	name = "%s"
}
data "exasol_table" "t6" {
	name = "t6"
	schema = data.exasol_physical_schema.dummy.name
}
`, test.ProviderInHCL(locked), schemaName),
				Check: resource.ComposeTestCheckFunc(
					testColumns("data.exasol_table.t6", expected),
					testPrimaryKeys("data.exasol_table.t6", nil),
					testForeignKeys("data.exasol_table.t6", nil),
				),
			},
		},
	})

	expected = []expectedColumns{
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
	resource.Test(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s

data "exasol_physical_schema" "dummy" {
	name = "%s"
}
data "exasol_table" "t7" {
	name = "t7"
	schema = data.exasol_physical_schema.dummy.name
}
`, test.ProviderInHCL(locked), schemaName),
				Check: resource.ComposeTestCheckFunc(
					testColumns("data.exasol_table.t7", expected),
					testPrimaryKeys("data.exasol_table.t7", nil),
					testForeignKeys("data.exasol_table.t7", nil),
				),
			},
		},
	})

	expected = []expectedColumns{
		{
			name: "REF_ID",
			t:    "DECIMAL(18,0)",
		},
		{
			name: "B",
			t:    "VARCHAR(20) UTF8",
		},
	}
	resource.Test(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s

data "exasol_physical_schema" "dummy" {
	name = "%s"
}
data "exasol_table" "t8" {
	name = "t8"
	schema = data.exasol_physical_schema.dummy.name
}
`, test.ProviderInHCL(locked), schemaName),
				Check: resource.ComposeTestCheckFunc(
					testColumns("data.exasol_table.t8", expected),
					testPrimaryKeys("data.exasol_table.t8", nil),
					testForeignKeys("data.exasol_table.t8", map[string]int{
						"ref_id": 0,
					}),
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

func testColumns(id string, expected []expectedColumns) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		ds, err := testDatasource(state, id)
		if err != nil {
			return err
		}

		fmt.Printf("%#v\n", ds.Primary.Attributes)
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
	}
}

func testDatasource(state *terraform.State, id string) (*terraform.ResourceState, error) {

	ds, ok := state.RootModule().Resources[id]
	if !ok {
		return nil, fmt.Errorf("Datasource not found: %s", id)
	}

	return ds, nil
}

func testForeignKeys(id string, expected map[string]int) resource.TestCheckFunc {

	if expected == nil {
		expected = map[string]int{}
	}

	return func(state *terraform.State) error {
		ds, err := testDatasource(state, id)
		if err != nil {
			return err
		}

		actual := map[string]int{}

		for k, v := range ds.Primary.Attributes {
			if strings.HasPrefix(k, "foreign_key_indices.") && !strings.HasSuffix(k, ".%") {
				actualIndex, err := strconv.Atoi(v)
				if err != nil {
					return err
				}
				nonPrefixedKey := k[len("foreign_key_indices."):]
				actual[nonPrefixedKey] = actualIndex
			}
		}

		if !reflect.DeepEqual(&actual, &expected) {
			return fmt.Errorf(`Foreign Key mismatch:
	Expected %#v
	Actual   %#v`, expected, actual)
		}

		return nil
	}
}

func testPrimaryKeys(id string, expected map[string]int) resource.TestCheckFunc {

	if expected == nil {
		expected = map[string]int{}
	}

	return func(state *terraform.State) error {

		ds, err := testDatasource(state, id)
		if err != nil {
			return err
		}

		actual := map[string]int{}

		for k, v := range ds.Primary.Attributes {
			if strings.HasPrefix(k, "primary_key_indices.") && !strings.HasSuffix(k, ".%") {
				actualIndex, err := strconv.Atoi(v)
				if err != nil {
					return err
				}
				nonPrefixedKey := k[len("primary_key_indices."):]
				actual[nonPrefixedKey] = actualIndex
			}
		}

		if !reflect.DeepEqual(&actual, &expected) {
			return fmt.Errorf(`Primary Key mismatch:
	Expected %#v
	Actual   %#v`, expected, actual)
		}

		return nil
	}
}
