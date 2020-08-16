package datasources

import (
	"fmt"
	"sync"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/pkg/db"
	"github.com/abergmeier/terraform-exasol/pkg/resource"
)

type tableTest struct {
	stmt string
	test func(*testing.T, internal.Data)
}

type expected struct {
	name string
	t    string
}

// TestReadTableExasol tests all examples provided by Exasol.
func TestReadTableExasol(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()

	createStmts := map[string]tableTest{
		"t1": {
			stmt: `CREATE TABLE t1 (a VARCHAR(20),
	b DECIMAL(24,4) NOT NULL,
	c DECIMAL DEFAULT 122,
	d DOUBLE,
	e TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	f BOOL)`,
			test: t1Test,
		},
		"t2": {
			stmt: `CREATE TABLE t2 AS SELECT a,b,c+1 AS c FROM t1`,
			test: t2Test,
		},
		"t3": {
			stmt: `CREATE TABLE t3 AS SELECT count(*) AS my_count FROM t1 WITH NO DATA`,
			test: t3Test,
		},
		"t4": {
			stmt: `CREATE TABLE t4 LIKE t1`,
			test: t4Test,
		},
		"t5": {
			stmt: `CREATE TABLE t5 (id int IDENTITY PRIMARY KEY,
	LIKE t1 INCLUDING DEFAULTS,
	g DOUBLE,
	DISTRIBUTE BY a,b)`,
			test: t5Test,
		},
		"t6": {
			stmt: `CREATE TABLE t6 (order_id INT,
	order_price DOUBLE,
	order_date DATE,
	country VARCHAR(40),
	PARTITION BY order_date)`,
			test: t6Test,
		},
		"t7": {
			stmt: `SELECT * INTO TABLE t7 FROM t1`,
			test: t7Test,
		},
		"t8": {
			stmt: `CREATE TABLE t8 (ref_id int CONSTRAINT FK_T5 REFERENCES t5 (id) DISABLE, b VARCHAR(20))`,
			test: t8Test,
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

	wg := sync.WaitGroup{}

	for _, ref := range stmtsRef {
		expectedID := resource.NewID(schemaName, ref)

		test := createStmts[ref]
		stmt := test.stmt

		tryDropTable := func() {
			stmt := fmt.Sprintf("DROP TABLE %s", ref)
			locked.Conn.Execute(stmt, nil, schemaName)
			locked.Conn.Commit()
		}

		tryDropTable()

		_, err := locked.Conn.Execute(stmt, nil, schemaName)
		if err != nil {
			t.Fatal("Unexpected error:", err)
		}
		defer tryDropTable()

		db.MustCommit(locked.Conn)

		read := &internal.TestData{
			Values: map[string]interface{}{
				"name":   ref,
				"schema": schemaName,
			},
		}
		func() {
			locked := exaClient.Lock()
			defer locked.Unlock()
			err = readData(read, locked.Conn)
			if err != nil {
				t.Fatal("Unexpected error:", err)
			}
		}()
		n := read.Get("name")
		readName, _ := n.(string)
		if readName != ref {
			t.Fatalf("Expected name %s: %#v", ref, n)
		}
		s := read.Get("schema")
		sn, _ := s.(string)
		if sn != schemaName {
			t.Fatalf("Expected schema %s: %#v", schemaName, s)
		}
		if read.Id() != expectedID {
			t.Fatalf("Expected id %s: %s", expectedID, read.Id())
		}

		wg.Add(1)
		func() {
			defer wg.Done()
			test.test(t, read)
		}()
	}

	wg.Wait()
}

func testColumns(t *testing.T, d internal.Data, expected []expected) {
	cols := d.Get("columns")
	colList := cols.([]interface{})

	if len(expected) != len(colList) {
		t.Errorf("Expected %d elements: %d", len(expected), len(colList))
	}

	for i, e := range expected {
		cfs := colList[i].(map[string]interface{})
		name := cfs["name"].(string)
		if name != e.name {
			t.Errorf("Name mismatch at %d. Expected %s: %s", i, e.name, name)
		}
		ct := cfs["type"].(string)
		if ct != e.t {
			t.Errorf("Type mismatch at %d. Expected %s: %s", i, e.t, ct)
		}
	}
}

func t1Test(t *testing.T, d internal.Data) {

	expected := []expected{
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
	testColumns(t, d, expected)
	testForeignKeys(t, d, nil)
	testPrimaryKeys(t, d, nil)
}

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

func testForeignKeys(t *testing.T, d internal.Data, expected map[string]interface{}) {
	fks := d.Get("foreign_key_indices").(map[string]interface{})
	if len(fks) != len(expected) {
		t.Errorf("Expected %d foreign keys: %d", len(expected), len(fks))
		return
	}

	if len(expected) == 0 {
		return
	}

	for ek, expectedIndex := range expected {
		actualIndex, ok := fks[ek].(int)
		if !ok {
			t.Fatal("Not found:", ek)
		}

		if expectedIndex.(int) != actualIndex {
			t.Fatalf("Expected primary key to have index %d: %d", expectedIndex.(int), actualIndex)
		}
	}
}

func testPrimaryKeys(t *testing.T, d internal.Data, expected map[string]interface{}) {
	pks := d.Get("primary_key_indices").(map[string]interface{})
	if len(pks) != len(expected) {
		t.Errorf("Expected %d primary keys: %d", len(expected), len(pks))
		return
	}

	if len(expected) == 0 {
		return
	}

	for ek, expectedIndex := range expected {
		actualIndex, ok := pks[ek].(int)
		if !ok {
			t.Fatal("Not found:", ek)
		}

		if expectedIndex.(int) != actualIndex {
			t.Fatalf("Expected primary key to have index %d: %d", expectedIndex.(int), actualIndex)
		}
	}
}
