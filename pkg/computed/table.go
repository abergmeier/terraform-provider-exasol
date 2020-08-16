package computed

import (
	"strings"

	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform/helper/schema"
)

type TableReader struct {
	Columns       []interface{}
	ColumnIndices map[string]interface{}
	PrimaryKeys   map[string]interface{}
	ForeignKeys   map[string]interface{}
}

// ColumnIndicesSchema provides a fully computed Schema for Column Indices of a Table
func ColumnIndicesSchema() *schema.Schema {
	return &schema.Schema{
		Type:     schema.TypeMap,
		Computed: true,
		Elem: &schema.Schema{
			Type: schema.TypeInt,
		},
	}
}

// ColumnsSchema provides a fully computed Schema for Columns of a Table
func ColumnsSchema() *schema.Schema {
	return &schema.Schema{
		Type:     schema.TypeList,
		Computed: true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"name": {
					Type:     schema.TypeString,
					Computed: true,
				},
				"type": {
					Type:     schema.TypeString,
					Computed: true,
				},
			},
		},
	}
}

// ForeignKeysSchema provides a fully computed Schema for Foreign Keys of a Table
func ForeignKeysSchema() *schema.Schema {
	return &schema.Schema{
		Type:     schema.TypeMap,
		Computed: true,
		Elem: &schema.Schema{
			Type: schema.TypeInt,
		},
	}
}

// PrimaryKeysSchema provides a fully computed Schema for Primary Keys of a Table
func PrimaryKeysSchema() *schema.Schema {
	return &schema.Schema{
		Type:     schema.TypeMap,
		Computed: true,
		Elem: &schema.Schema{
			Type: schema.TypeInt,
		},
	}
}

// ReadTable reads necessary information of a Table
func ReadTable(c *exasol.Conn, schema, table string) (*TableReader, error) {
	tr := &TableReader{}
	var err error
	tr.Columns, tr.ColumnIndices, err = readColumns(c, schema, table)
	if err != nil {
		return nil, err
	}
	tr.PrimaryKeys, err = readPrimaryKeys(c, schema, table)
	if err != nil {
		return nil, err
	}
	tr.ForeignKeys, err = readForeignKeys(c, schema, table)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func readPrimaryKeys(c *exasol.Conn, schema, name string) (map[string]interface{}, error) {
	stmt := "SELECT COLUMN_NAME, ORDINAL_POSITION FROM EXA_ALL_CONSTRAINT_COLUMNS WHERE UPPER(CONSTRAINT_SCHEMA) = UPPER(?) AND UPPER(CONSTRAINT_TABLE) = UPPER(?) AND CONSTRAINT_TYPE = 'PRIMARY KEY'"
	cons, err := c.FetchSlice(stmt, []interface{}{
		schema,
		name,
	}, "SYS")
	if err != nil {
		return nil, err
	}

	pks := make(map[string]interface{}, len(cons))

	for _, values := range cons {
		name := values[0].(string)
		pks[strings.ToLower(name)] = int(values[1].(float64)+0.5) - 1
	}

	return pks, nil
}

func readForeignKeys(c *exasol.Conn, schema, name string) (map[string]interface{}, error) {
	stmt := "SELECT COLUMN_NAME, ORDINAL_POSITION FROM EXA_ALL_CONSTRAINT_COLUMNS WHERE UPPER(CONSTRAINT_SCHEMA) = UPPER(?) AND UPPER(CONSTRAINT_TABLE) = UPPER(?) AND CONSTRAINT_TYPE = 'FOREIGN KEY'"
	cons, err := c.FetchSlice(stmt, []interface{}{
		schema,
		name,
	}, "SYS")
	if err != nil {
		return nil, err
	}

	fks := make(map[string]interface{}, len(cons))

	for _, values := range cons {
		name := values[0].(string)
		fks[strings.ToLower(name)] = int(values[1].(float64)+0.5) - 1
	}

	return fks, nil
}

func readColumns(c *exasol.Conn, schema, table string) ([]interface{}, map[string]interface{}, error) {
	stmt := `SELECT COLUMN_ORDINAL_POSITION, COLUMN_NAME, COLUMN_TYPE
		FROM EXA_ALL_COLUMNS
		WHERE UPPER(COLUMN_SCHEMA) = UPPER(?) AND UPPER(COLUMN_TABLE) = UPPER(?)
		ORDER BY COLUMN_ORDINAL_POSITION`

	res, err := c.FetchSlice(stmt, []interface{}{
		schema,
		table,
	}, "SYS")
	if err != nil {
		return nil, nil, err
	}

	cols := make([]interface{}, len(res))
	colIndices := make(map[string]interface{}, len(res))

	for i, values := range res {
		cn := values[1].(string)
		col := map[string]interface{}{
			"name": cn,
			"type": values[2].(string),
		}
		cols[i] = col
		colIndices[strings.ToLower(cn)] = int(values[0].(float64)+0.5) - 1
	}

	return cols, colIndices, nil
}
