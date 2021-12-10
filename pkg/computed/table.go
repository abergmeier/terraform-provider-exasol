package computed

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type tableColumns struct {
	cols        []interface{}
	indices     map[string]interface{}
	distributes []string
}

type TableReader struct {
	Columns       []interface{}
	ColumnIndices map[string]interface{}
	Comment       string
	Composite     string
	PrimaryKeys   map[string]interface{}
	ForeignKeys   map[string]interface{}
	distributes   []string
}

func (tr *TableReader) SetComment(d internal.Data) error {
	return setComment(tr.Comment, d)
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
				"comment": {
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
func ReadTable(ctx context.Context, tx *sql.Tx, schema, table string) (*TableReader, error) {
	tr := &TableReader{}
	var err error
	tcs, err := readTableColumns(ctx, tx, schema, table)
	if err != nil {
		return nil, err
	}
	tr.Columns = tcs.cols
	tr.ColumnIndices = tcs.indices
	tr.Comment, err = readComment(ctx, tx, schema, table)
	if err != nil {
		return nil, err
	}
	tr.PrimaryKeys, err = readPrimaryKeys(ctx, tx, schema, table)
	if err != nil {
		return nil, err
	}
	tr.ForeignKeys, err = readForeignKeys(ctx, tx, schema, table)
	if err != nil {
		return nil, err
	}

	stmt := `SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_IS_NULLABLE
FROM SYS.EXA_ALL_COLUMNS
WHERE UPPER(COLUMN_SCHEMA) = UPPER(?) AND UPPER(COLUMN_TABLE) = UPPER(?)
ORDER BY COLUMN_ORDINAL_POSITION`
	res, err := tx.QueryContext(ctx, stmt, schema, table)
	if err != nil {
		return nil, err
	}

	b := &strings.Builder{}
	for i := 0; res.Next(); i++ {
		var cn string
		var ct string
		var nullable bool
		err := res.Scan(&cn, &ct, &nullable)
		if err != nil {
			return nil, err
		}
		colInfo := tr.Columns[i].(map[string]interface{})
		b.WriteString(colInfo["name"].(string))
		b.WriteString(" ")
		b.WriteString(colInfo["type"].(string))
		if nullable {
			b.WriteString(" NULL")
		} else {
			b.WriteString(" NOT NULL")
		}

		comment, ok := colInfo["comment"]
		if ok && comment != "" {
			fmt.Fprintf(b, " COMMENT IS '%s',\n", comment)
		} else {
			b.WriteString(",\n")
		}
	}
	for columnName := range tr.PrimaryKeys {
		fmt.Fprintf(b, "CONSTRAINT PRIMARY KEY (%s),\n", strings.ToUpper(columnName))
	}

	if len(tcs.distributes) > 0 {
		dl := strings.Join(tcs.distributes, ", ")
		fmt.Fprintf(b, "DISTRIBUTE BY %s,\n", strings.ToUpper(dl))
	}
	tr.Composite = b.String()
	return tr, nil
}

func readComment(ctx context.Context, tx *sql.Tx, schema, name string) (string, error) {
	stmt := "SELECT TABLE_COMMENT FROM SYS.EXA_ALL_TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER(?) AND UPPER(TABLE_NAME) = UPPER(?)"
	res, err := tx.QueryContext(ctx, stmt, schema, name)
	if err != nil {
		return "", err
	}

	if !res.Next() {
		return "", nil
	}

	var comment interface{}
	err = res.Scan(&comment)
	if err != nil {
		return "", err
	}

	if comment == nil {
		return "", nil
	}

	return comment.(string), nil
}

func readPrimaryKeys(ctx context.Context, tx *sql.Tx, schema, name string) (map[string]interface{}, error) {
	stmt := "SELECT COLUMN_NAME, ORDINAL_POSITION FROM SYS.EXA_ALL_CONSTRAINT_COLUMNS WHERE UPPER(CONSTRAINT_SCHEMA) = UPPER(?) AND UPPER(CONSTRAINT_TABLE) = UPPER(?) AND CONSTRAINT_TYPE = 'PRIMARY KEY'"
	cons, err := tx.QueryContext(ctx, stmt, schema, name)
	if err != nil {
		return nil, err
	}

	pks := map[string]interface{}{}

	for cons.Next() {
		var name string
		var op float64
		err := cons.Scan(&name, &op)
		if err != nil {
			return nil, err
		}
		pks[strings.ToLower(name)] = int(op+0.5) - 1
	}

	return pks, nil
}

func readForeignKeys(ctx context.Context, tx *sql.Tx, schema, name string) (map[string]interface{}, error) {
	stmt := "SELECT COLUMN_NAME, ORDINAL_POSITION FROM SYS.EXA_ALL_CONSTRAINT_COLUMNS WHERE UPPER(CONSTRAINT_SCHEMA) = UPPER(?) AND UPPER(CONSTRAINT_TABLE) = UPPER(?) AND CONSTRAINT_TYPE = 'FOREIGN KEY'"
	cons, err := tx.QueryContext(ctx, stmt, schema, name)
	if err != nil {
		return nil, err
	}

	fks := map[string]interface{}{}

	for cons.Next() {
		var name string
		var op float64
		err := cons.Scan(&name, &op)
		if err != nil {
			return nil, err
		}
		fks[strings.ToLower(name)] = int(op+0.5) - 1
	}

	return fks, nil
}

func readTableColumns(ctx context.Context, tx *sql.Tx, schema, table string) (tableColumns, error) {
	stmt := `SELECT COLUMN_ORDINAL_POSITION, COLUMN_NAME, COLUMN_TYPE, COLUMN_IS_DISTRIBUTION_KEY, COLUMN_COMMENT
FROM SYS.EXA_ALL_COLUMNS
WHERE UPPER(COLUMN_SCHEMA) = UPPER(?) AND UPPER(COLUMN_TABLE) = UPPER(?)
ORDER BY COLUMN_ORDINAL_POSITION`

	res, err := tx.QueryContext(ctx, stmt, schema, table)
	if err != nil {
		return tableColumns{}, err
	}

	tcs := tableColumns{
		cols:    []interface{}{},
		indices: map[string]interface{}{},
	}

	for res.Next() {
		var op float64
		var cn string
		var t string
		var isDistributionColumn bool
		var c interface{}
		err = res.Scan(&op, &cn, &t, &isDistributionColumn, &c)
		if err != nil {
			return tableColumns{}, err
		}
		col := map[string]interface{}{
			"name": cn,
			"type": t,
		}

		if c != nil {
			comment, _ := c.(string)
			col["comment"] = comment
		}

		tcs.cols = append(tcs.cols, col)
		if isDistributionColumn {
			tcs.distributes = append(tcs.distributes, cn)
		}
		tcs.indices[strings.ToLower(cn)] = int(op+0.5) - 1
	}

	return tcs, nil
}
