package table

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"errors"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-exasol/pkg/argument"
	"github.com/abergmeier/terraform-exasol/pkg/computed"
	"github.com/abergmeier/terraform-exasol/pkg/db"
	"github.com/abergmeier/terraform-exasol/pkg/resource"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func Resource() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Table",
			},
			"schema": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Schema to create Table in",
				ForceNew:    true,
			},
			"composite": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Composite declarations as in CREATE TABLE FOO (<composite>)",
				ForceNew:    true,
			},
			"subquery": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Subquery declaration as in CREATE TABLE FOO AS <subquery>",
				ForceNew:    true,
			},
			"like": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Like declaration as in CREATE TABLE FOO LIKE <like>",
				ForceNew:    true,
			},
			"column_indices":      computed.ColumnIndicesSchema(),
			"columns":             computed.ColumnsSchema(),
			"primary_key_indices": computed.PrimaryKeysSchema(),
			"foreign_key_indices": computed.ForeignKeysSchema(),
		},
		Create: create,
		Read:   read,
		Update: update,
		Delete: delete,
		Exists: exists,
		Importer: &schema.ResourceImporter{
			State: imp,
		},
	}
}

func create(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := createData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func createData(d internal.Data, c *exasol.Conn) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	schema, err := argument.Schema(d)
	if err != nil {
		return err
	}

	comp := d.Get("composite")
	like := d.Get("like")
	subquery := d.Get("subquery")

	cNil := countEmpty(comp, like, subquery)
	if cNil == 3 {
		return errors.New("Need to set one of composite, like and subquery")
	}

	if cNil != 2 {
		return fmt.Errorf("Only one of composite, like or subquery may be used %#v", like)
	}

	err = createDataMutate(d, c, schema, name, comp, like, subquery)
	if err != nil {
		return err
	}

	return postCreate(d, c, schema, name)
}

func postCreate(d internal.Data, c *exasol.Conn, schema, name string) error {

	state, err := fetchMaterializedColumns(c, schema, name)
	if err != nil {
		return err
	}
	setMaterializedColumnHash(state, d)

	tr, err := computed.ReadTable(c, schema, name)
	if err != nil {
		return err
	}

	err = d.Set("columns", tr.Columns)
	if err != nil {
		return err
	}

	err = d.Set("column_indices", tr.ColumnIndices)
	if err != nil {
		return err
	}

	err = d.Set("primary_key_indices", tr.PrimaryKeys)
	if err != nil {
		return err
	}

	err = d.Set("foreign_key_indices", tr.ForeignKeys)
	if err != nil {
		return err
	}

	d.SetId(resource.NewID(schema, name))
	return nil
}

// createDataMutate contains the mutating part of creating a Table
func createDataMutate(d internal.Data, c *exasol.Conn, schema, name string, comp, like, subquery interface{}) error {

	if !reflect.ValueOf(comp).IsZero() {
		stmt := fmt.Sprintf("CREATE TABLE %s (%s)", name, comp.(string))
		setStmtHash("composite", stmt, d)
		_, err := c.Execute(stmt, nil, schema)
		if err != nil {
			return err
		}
	} else if !reflect.ValueOf(like).IsZero() {
		stmt := fmt.Sprintf("CREATE TABLE %s LIKE %s", name, like.(string))
		setStmtHash("like", stmt, d)
		_, err := c.Execute(stmt, nil, schema)
		if err != nil {
			return err
		}
	} else if !reflect.ValueOf(subquery).IsZero() {
		stmt := fmt.Sprintf("CREATE TABLE %s AS %s", name, subquery.(string))
		setStmtHash("subquery", stmt, d)
		_, err := c.Execute(stmt, nil, schema)
		if err != nil {
			return err
		}
	} else {
		panic("Internal conditions wrong")
	}

	return c.Commit()
}

func delete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := deleteData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func deleteData(d internal.Data, c *exasol.Conn) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	schema, err := argument.Schema(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("DROP TABLE %s", name)
	_, err = c.Execute(stmt, nil, schema)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}

func exists(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	return existsData(d, locked.Conn)
}

func existsData(d internal.Data, c *exasol.Conn) (bool, error) {
	name, err := argument.Name(d)
	if err != nil {
		return false, err
	}
	schema, err := argument.Schema(d)
	if err != nil {
		return false, err
	}

	res, err := c.FetchSlice("SELECT TABLE_OWNER FROM EXA_ALL_TABLES WHERE UPPER(TABLE_SCHEMA) = UPPER(?) AND UPPER(TABLE_NAME) = UPPER(?)", []interface{}{
		schema,
		name,
	}, "SYS")
	if err != nil {
		return false, err
	}

	return len(res) != 0, nil
}

func imp(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := importData(d, locked.Conn)
	if err != nil {
		return nil, err
	}
	err = locked.Conn.Commit()
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importData(d internal.Data, c *exasol.Conn) error {
	id := d.Id()

	schema, name, err := resource.SplitIDInSchema(id)
	if err != nil {
		return err
	}

	stmt := `SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_IS_NULLABLE
FROM EXA_ALL_COLUMNS
WHERE UPPER(COLUMN_SCHEMA) = UPPER(?) AND UPPER(COLUMN_TABLE) = UPPER(?)
ORDER BY COLUMN_ORDINAL_POSITION`
	res, err := c.FetchSlice(stmt, []interface{}{
		schema,
		name,
	})
	if err != nil {
		return err
	}

	b := strings.Builder{}
	for _, column := range res {
		b.WriteString(column[0].(string))
		b.WriteString(" ")
		b.WriteString(column[1].(string))
		nullable := column[2].(bool)
		if !nullable {
			b.WriteString(" NOT NULL")
		}
		b.WriteString("\n")
	}

	err = d.Set("name", name)
	if err != nil {
		return err
	}
	err = d.Set("schema", schema)
	if err != nil {
		return err
	}
	err = d.Set("composite", b.String())
	if err != nil {
		return err
	}

	return postCreate(d, c, schema, name)
}

func read(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	return readData(d, locked.Conn)
}

func readData(d internal.Data, c *exasol.Conn) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	schema, err := argument.Schema(d)
	if err != nil {
		return err
	}

	tr, err := computed.ReadTable(c, schema, name)
	if err != nil {
		return err
	}

	err = d.Set("columns", tr.Columns)
	if err != nil {
		return err
	}

	err = d.Set("column_indices", tr.ColumnIndices)
	if err != nil {
		return err
	}

	err = d.Set("primary_key_indices", tr.PrimaryKeys)
	if err != nil {
		return err
	}

	err = d.Set("foreign_key_indices", tr.ForeignKeys)
	if err != nil {
		return err
	}

	d.SetId(resource.NewID(schema, name))
	return nil
}

func update(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := updateData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func updateData(d internal.Data, c *exasol.Conn) error {

	schema, err := argument.Schema(d)
	if err != nil {
		return err
	}

	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.Rename(c, "TABLE", old.(string), new.(string), schema)
		if err != nil {
			return err
		}

		d.Set("name", new)
	}

	_ = d.Get("name").(string)
	return nil
}

func countEmpty(elems ...interface{}) int {
	i := 0

	for _, elem := range elems {
		if elem == nil || reflect.ValueOf(elem).IsZero() {
			i++
		}
	}
	return i
}

func fetchMaterializedColumns(c *exasol.Conn, schema, table string) ([][]interface{}, error) {
	stmt := `SELECT COLUMN_OBJECT_TYPE, COLUMN_NAME, COLUMN_TYPE,
COLUMN_TYPE_ID, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE,
COLUMN_IS_VIRTUAL, COLUMN_IS_NULLABLE, COLUMN_IS_DISTRIBUTION_KEY, COLUMN_PARTITION_KEY_ORDINAL_POSITION, COLUMN_DEFAULT,
COLUMN_IDENTITY FROM EXA_ALL_COLUMNS WHERE UPPER(COLUMN_SCHEMA) = UPPER(?) AND UPPER(COLUMN_TABLE) = UPPER(?) ORDER BY COLUMN_ORDINAL_POSITION`

	res, err := c.FetchSlice(stmt, []interface{}{
		schema,
		table,
	}, "SYS")
	if err != nil {
		return nil, err
	}

	return res, nil
}

func setMaterializedColumnHash(res [][]interface{}, d internal.Data) {
	columnHash, err := internal.HashUnknown(res...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		// ignore error
	}

	d.Set("hash_columns", string(columnHash))
}

func setStmtHash(variant, stmt string, d internal.Data) {
	stmtHash, err := internal.HashStrings(variant, stmt)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		// ignore error
	}

	d.Set("hash_stmt", string(stmtHash))
}
