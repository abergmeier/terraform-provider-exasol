package table

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"

	"errors"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/computed"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/abergmeier/terraform-provider-exasol/pkg/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/customdiff"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// Resource for Exasol Table
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
				Type:         schema.TypeString,
				Optional:     true,
				Description:  "Composite declarations as in CREATE TABLE FOO (<composite>)",
				ExactlyOneOf: []string{"composite", "like", "subquery"},
			},
			"subquery": {
				Type:         schema.TypeString,
				Optional:     true,
				Description:  "Subquery declaration as in CREATE TABLE FOO AS <subquery>",
				ExactlyOneOf: []string{"composite", "like", "subquery"},
			},
			"like": {
				Type:         schema.TypeString,
				Optional:     true,
				Description:  "Like declaration as in CREATE TABLE FOO LIKE <like>",
				ExactlyOneOf: []string{"composite", "like", "subquery"},
			},
			"comment": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Comment for the Table",
			},
			"replace": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Allows for replacing Table inplace",
			},
			"column_indices":      computed.ColumnIndicesSchema(),
			"columns":             computed.ColumnsSchema(),
			"primary_key_indices": computed.PrimaryKeysSchema(),
			"foreign_key_indices": computed.ForeignKeysSchema(),
		},
		CustomizeDiff: customdiff.All(
			customdiff.ForceNewIf("composite", isReplaceFalse),
			customdiff.ForceNewIf("subquery", isReplaceFalse),
			customdiff.ForceNewIf("like", isReplaceFalse),
		),
		CreateContext: create,
		ReadContext:   read,
		UpdateContext: update,
		DeleteContext: delete,
		Importer: &schema.ResourceImporter{
			StateContext: imp,
		},
	}
}

func isReplaceFalse(ctx context.Context, d *schema.ResourceDiff, meta interface{}) bool {
	return !d.Get("replace").(bool)
}

func create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	ra, diags := argument.ExtractRequiredArguments(d)
	if diags.HasError() {
		return diags
	}
	diags = append(diags, diag.FromErr(createData(ctx, d, locked.Tx, ra, false))...)
	if diags.HasError() {
		return diags
	}
	err := locked.Tx.Commit()
	return append(diags, diag.FromErr(err)...)
}

func createData(ctx context.Context, d internal.Data, tx *sql.Tx, args argument.RequiredArguments, replace bool) error {

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

	err := createDataMutate(d, tx, args.Schema, args.Name, comp, like, subquery, replace)
	if err != nil {
		return err
	}

	return postCreate(ctx, d, tx, args.Schema, args.Name)
}

func postCreate(ctx context.Context, d internal.Data, tx *sql.Tx, schema, name string) error {

	tr, err := computed.ReadTable(ctx, tx, schema, name)
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
func createDataMutate(d internal.Data, tx *sql.Tx, schema, name string, comp, like, subquery interface{}, replace bool) error {

	initWords := "CREATE TABLE"
	if replace {
		initWords = "CREATE OR REPLACE TABLE"
	}

	commentSuffix := ""
	comment, _ := d.Get("comment").(string)
	if comment != "" {
		commentSuffix = fmt.Sprintf(" COMMENT IS '%s'", comment)
	}

	var err error
	if !reflect.ValueOf(comp).IsZero() {
		cleaned := strings.Trim(comp.(string), ",\n ")
		stmt := fmt.Sprintf("%s %s.%s (%s)%s", initWords, schema, name, cleaned, commentSuffix)
		setStmtHash("composite", stmt, d)
		_, err = tx.Exec(stmt)
	} else if !reflect.ValueOf(like).IsZero() {
		stmt := fmt.Sprintf("%s %s.%s LIKE %s%s", initWords, schema, name, like.(string), commentSuffix)
		setStmtHash("like", stmt, d)
		_, err = tx.Exec(stmt)
	} else if !reflect.ValueOf(subquery).IsZero() {
		stmt := fmt.Sprintf("%s %s.%s AS %s%s", initWords, schema, name, subquery.(string), commentSuffix)
		setStmtHash("subquery", stmt, d)
		_, err = tx.Exec(stmt)
	} else {
		panic("Internal conditions wrong")
	}

	return err
}

func delete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	ra, diags := argument.ExtractRequiredArguments(d)
	if diags.HasError() {
		return diags
	}
	err := deleteData(d, locked.Tx, ra)
	if err != nil {
		return append(diags, diag.FromErr(err)...)
	}
	err = locked.Tx.Commit()
	return append(diags, diag.FromErr(err)...)
}

func deleteData(d internal.Data, tx *sql.Tx, args argument.RequiredArguments) error {

	stmt := fmt.Sprintf("DROP TABLE %s.%s", args.Schema, args.Name)
	_, err := tx.Exec(stmt)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}

func imp(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := importData(ctx, d, locked.Tx)
	if err != nil {
		return nil, err
	}
	err = locked.Tx.Commit()
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importData(ctx context.Context, d internal.Data, tx *sql.Tx) error {
	id := d.Id()

	m, err := resource.GetMetaFromQNDefault(id, d.Get("schema").(string))
	if err != nil {
		return err
	}

	if len(strings.TrimSpace(m.Schema)) == 0 {
		return errors.New("Missing schema in import")
	}

	err = d.Set("name", m.ObjectName)
	if err != nil {
		return err
	}
	err = d.Set("schema", m.Schema)
	if err != nil {
		return err
	}

	tr, err := computed.ReadTable(ctx, tx, m.Schema, m.ObjectName)
	if err != nil {
		return err
	}

	err = tr.SetComment(d)
	if err != nil {
		return err
	}

	handled := false
	_, ok := d.GetOk("like")
	if !handled && ok {
		handled = true
	}

	_, ok = d.GetOk("subquery")
	if !handled && ok {
		handled = true
	}

	_, ok = d.GetOk("composite")
	if !handled && ok {
		// Update composite value
		err = d.Set("composite", tr.Composite)
		if err != nil {
			return err
		}
	}

	return postCreate(ctx, d, tx, m.Schema, m.ObjectName)
}

func read(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	ra, diags := argument.ExtractRequiredArguments(d)
	if diags.HasError() {
		return diags
	}
	return append(diags, readData(ctx, d, locked.Tx, ra)...)
}

func readData(ctx context.Context, d internal.Data, tx *sql.Tx, args argument.RequiredArguments) diag.Diagnostics {

	tr, err := computed.ReadTable(ctx, tx, args.Schema, args.Name)
	if err != nil {
		return diag.FromErr(err)
	}

	err = tr.SetComment(d)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("columns", tr.Columns)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("column_indices", tr.ColumnIndices)
	if err != nil {
		return diag.FromErr(err)
	}

	handled := false
	_, ok := d.GetOk("like")
	if !handled && ok {
		handled = true
	}

	_, ok = d.GetOk("subquery")
	if !handled && ok {
		handled = true
	}

	_, ok = d.GetOk("composite")
	if !handled && ok {
		// Update composite value
		err = d.Set("composite", tr.Composite)
		if err != nil {
			return diag.FromErr(err)
		}
	}

	err = d.Set("primary_key_indices", tr.PrimaryKeys)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("foreign_key_indices", tr.ForeignKeys)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(resource.NewID(args.Schema, args.Name))
	return nil
}

func update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	ra, diags := argument.ExtractRequiredArguments(d)
	if diags.HasError() {
		return diags
	}
	diags = append(diags, updateData(ctx, d, locked.Tx, ra)...)
	if diags.HasError() {
		return diags
	}
	err := locked.Tx.Commit()
	return append(diags, diag.FromErr(err)...)
}

func updateData(ctx context.Context, d internal.Data, tx *sql.Tx, args argument.RequiredArguments) diag.Diagnostics {

	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.Rename(tx, "TABLE", old.(string), new.(string), args.Schema)
		if err != nil {
			return diag.FromErr(err)
		}
	}

	replaceNecessary := d.HasChange("composite") || d.HasChange("subquery") || d.HasChange("like")
	if replaceNecessary {
		err := createData(ctx, d, tx, argument.RequiredArguments{
			Schema: args.Schema,
			Name:   d.Get("name").(string),
		}, true)
		if err != nil {
			return diag.FromErr(err)
		}
	} else if d.HasChange("comment") {
		err := db.Comment(tx, "TABLE", d.Get("name").(string), d.Get("comment").(string), args.Schema)
		if err != nil {
			return diag.FromErr(err)
		}
	}

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

func setStmtHash(variant, stmt string, d internal.Data) {
	stmtHash, err := internal.HashStrings(variant, stmt)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		// ignore error
	}

	d.Set("hash_stmt", string(stmtHash))
}
