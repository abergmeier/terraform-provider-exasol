package view

import (
	"context"
	"strings"

	"errors"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/statements"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/computed"
	"github.com/abergmeier/terraform-provider-exasol/pkg/resource"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var (
	Column = &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Column",
			},
			"comment": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Comment of Column",
			},
		},
	}
)

// Resource for Exasol View
func Resource() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of View",
				ForceNew:    true,
			},
			"schema": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Schema to create View in",
				ForceNew:    true,
			},
			"column": {
				Type:        schema.TypeList,
				Elem:        Column,
				Optional:    true,
				Description: "Columns to expose",
				DefaultFunc: func() (interface{}, error) {
					return []interface{}{}, nil
				},
			},
			"subquery": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Subquery declaration as in CREATE VIEW FOO AS <subquery>",
			},
			"comment": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Comment for the View",
			},
			"replace": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Allows for replacing View inplace",
			},
		},
		CreateContext: create,
		ReadContext:   read,
		UpdateContext: update,
		DeleteContext: delete,
		Importer: &schema.ResourceImporter{
			State: imp,
		},
	}
}

func isReplaceFalse(ctx context.Context, d *schema.ResourceDiff, meta interface{}) bool {
	return !d.Get("replace").(bool)
}

type RequiredCreateArguments struct {
	argument.RequiredArguments
	subquery string
}

func requiredCreateArguments(d *schema.ResourceData) (RequiredCreateArguments, diag.Diagnostics) {
	ra, diags := argument.ExtractRequiredArguments(d)
	subquery, ok := d.Get("subquery").(string)
	if !ok {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Warning,
			Summary:  "subquery argument ignored: not of type string",
		})
	}
	if diags.HasError() {
		return RequiredCreateArguments{}, diags
	}
	return RequiredCreateArguments{
		RequiredArguments: ra,
		subquery:          subquery,
	}, diags
}

func create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	ca, diags := requiredCreateArguments(d)
	if diags.HasError() {
		return diags
	}
	diags = append(diags, createData(d, locked.Conn, ca, false)...)
	if diags.HasError() {
		return diags
	}
	return diag.FromErr(locked.Conn.Commit())
}

func appendColumns(columns []statements.ViewColumn, d internal.Data) []statements.ViewColumn {
	columniface, ok := d.GetOk("column")
	if !ok {
		return columns
	}
	listiface := columniface.([]interface{})
	for _, columniface := range listiface {
		c := columniface.(map[string]interface{})
		commentiface, ok := c["comment"]
		var comment string
		if ok {
			comment = commentiface.(string)
		}
		column := statements.ViewColumn{
			Name:    c["name"].(string),
			Comment: comment,
		}
		columns = append(columns, column)
	}
	return columns
}

func createData(d internal.Data, c *exasol.Conn, args RequiredCreateArguments, replace bool) diag.Diagnostics {

	diags := diag.Diagnostics{}
	comment, _ := argument.GetOkAsString(d, "comment")

	var columns []statements.ViewColumn
	columns = appendColumns(columns, d)

	cv := statements.CreateView{
		Schema:   args.Schema,
		Name:     args.Name,
		Columns:  columns,
		Subquery: args.subquery,
		Comment:  comment,
		Replace:  replace,
	}

	err := cv.Execute(c)
	if err != nil {
		return append(diags, diag.FromErr(err)...)
	}

	d.SetId(resource.NewID(args.Schema, args.Name))
	return diags
}

func delete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	ra, diags := argument.ExtractRequiredArguments(d)
	if diags.HasError() {
		return diags
	}
	diags = append(diags, deleteData(d, locked.Conn, ra)...)
	if diags.HasError() {
		return diags
	}
	return append(diags, diag.FromErr(locked.Conn.Commit())...)
}

func deleteData(d internal.Data, c *exasol.Conn, args argument.RequiredArguments) diag.Diagnostics {

	dv := statements.DropView{
		Schema: args.Schema,
		Name:   args.Name,
	}
	err := dv.Execute(c)
	if err != nil {
		return diag.FromErr(err)
	}
	d.SetId("")
	return nil
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

	m, err := resource.GetMetaFromQNDefault(id, d.Get("schema").(string))
	if err != nil {
		return err
	}

	if len(strings.TrimSpace(m.Schema)) == 0 {
		return errors.New("missing schema in import")
	}

	err = d.Set("name", m.ObjectName)
	if err != nil {
		return err
	}
	err = d.Set("schema", m.Schema)
	if err != nil {
		return err
	}

	tv, err := computed.ReadView(c, m.Schema, m.ObjectName)
	if err != nil {
		return err
	}

	err = tv.SetComment(d)
	if err != nil {
		return err
	}

	d.SetId(resource.NewID(m.Schema, m.ObjectName))
	return nil
}

func read(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	ra, diags := argument.ExtractRequiredArguments(d)
	if diags.HasError() {
		return diags
	}
	return readData(d, locked.Conn, ra)
}

func readData(d internal.Data, c *exasol.Conn, args argument.RequiredArguments) diag.Diagnostics {

	tr, err := computed.ReadView(c, args.Schema, args.Name)
	if err != nil {
		return diag.FromErr(err)
	}

	err = tr.SetComment(d)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("subquery", tr.Subquery)
	if err != nil {
		return diag.FromErr(err)
	}

	err = d.Set("column", tr.Columns)
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	ra, diags := argument.ExtractRequiredArguments(d)
	if diags.HasError() {
		return diags
	}
	diags = append(diags, updateData(d, locked.Conn, ra)...)
	if diags.HasError() {
		return diags
	}
	return append(diags, diag.FromErr(locked.Conn.Commit())...)
}

func updateData(d internal.Data, c *exasol.Conn, args argument.RequiredArguments) diag.Diagnostics {

	var diags diag.Diagnostics
	replaceNecessary := d.HasChange("subquery") || d.HasChange("comment")
	if replaceNecessary {
		diags = createData(d, c, RequiredCreateArguments{
			RequiredArguments: args,
			subquery:          d.Get("subquery").(string),
		}, true)
		if diags.HasError() {
			return diags
		}
	}

	return diags
}
