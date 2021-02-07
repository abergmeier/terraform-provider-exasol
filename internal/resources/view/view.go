package view

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/computed"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/abergmeier/terraform-provider-exasol/pkg/resource"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
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
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Composite declarations as in CREATE VIEW FOO (<composite>)",
			},
			"subquery": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Subquery declaration as in CREATE VIEW FOO AS <subquery>",
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
		},
		CreateContext: create,
		ReadContext:   read,
		UpdateContext: update,
		DeleteContext: delete,
		Exists:        exists,
		Importer: &schema.ResourceImporter{
			State: imp,
		},
	}
}

func create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := createData(d, locked.Conn, false)
	if err != nil {
		return diag.FromErr(err)
	}
	return diag.FromErr(locked.Conn.Commit())
}

func createData(d internal.Data, c *exasol.Conn, replace bool) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	schema, err := argument.Schema(d)
	if err != nil {
		return err
	}

	ci := d.Get("composite")
	composite := ""
	if ci != nil {
		composite = ci.(string)
	}

	subquery := d.Get("subquery").(string)

	err = createView(d, c, schema, name, composite, subquery, replace)
	if err != nil {
		return err
	}

	d.SetId(resource.NewID(schema, name))
	return nil
}

func createView(d internal.Data, c *exasol.Conn, schema, name, composite, subquery string, replace bool) error {
	initWords := "CREATE VIEW"
	if replace {
		initWords = "CREATE OR REPLACE VIEW"
	}

	commentSuffix := ""
	comment, ok := d.Get("comment").(string)
	if comment != "" && ok {
		commentSuffix = fmt.Sprintf(" COMMENT IS '%s'", comment)
	}

	var stmt string
	if composite == "" {
		cleaned := strings.Trim(composite, ",\n ")
		stmt = fmt.Sprintf("%s %s (%s) AS %s%s", initWords, name, cleaned, subquery, commentSuffix)
	} else {
		stmt = fmt.Sprintf("%s %s AS %s%s", initWords, name, subquery, commentSuffix)
	}
	_, err := c.Execute(stmt, nil, schema)
	return err
}

func delete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := deleteData(d, locked.Conn)
	if err != nil {
		return diag.FromErr(err)
	}
	return diag.FromErr(locked.Conn.Commit())
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

	stmt := fmt.Sprintf("DROP VIEW %s", name)
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

	res, err := c.FetchSlice("SELECT VIEW_OWNER FROM EXA_ALL_VIEWS WHERE UPPER(VIEW_SCHEMA) = UPPER(?) AND UPPER(VIEW_NAME) = UPPER(?)", []interface{}{
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

	vr, err := computed.ReadView(c, m.Schema, m.ObjectName)
	if err != nil {
		return err
	}

	err = vr.SetComment(d)
	if err != nil {
		return err
	}

	if vr.Composite == "" {
		d.Set("composite", nil)
	} else {
		d.Set("composite", vr.Composite)
	}

	d.Set("subquery", vr.Subquery)
	return nil
}

func read(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	return diag.FromErr(readData(d, locked.Conn))
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

	vr, err := computed.ReadView(c, schema, name)
	if err != nil {
		return err
	}

	err = vr.SetComment(d)
	if err != nil {
		return err
	}

	if vr.Composite == "" {
		d.Set("composite", nil)
	} else {
		d.Set("composite", vr.Composite)
	}

	d.Set("subquery", vr.Subquery)
	return nil
}

func update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := updateData(d, locked.Conn)
	if err != nil {
		return diag.FromErr(err)
	}
	return diag.FromErr(locked.Conn.Commit())
}

func updateData(d internal.Data, c *exasol.Conn) error {

	schema, err := argument.Schema(d)
	if err != nil {
		return err
	}

	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.Rename(c, "VIEW", old.(string), new.(string), schema)
		if err != nil {
			return err
		}

		d.Set("name", new)
	}

	replacingNecessary := d.HasChange("composite") || d.HasChange("subquery")
	if replacingNecessary {
		err = createData(d, c, true)
		if err != nil {
			return err
		}
	} else if d.HasChange("comment") {
		err := db.Comment(c, "VIEW", d.Get("name").(string), d.Get("comment").(string), schema)
		if err != nil {
			return err
		}
	}

	return nil
}
