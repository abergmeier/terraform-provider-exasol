package resources

import (
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/hashicorp/terraform/helper/schema"
)

func DataSourceExaPhysicalSchema() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Schema",
			},
		},
		Read: readPhysicalSchema,
	}
}

func ResourceExaPhysicalSchema() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Schema",
			},
		},
		Create: createPhysicalSchema,
		Read:   readPhysicalSchema,
		Update: updatePhysicalSchema,
		Delete: deletePhysicalSchema,
		Exists: existsPhysicalSchema,
		Importer: &schema.ResourceImporter{
			State: importPhysicalSchema,
		},
	}
}

func createPhysicalSchema(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return createPhysicalSchemaData(d, c)
}

func createPhysicalSchemaData(d internal.Data, c *exaprovider.Client) error {
	name, err := resourceName(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("CREATE SCHEMA %s", name)
	_, err = c.Conn.Execute(stmt)

	if err != nil {
		return err
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func deletePhysicalSchema(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return deletePhysicalSchemaData(d, c)
}

func deletePhysicalSchemaData(d internal.Data, c *exaprovider.Client) error {
	name, err := resourceName(d)
	if err != nil {
		return err
	}
	stmt := fmt.Sprintf("DROP SCHEMA %s", name)
	_, err = c.Conn.Execute(stmt)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}

func existsPhysicalSchema(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*exaprovider.Client)
	return existsPhysicalSchemaData(d, c)
}

func existsPhysicalSchemaData(d internal.Data, c *exaprovider.Client) (bool, error) {

	result, err := c.Conn.Execute("SELECT SCHEMA_NAME FROM EXA_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?)", [][]interface{}{
		{
			d.Id(),
		},
	}, "SYS")
	if err != nil {
		return false, err
	}

	results := result["results"].([]interface{})[0].(map[string]interface{})["resultSet"].(map[string]interface{})
	ri := results["numRows"]
	if ri == nil {
		return false, errors.New("numRows is nil")
	}

	rows, ok := ri.(float64)
	if !ok {
		return false, errors.New("numRows not float64")
	}

	return rows > 0.0, nil
}

func importPhysicalSchema(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	err := importPhysicalSchemaData(d, c)
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importPhysicalSchemaData(d internal.Data, c *exaprovider.Client) error {

	slice, err := c.Conn.FetchSlice("SELECT SCHEMA_NAME FROM EXA_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?) AND SCHEMA_IS_VIRTUAL = false", []interface{}{
		d.Id(),
	}, "SYS")
	if err != nil {
		return err
	}

	if len(slice) == 0 {
		return fmt.Errorf("Schema %s not found", d.Id())
	}
	d.SetId(strings.ToUpper(d.Id()))
	return nil
}

func readPhysicalSchema(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return readPhysicalSchemaData(d, c)
}

func readPhysicalSchemaData(d internal.Data, c *exaprovider.Client) error {
	name, err := resourceName(d)
	if err != nil {
		return err
	}

	res, err := c.Conn.FetchSlice("SELECT SCHEMA_NAME FROM EXA_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?) AND SCHEMA_IS_VIRTUAL = FALSE ", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("Schema %s not found", name)
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func updatePhysicalSchema(d *schema.ResourceData, meta interface{}) error {
	// Noop currently
	return nil
}
