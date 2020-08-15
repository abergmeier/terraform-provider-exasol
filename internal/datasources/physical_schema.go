package datasources

import (
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/hashicorp/terraform/helper/schema"
)

func PhysicalSchema() *schema.Resource {
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

func readPhysicalSchema(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return readPhysicalSchemaData(d, c)
}

func readPhysicalSchemaData(d internal.Data, c *exaprovider.Client) error {
	name := d.Get("name").(string)

	res, err := c.FetchSlice("SELECT SCHEMA_NAME FROM EXA_ALL_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?) AND SCHEMA_IS_VIRTUAL = FALSE ", []interface{}{
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
