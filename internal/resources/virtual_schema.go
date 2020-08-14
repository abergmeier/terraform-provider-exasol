package resources

import (
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/hashicorp/terraform/helper/schema"
)

func DataSourceExaVirtualSchema() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Schema",
			},
			"adapter_script": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "UDF script used as Adapter",
			},
			"properties": {
				Type:        schema.TypeMap,
				Computed:    true,
				Description: "User with connection",
			},
		},
		Read: readVirtualSchema,
	}
}

func ResourceExaVirtualSchema() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Schema",
			},
			"adapter_script": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "UDF script to use as Adapter",
			},
			"properties": {
				Type:        schema.TypeMap,
				Optional:    true,
				Description: "User to use with connection",
			},
		},
		Create: createVirtualSchema,
		Read:   readVirtualSchema,
		//Update: updateSchema,
		Delete: deleteVirtualSchema,
		//Exists: existsSchema,
		//Importer: &schema.ResourceImporter{
		//	State: importSchema,
		//},
	}
}

func createVirtualSchema(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return createVirtualSchemaData(d, c)
}

func createVirtualSchemaData(d internal.Data, c *exaprovider.Client) error {
	name, err := resourceName(d)
	if err != nil {
		return err
	}
	adapter, err := resourceAdapterScript(d)
	if err != nil {
		return err
	}
	properties := resourceProperties(d)

	if len(properties) == 0 {
		stmt := fmt.Sprintf("CREATE VIRTUAL SCHEMA %s USING %s", name, adapter)
		_, err = c.Conn.Execute(stmt)
	} else {
		with := "\n"
		for k, v := range properties {
			with = fmt.Sprintf("%s %s = '%s'\n", with, k, v)
		}
		stmt := fmt.Sprintf("CREATE VIRTUAL SCHEMA %s USING %s WITH %s", name, adapter, with)
		_, err = c.Conn.Execute(stmt)
	}

	if err != nil {
		return err
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func deleteVirtualSchema(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return deleteVirtualSchemaData(d, c)
}

func deleteVirtualSchemaData(d internal.Data, c *exaprovider.Client) error {

	stmt := fmt.Sprintf("DROP VIRTUAL SCHEMA %s", d.Id())
	_, err := c.Conn.Execute(stmt)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}

func readVirtualSchema(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return readVirtualSchemaData(d, c)
}

func readVirtualSchemaData(d internal.Data, c *exaprovider.Client) error {

	res, err := c.Conn.FetchSlice("SELECT SCHEMA_OBJECT_ID, ADAPTER_SCRIPT FROM EXA_VIRTUAL_SCHEMAS WHERE UPPER(SCHEMA_NAME) = UPPER(?)", []interface{}{
		d.Id(),
	}, "SYS")
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("Schema %s not found", d.Id())
	}

	schemaObjectID := res[0][0].(string)
	adapter := res[0][1].(string)

	res, err = c.Conn.FetchSlice("SELECT PROPERTY_NAME, PROPERTY_VALUE FROM EXA_DBA_VIRTUAL_SCHEMA_PROPERTIES WHERE UPPER(SCHEMA_NAME) = UPPER(?)", []interface{}{
		d.Id(),
	}, "SYS_VIEWS")

	if err != nil {
		return err
	}

	props := make(map[string]string, len(res))

	for _, v := range res {
		name := v[0].(string)
		value := v[1].(string)
		props[name] = value
	}

	d.Set("adapter_script", adapter)
	d.Set("properties", props)
	d.SetId(schemaObjectID)
	return nil
}

func resourceAdapterScript(d internal.Data) (string, error) {
	a := d.Get("adapter_script")
	aString, _ := a.(string)
	if aString == "" {
		return "", errors.New("Missing adapter_script")
	}
	return aString, nil
}

func resourceProperties(d internal.Data) map[string]string {
	p := d.Get("properties")
	pMap, _ := p.(map[string]string)
	return pMap
}
