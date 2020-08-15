package datasources

import (
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/hashicorp/terraform/helper/schema"
)

func AdapterScriptResource() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of Adapter script",
			},
			"schema": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Schema this script belongs to",
			},
			"java": {
				Type:     schema.TypeList,
				Computed: true,
				Description: `Script implemented in Java.
One of 'java' or 'python' must be provided.`,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"input_type": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"result_type": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"text": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},
			"python": {
				Type:     schema.TypeList,
				Computed: true,
				Description: `Script implemented in Python.
One of 'java' or 'python' must be provided.`,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"input_type": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"result_type": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"text": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},
		},
		Read: readAdapterScript,
	}
}

func readAdapterScript(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return readAdapterScriptData(d, c)
}

func readAdapterScriptData(d internal.Data, c *exaprovider.Client) error {
	name := d.Get("name").(string)
	schema := d.Get("schema").(string)

	stmt := "SELECT SCRIPT_LANGUAGE, SCRIPT_TEXT, SCRIPT_INPUT_TYPE, SCRIPT_RESULT_TYPE FROM EXA_ALL_SCRIPTS WHERE SCRIPT_TYPE = ADAPTER AND UPPER(SCRIPT_SCHEMA) = UPPER(?) AND UPPER(SCRIPT_NAME) = UPPER(?)"
	slice, err := c.Conn.FetchSlice(stmt, []interface{}{
		schema,
		name,
	}, "SYS")

	if err != nil {
		return err
	}

	if len(slice) != 1 {
		return fmt.Errorf("Could not find one adapter Script %s in Schema %s", name, schema)
	}

	first := slice[0]

	var sb map[string]interface{}
	switch first[0] {
	case "java":
		sb, err = readJavaAdapterScript(d, first[1:]...)
		if err != nil {
			return err
		}
		err = d.Set("java", []interface{}{sb})
		if err != nil {
			return err
		}
	case "python":
		sb, err = readPythonAdapterScript(d, first[1:]...)
		if err != nil {
			return err
		}
		err = d.Set("python", []interface{}{sb})
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unsupported scripting language: %s", first[0])
	}

	if err != nil {
		return err
	}

	id := fmt.Sprintf("%s/%s", strings.ToUpper(schema), strings.ToUpper(name))
	d.SetId(id)
	return nil
}

func readJavaAdapterScript(d internal.Data, data ...interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{
		"text":        data[0],
		"input_type":  data[1],
		"result_type": data[2],
	}, nil
}

func readPythonAdapterScript(d internal.Data, data ...interface{}) (map[string]interface{}, error) {
	return map[string]interface{}{
		"text":        data[0],
		"input_type":  data[1],
		"result_type": data[2],
	}, nil
}
