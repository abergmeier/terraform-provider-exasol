package datasources

import (
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/hashicorp/terraform/helper/schema"
)

func ConnectionResource() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of connection",
			},
			"to": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Where connection points to",
			},
			"username": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "User used with connection",
			},
		},
		Read: readConnection,
	}
}

func readConnection(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	return readConnectionData(d, c)
}

func readConnectionData(d internal.Data, c *exaprovider.Client) error {
	name := d.Get("name").(string)

	res, err := c.FetchSlice("SELECT CONNECTION_STRING, USER_NAME, CREATED FROM EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("Connection %s not found", name)
	}

	d.Set("to", res[0][0].(string))
	username, _ := res[0][1].(string)
	if username != "" {
		d.Set("username", username)
	}
	d.SetId(strings.ToUpper(name))
	return nil
}
