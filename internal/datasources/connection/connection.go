package connection

import (
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/exaprovider"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform/helper/schema"
)

func Resource() *schema.Resource {
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
		Read: read,
	}
}

func read(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	return readData(d, locked.Conn)
}

func readData(d internal.Data, c *exasol.Conn) error {
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
