package connection

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal/binding"
	"github.com/abergmeier/terraform-provider-exasol/internal/cached"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/globallock"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/computed"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// Resource for Exasol Connection
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
				Required:    true,
				Description: "Where connection points to",
			},
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
				Description: "User to use for connection",
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
				Description: "Password to use for connection",
				Sensitive:   true,
			},
		},
		CreateContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
			return globallock.RunAndRetryRollbacks(func() error {
				return cached.Create(create, d, meta)
			})
		},
		ReadContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
			return cached.ReadContext(read, ctx, d, meta)
		},
		UpdateContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
			return globallock.RunAndRetryRollbacks(func() error {
				return cached.Update(update, d, meta)
			})
		},
		DeleteContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
			return globallock.RunAndRetryRollbacks(func() error {
				return cached.Delete(delete, d, meta)
			})
		},
		Exists: func(d *schema.ResourceData, meta interface{}) (bool, error) {
			return cached.Exists(exists, d, meta)
		},
		Importer: &schema.ResourceImporter{
			StateContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
				return cached.ImporterStateContext(importConnection, ctx, d, meta)
			},
		},
	}

}

func read(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) diag.Diagnostics {
	err := readConnectionData(d, conn.Conn)
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func readConnectionData(d binding.Data, c binding.Conn) error {

	err := computed.ReadConnection(d, c)
	if errors.Is(err, argument.ErrorEmptyName) {
		return fmt.Errorf("Empty name not allowed for Connection (id: %s)", d.Id())
	}
	return err
}

func create(d *schema.ResourceData, conn *exaprovider.Connection) error {
	err := createConnectionData(d, conn.Conn)
	if err != nil {
		return err
	}

	return conn.Conn.Commit()
}

func createConnectionData(d binding.Data, c *exasol.Conn) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	to := d.Get("to").(string)

	user := resourceUser(d)
	identifiedBy := resourceIdentifiedBy(d)

	if user == "" {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s'", name, to)
		_, err = c.Execute(stmt)
	} else if identifiedBy == "" {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s' USER '%s'", name, to, user)
		_, err = c.Execute(stmt)
	} else {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s' USER '%s' IDENTIFIED BY '%s'", name, to, user, identifiedBy)
		_, err = c.Execute(stmt)
	}

	if err != nil {
		return err
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func delete(d *schema.ResourceData, conn *exaprovider.Connection) error {

	err := deleteConnectionData(d, conn.Conn)
	if err != nil {
		return err
	}
	return conn.Conn.Commit()
}

func deleteConnectionData(d binding.Data, c *exasol.Conn) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("DROP CONNECTION %s", name)
	_, err = c.Execute(stmt)
	if err != nil {
		return err
	}
	d.SetId("")
	return nil
}

func importConnection(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) ([]*schema.ResourceData, error) {
	err := importConnectionData(d, conn.Conn)
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importConnectionData(d binding.Data, c binding.Conn) error {
	name := d.Id()
	if name == "" {
		return errors.New("Import expects id to be set")
	}
	err := d.Set("name", name)
	if err != nil {
		return err
	}

	return readConnectionData(d, c)
}

func update(d *schema.ResourceData, conn *exaprovider.Connection) error {

	err := updateConnectionData(d, conn.Conn)
	if err != nil {
		return err
	}
	return conn.Conn.Commit()
}

func updateConnectionData(d binding.Data, c *exasol.Conn) error {
	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.RenameGlobal(c, "CONNECTION", old.(string), new.(string))
		if err != nil {
			return err
		}
	}

	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	to, err := resourceTo(d)
	if err != nil {
		return err
	}
	user := resourceUser(d)
	identifiedBy := resourceIdentifiedBy(d)

	if user == "" {
		stmt := fmt.Sprintf("ALTER CONNECTION %s TO '%s'", name, to)
		_, err := c.Execute(stmt)
		return err
	}

	if identifiedBy == "" {
		stmt := fmt.Sprintf("ALTER CONNECTION %s TO '%s' USER '%s'", name, to, user)
		_, err := c.Execute(stmt)
		return err
	}

	stmt := fmt.Sprintf("ALTER CONNECTION %s TO '%s' USER '%s' IDENTIFIED BY '%s'", name, to, user, identifiedBy)
	_, err = c.Execute(stmt)
	return err
}

func Exists(c binding.Conn, name string) (bool, error) {
	rows, err := c.FetchSlice("SELECT CONNECTION_NAME FROM EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return false, err
	}

	return len(rows) > 0, nil
}

func exists(d *schema.ResourceData, conn *exaprovider.Connection) (bool, error) {
	return existsData(d, conn.Conn)
}

func existsData(d binding.Data, c binding.Conn) (bool, error) {
	name, err := argument.Name(d)
	if err != nil {
		return false, err
	}

	return Exists(c, name)
}

func resourceTo(d binding.Data) (string, error) {
	to := d.Get("to").(string)
	if to == "" {
		return "", fmt.Errorf("Empty attribute `to` for `%s`", d)
	}
	return to, nil
}

func resourceUser(d binding.Data) string {
	user := d.Get("username")
	if user == nil {
		return ""
	}
	return user.(string)
}

func resourceIdentifiedBy(d binding.Data) string {
	pwd := d.Get("password")
	if pwd == nil {
		return ""
	}
	return pwd.(string)
}
