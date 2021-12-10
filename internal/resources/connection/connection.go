package connection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/globallock"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/computed"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
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
		CreateContext: createConnection,
		ReadContext:   readConnection,
		UpdateContext: updateConnection,
		DeleteContext: deleteConnection,
		Importer: &schema.ResourceImporter{
			StateContext: importConnection,
		},
	}

}

func readConnection(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := readConnectionData(ctx, d, locked.Tx)
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func readConnectionData(ctx context.Context, d internal.Data, tx *sql.Tx) error {

	err := computed.ReadConnection(ctx, d, tx)
	if errors.Is(err, argument.ErrorEmptyName) {
		return fmt.Errorf("empty name not allowed for Connection (id: %s)", d.Id())
	}
	return err
}

func createConnection(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	err := globallock.RunAndRetryRollbacks(func() error {
		locked := c.Lock(ctx)
		defer locked.Unlock()
		err := createConnectionData(d, locked.Tx)
		if err != nil {
			return err
		}

		return locked.Tx.Commit()
	})
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func createConnectionData(d internal.Data, tx *sql.Tx) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}
	to := d.Get("to").(string)

	user := resourceUser(d)
	identifiedBy := resourceIdentifiedBy(d)

	if user == "" {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s'", name, to)
		_, err = tx.Exec(stmt)
	} else if identifiedBy == "" {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s' USER '%s'", name, to, user)
		_, err = tx.Exec(stmt)
	} else {
		stmt := fmt.Sprintf("CREATE CONNECTION %s TO '%s' USER '%s' IDENTIFIED BY '%s'", name, to, user, identifiedBy)
		_, err = tx.Exec(stmt)
	}

	if err != nil {
		return err
	}

	d.SetId(strings.ToUpper(name))
	return nil
}

func deleteConnection(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	err := globallock.RunAndRetryRollbacks(func() error {
		locked := c.Lock(ctx)
		defer locked.Unlock()
		err := deleteConnectionData(d, locked.Tx)
		if err != nil {
			return err
		}
		return locked.Tx.Commit()
	})
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func deleteConnectionData(d internal.Data, tx *sql.Tx) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("DROP CONNECTION %s", name)
	_, err = tx.Exec(stmt)
	if err != nil {
		return err
	}
	d.SetId("")
	return nil
}

func importConnection(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := importConnectionData(ctx, d, locked.Tx)
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importConnectionData(ctx context.Context, d internal.Data, tx *sql.Tx) error {
	name := d.Id()
	if name == "" {
		return errors.New("import expects id to be set")
	}
	err := d.Set("name", name)
	if err != nil {
		return err
	}

	return readConnectionData(ctx, d, tx)
}

func updateConnection(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	err := globallock.RunAndRetryRollbacks(func() error {
		locked := c.Lock(ctx)
		defer locked.Unlock()
		err := updateConnectionData(d, locked.Tx)
		if err != nil {
			return err
		}
		return locked.Tx.Commit()
	})
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func updateConnectionData(d internal.Data, tx *sql.Tx) error {
	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.RenameGlobal(tx, "CONNECTION", old.(string), new.(string))
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
		_, err := tx.Exec(stmt)
		return err
	}

	if identifiedBy == "" {
		stmt := fmt.Sprintf("ALTER CONNECTION %s TO '%s' USER '%s'", name, to, user)
		_, err := tx.Exec(stmt)
		return err
	}

	stmt := fmt.Sprintf("ALTER CONNECTION %s TO '%s' USER '%s' IDENTIFIED BY '%s'", name, to, user, identifiedBy)
	_, err = tx.Exec(stmt)
	return err
}

func Exists(ctx context.Context, tx *sql.Tx, name string) (bool, error) {
	res, err := tx.QueryContext(ctx, "SELECT CONNECTION_NAME FROM SYS.EXA_DBA_CONNECTIONS WHERE UPPER(CONNECTION_NAME) = UPPER(?)", name)
	if err != nil {
		return false, err
	}
	return res.Next(), nil
}

func resourceTo(d internal.Data) (string, error) {
	to := d.Get("to").(string)
	if to == "" {
		return "", fmt.Errorf("empty attribute `to` for `%s`", d)
	}
	return to, nil
}

func resourceUser(d internal.Data) string {
	user := d.Get("username")
	if user == nil {
		return ""
	}
	return user.(string)
}

func resourceIdentifiedBy(d internal.Data) string {
	pwd := d.Get("password")
	if pwd == nil {
		return ""
	}
	return pwd.(string)
}
