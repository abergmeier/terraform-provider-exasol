package user

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
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// Resource for Exasol User
func Resource() *schema.Resource {
	return &schema.Resource{
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Name of User",
			},
			"password": {
				Type:         schema.TypeString,
				Optional:     true,
				Sensitive:    true,
				Description:  "",
				ExactlyOneOf: []string{"ldap", "kerberos", "password"},
			},
			"kerberos": {
				Type:         schema.TypeString,
				Optional:     true,
				Description:  "Authentication using Kerberos Principals. The defined principal looks like <user>@<realm>",
				ExactlyOneOf: []string{"ldap", "kerberos", "password"},
			},
			"ldap": {
				Type:         schema.TypeString,
				Optional:     true,
				Description:  "Authentication using LDAP",
				ExactlyOneOf: []string{"ldap", "kerberos", "password"},
			},
		},
		CreateContext: create,
		UpdateContext: update,
		DeleteContext: delete,
		Importer: &schema.ResourceImporter{
			StateContext: imp,
		},
		ReadContext: read,
	}
}

func create(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	err := globallock.RunAndRetryRollbacks(func() error {
		locked := c.Lock(ctx)
		defer locked.Unlock()
		err := createData(d, locked.Tx)
		if err != nil {
			return err
		}
		err = locked.Tx.Commit()
		return err
	})
	if err != nil {
		return diag.FromErr(err)
	}
	return nil
}

func createData(d internal.Data, tx *sql.Tx) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	var stmt string

	password := d.Get("password")
	kerberos := d.Get("kerberos")
	ldap := d.Get("ldap")

	if password != "" {
		stmt = fmt.Sprintf(`CREATE USER %s IDENTIFIED BY "%s"`, name, password)
	} else if kerberos != "" {
		stmt = fmt.Sprintf(`CREATE USER %s IDENTIFIED BY KERBEROS PRINCIPAL '%s'`, name, password)
	} else if ldap != "" {
		stmt = fmt.Sprintf(`CREATE USER %s IDENTIFIED AT LDAP AS '%s'`, name, password)
	} else {
		return errors.New("no identification found")
	}

	_, err = tx.Exec(stmt)
	if err != nil {
		return err
	}
	d.SetId(strings.ToUpper(name))
	return err
}

func delete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	err := globallock.RunAndRetryRollbacks(func() error {
		c := meta.(*exaprovider.Client)
		locked := c.Lock(ctx)
		defer locked.Unlock()
		err := deleteData(d, locked.Tx)
		if err != nil {
			return err
		}
		err = locked.Tx.Commit()
		return err
	})
	if err != nil {
		return diag.FromErr(err)
	}
	return nil
}

func deleteData(d internal.Data, tx *sql.Tx) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("DROP USER %s", name)
	_, err = tx.Exec(stmt)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}

func imp(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := importData(ctx, d, locked.Tx)
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importData(ctx context.Context, d internal.Data, tx *sql.Tx) error {
	name := d.Id()
	if name == "" {
		return errors.New("import expects id to be set")
	}
	err := d.Set("name", name)
	if err != nil {
		return err
	}

	err = readData(ctx, d, tx)
	if errors.Is(err, db.ErrorNamedObjectNotFound) {
		return fmt.Errorf("could not find User %s", name)
	}
	return err
}

func read(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	locked := c.Lock(ctx)
	defer locked.Unlock()
	err := readData(ctx, d, locked.Tx)
	if err != nil {
		return diag.FromErr(err)
	}
	return nil
}

func readData(ctx context.Context, d internal.Data, tx *sql.Tx) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	res, err := tx.QueryContext(ctx, "SELECT DISTINGUISHED_NAME, KERBEROS_PRINCIPAL FROM SYS.EXA_DBA_USERS WHERE UPPER(USER_NAME) = UPPER(?)", name)
	if err != nil {
		return err
	}

	if !res.Next() {
		return db.ErrorNamedObjectNotFound
	}

	var ldapIf interface{}
	var kerberosIf interface{}

	err = res.Scan(&ldapIf, &kerberosIf)
	if err != nil {
		return err
	}
	if ldapIf != nil {
		err = d.Set("ldap", ldapIf.(string))
		if err != nil {
			return err
		}
		err = d.Set("kerberos", nil)
		if err != nil {
			return err
		}
		err = d.Set("password", nil)
		if err != nil {
			return err
		}
	} else if kerberosIf != nil {
		err = d.Set("ldap", nil)
		if err != nil {
			return err
		}
		err = d.Set("kerberos", kerberosIf.(string))
		if err != nil {
			return err
		}
		err = d.Set("password", nil)
		if err != nil {
			return err
		}
	} else {
		//TODO: implement
	}

	return nil
}

func update(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	c := meta.(*exaprovider.Client)
	err := globallock.RunAndRetryRollbacks(func() error {
		locked := c.Lock(ctx)
		defer locked.Unlock()
		err := updateData(ctx, d, locked.Tx)
		if err != nil {
			return err
		}
		err = locked.Tx.Commit()
		return err
	})
	if err != nil {
		return diag.FromErr(err)
	}
	return nil
}

func updateData(ctx context.Context, d internal.Data, tx *sql.Tx) error {

	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.RenameGlobal(tx, "USER", old.(string), new.(string))
		if err != nil {
			return err
		}
	}

	return readData(ctx, d, tx)
}
