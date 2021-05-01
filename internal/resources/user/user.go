package user

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
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/grantstreetgroup/go-exasol-client"
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
				return cached.ImporterStateContext(imp, ctx, d, meta)
			},
		},
	}
}

func create(d *schema.ResourceData, conn *exaprovider.Connection) error {
	err := createData(d, conn.Conn)
	if err != nil {
		return err
	}
	return conn.Conn.Commit()
}

func createData(d binding.Data, c *exasol.Conn) error {

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
		return errors.New("No identification found")
	}

	_, err = c.Execute(stmt)
	if err != nil {
		return err
	}
	d.SetId(strings.ToUpper(name))
	return err
}

func delete(d *schema.ResourceData, conn *exaprovider.Connection) error {

	err := deleteData(d, conn.Conn)
	if err != nil {
		return err
	}
	return conn.Conn.Commit()
}

func deleteData(d binding.Data, c *exasol.Conn) error {

	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf("DROP USER %s", name)
	_, err = c.Execute(stmt)
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
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

func Exists(c binding.Conn, name string) (bool, error) {
	res, err := c.FetchSlice("SELECT CREATED FROM EXA_ALL_USERS WHERE UPPER(USER_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return false, err
	}

	return len(res) != 0, nil
}

func imp(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) ([]*schema.ResourceData, error) {
	err := importData(d, conn.Conn)
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importData(d binding.Data, c binding.Conn) error {
	name := d.Id()
	if name == "" {
		return errors.New("Import expects id to be set")
	}
	err := d.Set("name", name)
	if err != nil {
		return err
	}

	err = readData(d, c)
	if errors.Is(err, db.ErrorNamedObjectNotFound) {
		return fmt.Errorf("Could not find User %s", name)
	}
	return err
}

func read(ctx context.Context, d *schema.ResourceData, conn *exaprovider.Connection) diag.Diagnostics {
	err := readData(d, conn.Conn)
	if err != nil {
		return diag.FromErr(err)
	}
	return nil
}

func readData(d binding.Data, c binding.Conn) error {
	name, err := argument.Name(d)
	if err != nil {
		return err
	}

	res, err := c.FetchSlice("SELECT DISTINGUISHED_NAME, KERBEROS_PRINCIPAL FROM EXA_DBA_USERS WHERE UPPER(USER_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return db.ErrorNamedObjectNotFound
	}

	ldapIf := res[0][0]
	kerberosIf := res[0][1]
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

func update(d *schema.ResourceData, conn *exaprovider.Connection) error {
	err := updateData(d, conn.Conn)
	if err != nil {
		return err
	}
	return conn.Conn.Commit()
}

func updateData(d binding.Data, c *exasol.Conn) error {

	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.RenameGlobal(c, "USER", old.(string), new.(string))
		if err != nil {
			return err
		}
	}

	return readData(d, c)
}
