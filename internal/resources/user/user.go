package user

import (
	"errors"
	"fmt"
	"strings"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/argument"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/grantstreetgroup/go-exasol-client"
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
		Create: create,
		Update: update,
		Delete: delete,
		Exists: exists,
		Importer: &schema.ResourceImporter{
			State: imp,
		},
		Read: read,
	}
}

func create(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := createData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func createData(d internal.Data, c *exasol.Conn) error {

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

func delete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := deleteData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func deleteData(d internal.Data, c *exasol.Conn) error {

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

func exists(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	return existsData(d, locked.Conn)
}

func existsData(d internal.Data, c internal.Conn) (bool, error) {
	name, err := argument.Name(d)
	if err != nil {
		return false, err
	}

	return Exists(c, name)
}

func Exists(c internal.Conn, name string) (bool, error) {
	res, err := c.FetchSlice("SELECT CREATED FROM EXA_ALL_USERS WHERE UPPER(USER_NAME) = UPPER(?)", []interface{}{
		name,
	}, "SYS")
	if err != nil {
		return false, err
	}

	return len(res) != 0, nil
}

func imp(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := importData(d, locked.Conn)
	if err != nil {
		return nil, err
	}
	err = locked.Conn.Commit()
	if err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}

func importData(d internal.Data, c internal.Conn) error {
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

func read(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := readData(d, locked.Conn)
	if err != nil {
		return err
	}
	return nil
}

func readData(d internal.Data, c internal.Conn) error {
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

func update(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*exaprovider.Client)
	locked := c.Lock()
	defer locked.Unlock()
	err := updateData(d, locked.Conn)
	if err != nil {
		return err
	}
	return locked.Conn.Commit()
}

func updateData(d internal.Data, c *exasol.Conn) error {

	if d.HasChange("name") {
		old, new := d.GetChange("name")

		err := db.RenameGlobal(c, "USER", old.(string), new.(string))
		if err != nil {
			return err
		}
	}

	return readData(d, c)
}