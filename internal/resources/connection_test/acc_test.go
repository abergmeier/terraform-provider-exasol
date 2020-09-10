package connection_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/resources/connection"
	"github.com/abergmeier/terraform-provider-exasol/internal/test"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var (
	nameSuffix = acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)
)

func TestAccExasolRole_rename(t *testing.T) {

	dbName := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	renamedDbName := fmt.Sprintf("%s_RENAMED", dbName)

	locked := exaClient.Lock()
	defer locked.Unlock()

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s
				resource "exasol_connection" "test" {
					name = "%s"
					to = "foo"
				}
				`, test.HCLProviderFromConf(&locked.Conn.Conf), dbName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("exasol_connection.test", "name", dbName),
					testExists("exasol_connection.test"),
				),
			},
			{
				ExpectNonEmptyPlan: true,
				Config: fmt.Sprintf(`%s
				resource "exasol_connection" "test" {
					name = "%s"
					to = "foo"
				}
				`, test.HCLProviderFromConf(&locked.Conn.Conf), renamedDbName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("exasol_connection.test", "name", renamedDbName),
					testExists("exasol_connection.test"),
					testExistsNotByName(dbName),
				),
			},
		},
	})
}

/* Can be reenable once https://github.com/hashicorp/terraform-plugin-sdk/issues/566 is fixed
func TestAccExasolConnection_import(t *testing.T) {

	dbName := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	locked := exaClient.Lock()
	defer locked.Unlock()

	createConnection := func() {
		stmt := fmt.Sprintf(`CREATE OR REPLACE CONNECTION %s
TO 'ftp://192.168.1.1/'
USER 'agent_007'
IDENTIFIED BY 'secret'`, dbName)
		test.Execute(t, locked.Conn, stmt)
		test.Commit(t, locked.Conn)
	}

	tryDeleteConnection := func() {
		stmt := fmt.Sprintf(`DROP CONNECTION %s`, dbName)
		locked.Conn.Execute(stmt)
		locked.Conn.Commit()
	}

	resource.ParallelTest(t, resource.TestCase{
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				PreConfig: tryDeleteConnection,
				Config: fmt.Sprintf(`%s
resource "exasol_connection" "test" {
	name = "%s"
	to = "foo"
}
				`, test.HCLProviderFromConf(&exaConf), dbName),
			},
			{
				PreConfig:         createConnection,
				ResourceName:      "exasol_connection.test",
				ImportState:       true,
				ImportStateId:     strings.ToUpper(dbName),
				ImportStateVerify: true,
				ImportStateCheck:  checkImport(dbName),
			},
			{
				Destroy: true,
			},
		},
	})
}
*/

func checkImport(tableName string) resource.ImportStateCheckFunc {
	return func(s []*terraform.InstanceState) error {
		if len(s) == 0 {
			return errors.New("No Instance found")
		}

		if len(s) != 1 {
			return fmt.Errorf("Expected one Instance: %d", len(s))
		}

		name := s[0].Attributes["name"]
		if name != tableName {
			return fmt.Errorf("Expected name %s: %s", tableName, name)
		}

		to := s[0].Attributes["to"]
		if to != "ftp://192.168.1.1/" {
			return fmt.Errorf("Expected to ftp://192.168.1.1/: %s", to)
		}

		username := s[0].Attributes["username"]
		if username != "agent_007" {
			return fmt.Errorf("Expected username agent_007: %s", username)
		}

		return nil
	}
}

func testExists(id string) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		actualName, err := internal.RootName(state, id)

		if err != nil {
			return err
		}

		return test.True(func(c internal.Conn) (bool, error) {
			return connection.Exists(c, actualName)
		})(state)
	}
}

func testExistsNotByName(actualName string) resource.TestCheckFunc {

	return test.False(func(c internal.Conn) (bool, error) {
		return connection.Exists(c, actualName)
	})
}
