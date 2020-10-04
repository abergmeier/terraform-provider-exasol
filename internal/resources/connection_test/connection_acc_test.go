package connection_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/resources/connection"
	"github.com/abergmeier/terraform-provider-exasol/internal/resources/root"
	"github.com/abergmeier/terraform-provider-exasol/internal/test"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var (
	nameSuffix = acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)
)

func TestAccExasolConnection_rename(t *testing.T) {

	dbName := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)
	renamedDbName := fmt.Sprintf("%s_RENAMED", dbName)

	resource.ParallelTest(t, resource.TestCase{
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s
				resource "exasol_connection" "test" {
					name = "%s"
					to = "foo"
				}
				`, test.HCLProviderFromConf(&exaConf), dbName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("exasol_connection.test", "name", dbName),
					testExists("exasol_connection.test"),
					testId("exasol_connection.test", strings.ToUpper(dbName)),
				),
			},
			{
				Config: fmt.Sprintf(`%s
				resource "exasol_connection" "test" {
					name = "%s"
					to = "foo"
				}
				`, test.HCLProviderFromConf(&exaConf), renamedDbName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("exasol_connection.test", "name", renamedDbName),
					testExists("exasol_connection.test"),
					testExistsNotByName(dbName),
					testId("exasol_connection.test", strings.ToUpper(dbName)),
				),
			},
		},
	})
}

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
		_, err := locked.Conn.Execute(stmt)
		if err != nil {
			return
		}
		locked.Conn.Commit()
	}
	defer tryDeleteConnection()

	resource.ParallelTest(t, resource.TestCase{
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				PreConfig: tryDeleteConnection,
				Config: fmt.Sprintf(`%s
resource "exasol_connection" "test" {
  name     = "%s"
  to       = "ftp://192.168.1.1/"
  username = "agent_007"
}
				`, test.HCLProviderFromConf(&exaConf), strings.ToUpper(dbName)),
			},
			{
				PreConfig:         createConnection,
				ResourceName:      "exasol_connection.test",
				ImportState:       true,
				ImportStateId:     strings.ToUpper(dbName),
				ImportStateVerify: true,
			},
		},
	})
}

func testId(resourceName, id string) resource.TestCheckFunc {
	return func(state *terraform.State) error {

		r, err := root.ResourceByName(state, resourceName)
		if err != nil {
			return err
		}

		if r.Primary.ID != id {
			return fmt.Errorf("Expected Id %s: %s", id, r.Primary.ID)
		}

		return nil
	}
}

func testExists(id string) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		actualName, err := root.NameAttribute(state, id)

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
