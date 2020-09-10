package user_test

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/resources/user"
	"github.com/abergmeier/terraform-provider-exasol/internal/test"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var (
	roleSuffix = acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)
)

func TestAccExasolUser_rename(t *testing.T) {

	dbName := fmt.Sprintf("%s_%s", t.Name(), roleSuffix)

	renamedDbName := fmt.Sprintf("%s_RENAMED", dbName)

	resource.Test(t, resource.TestCase{
		PreCheck:  nil,
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s
				resource "exasol_user" "test" {
					name     = "%s"
					password = "foo"
				}
				`, test.HCLProviderFromConf(&exaConf), dbName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("exasol_user.test", "name", dbName),
					testExist("exasol_user.test"),
				),
			},
			{
				ExpectNonEmptyPlan: true,
				Config: fmt.Sprintf(`%s
				resource "exasol_user" "test" {
					name     = "%s"
					password = "bar"
				}
				`, test.HCLProviderFromConf(&exaConf), renamedDbName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("exasol_user.test", "name", renamedDbName),
					testExist("exasol_user.test"),
					testExistsNotByName(dbName),
				),
			},
		},
	})
}

func testExistsNotByName(actualName string) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		c := test.AccProvider.Meta().(*exaprovider.Client)
		locked := c.Lock()
		defer locked.Unlock()

		exists, err := user.Exists(locked.Conn, actualName)
		if err != nil {
			return err
		}

		if exists {
			return fmt.Errorf("User %s does exist", actualName)
		}

		return nil
	}
}

func testExist(id string) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		rs, err := rootRole(state, id)
		if err != nil {
			return err
		}

		actualName, ok := rs.Primary.Attributes["name"]
		if !ok {
			return fmt.Errorf("Attribute name not found")
		}

		c := test.AccProvider.Meta().(*exaprovider.Client)
		locked := c.Lock()
		defer locked.Unlock()

		exists, err := user.Exists(locked.Conn, actualName)
		if err != nil {
			return err
		}

		if !exists {
			return fmt.Errorf("User %s does not exist", actualName)
		}

		return nil
	}
}

func testName(id, expectedName string) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		rs, err := rootRole(state, id)
		if err != nil {
			return err
		}

		actualName := rs.Primary.Attributes["name"]
		if actualName != expectedName {
			return fmt.Errorf("Expected name %s: %s", expectedName, actualName)
		}

		return nil
	}
}

func rootRole(state *terraform.State, id string) (*terraform.ResourceState, error) {

	rs, ok := state.RootModule().Resources[id]
	if !ok {
		return nil, fmt.Errorf("User not found: %s", id)
	}

	return rs, nil
}
