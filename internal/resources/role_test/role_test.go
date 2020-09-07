package role_test

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
	"github.com/abergmeier/terraform-exasol/internal/resources/role"
	"github.com/abergmeier/terraform-exasol/internal/test"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var (
	roleSuffix = acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)
)

func TestAccExasolRole_rename(t *testing.T) {

	dbName := fmt.Sprintf("%s_%s", t.Name(), roleSuffix)

	renamedDbName := fmt.Sprintf("%s_RENAMED", dbName)

	resource.Test(t, resource.TestCase{
		Providers: test.DefaultAccProviders,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`%s
				resource "exasol_role" "test_role" {
					name = "%s"
				}
				`, test.HCLProviderFromConf(&exaConf), dbName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("exasol_role.test_role", "name", dbName),
					testExists("exasol_role.test_role"),
				),
			},
			{
				ExpectNonEmptyPlan: true,
				Config: fmt.Sprintf(`%s
				resource "exasol_role" "test_role" {
					name = "%s"
				}
				`, test.HCLProviderFromConf(&exaConf), renamedDbName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("exasol_role.test_role", "name", renamedDbName),
					testExists("exasol_role.test_role"),
					testExistsNotByName(dbName),
				),
			},
		},
	})
}

func testExists(id string) resource.TestCheckFunc {

	return func(state *terraform.State) error {

		actualName, err := internal.RootName(state, id)

		if err != nil {
			return err
		}

		return test.True(func(c internal.Conn) (bool, error) {
			return role.Exists(c, actualName)
		})(state)
	}
}

func testExistsNotByName(actualName string) resource.TestCheckFunc {

	return test.False(func(c internal.Conn) (bool, error) {
		return role.Exists(c, actualName)
	})
}
