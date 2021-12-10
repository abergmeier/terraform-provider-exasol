package view_test

import (
	"fmt"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/statements"
	"github.com/abergmeier/terraform-provider-exasol/internal/test"
	"github.com/abergmeier/terraform-provider-exasol/pkg/tx"
	"github.com/exasol/exasol-driver-go"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccDeleteInBetween(t *testing.T) {

	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			dv := statements.DropView{
				Schema: schemaName,
				Name:   name,
			}
			locked := exaprovider.TestLock(t, exaClient)
			defer locked.Unlock()
			dv.Execute(locked.Tx)
		},
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: testAccViewResource(exaConf, name),
			},
			{
				PreConfig: func() {
					dv := statements.DropView{
						Schema: schemaName,
						Name:   name,
					}
					locked := exaprovider.TestLock(t, exaClient)
					defer locked.Unlock()
					err := dv.Execute(locked.Tx)
					if err != nil {
						t.Fatal(err)
					}
				},
				Config: testAccViewChangedResource(exaConf, name),
			},
		},
	})
}

func testAccViewResource(conf *exasol.DSNConfig, name string) string {
	return fmt.Sprintf(`%s
resource "exasol_view" "%s" {
	name     = "%s"
	schema   = "%s"
	subquery = "SELECT COLUMN_SCHEMA FROM SYS.EXA_ALL_COLUMNS"
}
`, test.HCLProviderFromConf(conf), name, name, schemaName)
}

func testAccViewChangedResource(conf *exasol.DSNConfig, name string) string {
	return fmt.Sprintf(`%s
resource "exasol_view" "%s" {
	name     = "%s"
	schema   = "%s"
	subquery = "SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS"
}
`, test.HCLProviderFromConf(conf), name, name, schemaName)
}

func TestAccViewResourceComment(t *testing.T) {
	t.Parallel()

	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	checkAfterComment := func(s *terraform.State) error {
		locked := exaprovider.TestLock(t, exaClient)
		defer locked.Unlock()

		res := tx.MustQueryAtLeastOne(locked.Tx, "SELECT VIEW_COMMENT FROM SYS.EXA_DBA_VIEWS WHERE UPPER(VIEW_SCHEMA) = UPPER(?) AND UPPER(VIEW_NAME) = UPPER(?)", schemaName, name)
		var actual string
		err := res.Scan(&actual)
		if err != nil {
			return err
		}

		if actual != "Foo" {
			return fmt.Errorf("Expected comment Foo: %s", actual)
		}
		return nil
	}

	resource.Test(t, resource.TestCase{
		/*PreCheck: func() {
			locked := exaprovider.TestLock(t, exaClient)
			defer locked.Unlock()

			tx.MustExecf(locked.Tx, "CREATE OR REPLACE VIEW %s.%s AS SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS", schemaName, name)
			locked.Tx.Commit()
		},*/
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: testAccViewResourceBeforeComment(exaConf, name),
			},
			{
				Config: testAccViewResourceAfterComment(exaConf, name),
				Check:  checkAfterComment,
			},
		},
	})
}

func testAccViewResourceBeforeComment(conf *exasol.DSNConfig, name string) string {
	return fmt.Sprintf(`%s
resource "exasol_view" "%s" {
	name     = "%s"
	schema   = "%s"
	subquery = "SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS"
}
`, test.HCLProviderFromConf(conf), name, name, schemaName)
}

func testAccViewResourceAfterComment(conf *exasol.DSNConfig, name string) string {
	return fmt.Sprintf(`%s
resource "exasol_view" "%s" {
	name     = "%s"
	schema   = "%s"
	subquery = "SELECT COLUMN_TYPE FROM SYS.EXA_ALL_COLUMNS"
	comment  = "Foo"
}
`, test.HCLProviderFromConf(conf), name, name, schemaName)
}
