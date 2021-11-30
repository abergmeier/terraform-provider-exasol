package view_test

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/internal/resourceprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

const (
	schemaName = "resources_view_test_TestMain"
)

var (
	exaClient        *exaprovider.Client
	nameSuffix       = acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)
	testAccProviders map[string]*schema.Provider
	testAccProvider  *schema.Provider
)

func init() {
	os.Setenv("EXAUID", "sys")
	os.Setenv("EXAPWD", "exasol")
	testAccProvider = resourceprovider.Provider()
	testAccProviders = map[string]*schema.Provider{
		"exasol": testAccProvider,
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(testRun(m))
}

func testRun(m *testing.M) int {
	exaClient = internal.MustCreateTestClient()

	func() {
		locked := exaClient.Lock()
		defer locked.Unlock()
		locked.Conn.Execute(fmt.Sprintf("CREATE SCHEMA %s", schemaName))
		db.MustCommit(locked.Conn)
	}()

	defer func() {
		locked := exaClient.Lock()
		defer locked.Unlock()
		locked.Conn.Execute(fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
		locked.Conn.Commit()
	}()

	return m.Run()
}
