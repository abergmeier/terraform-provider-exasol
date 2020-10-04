package connection

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
)

const (
	schemaName = "datasources_connection_TestMain"
)

var (
	exaClient  *exaprovider.Client
	nameSuffix = acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)
)

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
