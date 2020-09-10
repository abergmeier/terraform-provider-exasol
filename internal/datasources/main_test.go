package datasources

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/exaprovider"
	"github.com/abergmeier/terraform-provider-exasol/pkg/db"
)

const (
	schemaName = "datasources_TestMain"
)

var (
	exaClient *exaprovider.Client
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(testRun(m))
}

func testRun(m *testing.M) int {
	exaClient = internal.MustCreateTestClient()
	defer exaClient.Close()

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
