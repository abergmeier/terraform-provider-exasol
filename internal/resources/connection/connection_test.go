package connection

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-provider-exasol/internal"
	"github.com/abergmeier/terraform-provider-exasol/internal/globallock"
)

func TestCreateConnection(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	err := globallock.RunAndRetryRollbacks(func() error {
		locked := exaClient.Lock()
		defer locked.Unlock()

		create := &internal.TestData{
			Values: map[string]interface{}{
				"name": name,
				"to":   "me",
			},
		}
		err := deleteConnectionData(create, locked.Conn)
		if globallock.IsRollbackError(err) {
			return err
		}

		err = createConnectionData(create, locked.Conn)
		if err != nil {
			if globallock.IsRollbackError(err) {
				return err
			}
			t.Fatal("Unexpected error:", err)
		}

		if create.Id() != strings.ToUpper(name) {
			t.Fatal("Unexpected id:", create.Id())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteConnection(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	err := globallock.RunAndRetryRollbacks(func() error {
		locked := exaClient.Lock()
		defer locked.Unlock()

		d := &internal.TestData{
			Values: map[string]interface{}{
				"name": name,
			},
		}
		err := deleteConnectionData(d, locked.Conn)
		if err == nil {
			t.Fatal("Expected error")
		} else if globallock.IsRollbackError(err) {
			return err
		}

		create := &internal.TestData{
			Values: map[string]interface{}{
				"name": name,
				"to":   "me",
			},
		}
		err = createConnectionData(create, locked.Conn)
		if err != nil {
			if globallock.IsRollbackError(err) {
				return err
			}
			t.Fatal("Unexpected error:", err)
		}

		err = deleteConnectionData(d, locked.Conn)
		if err != nil {
			if globallock.IsRollbackError(err) {
				return err
			}
			t.Fatal("Unexpected error:", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadConnection(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	err := globallock.RunAndRetryRollbacks(func() error {
		locked := exaClient.Lock()
		defer locked.Unlock()

		read := &internal.TestData{
			Values: map[string]interface{}{
				"name": name,
			},
		}

		err := deleteConnectionData(read, locked.Conn)
		if globallock.IsRollbackError(err) {
			return err
		}
		err = readConnectionData(read, locked.Conn)
		if err == nil {
			t.Fatal("Expected error by readConnectionData")
		} else if globallock.IsRollbackError(err) {
			return err
		}

		create := &internal.TestData{
			Values: map[string]interface{}{
				"name": name,
				"to":   "bar",
			},
		}

		err = createConnectionData(create, locked.Conn)
		if err != nil {
			if globallock.IsRollbackError(err) {
				return err
			}
			t.Fatal("Unexpected error:", err)
		}

		err = readConnectionData(read, locked.Conn)
		if err != nil {
			if globallock.IsRollbackError(err) {
				return err
			}
			t.Fatal("Unexpected error:", err)
		}

		readTo := read.Get("to")
		readToString, _ := readTo.(string)
		if readToString != "bar" {
			t.Fatalf("Unexpected to value: %#v", readTo)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestImportConnection(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	err := globallock.RunAndRetryRollbacks(func() error {
		locked := exaClient.Lock()
		defer locked.Unlock()

		deleteData := &internal.TestData{
			Values: map[string]interface{}{
				"name": name,
			},
		}
		err := deleteConnectionData(deleteData, locked.Conn)
		if globallock.IsRollbackError(err) {
			return err
		}

		imp := &internal.TestData{}
		imp.SetId(name)
		err = importConnectionData(imp, locked.Conn)
		if err == nil {
			t.Fatal("Expected error from importConnectionData")
		} else if globallock.IsRollbackError(err) {
			return err
		}

		stmt := fmt.Sprintf("CREATE OR REPLACE CONNECTION %s TO 'http://foo' USER 'foo' IDENTIFIED BY 'bar'", name)
		_, err = locked.Conn.Execute(stmt)
		if err != nil {
			if globallock.IsRollbackError(err) {
				return err
			}
			t.Fatal(err)
		}

		err = importConnectionData(imp, locked.Conn)
		if err != nil {
			if globallock.IsRollbackError(err) {
				return err
			}
			t.Fatal("Unexpected error:", err)
		}

		to := imp.Get("to")
		toString, _ := to.(string)
		if toString != "http://foo" {
			t.Errorf("Expected to http://foo: %#v", to)
		}

		username := imp.Get("username")
		usernameString, _ := username.(string)
		if usernameString != "foo" {
			t.Errorf("Expected username foo: %#v", username)
		}

		password := imp.Get("password")
		if password != nil {
			t.Errorf("Did not expect password: %#v", password)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestUpdateConnection(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("%s_%s", t.Name(), nameSuffix)

	err := globallock.RunAndRetryRollbacks(func() error {
		locked := exaClient.Lock()
		defer locked.Unlock()

		create := &internal.TestData{
			Values: map[string]interface{}{
				"name": name,
				"to":   "foo",
			},
		}

		err := deleteConnectionData(create, locked.Conn)
		if globallock.IsRollbackError(err) {
			return err
		}

		err = updateConnectionData(create, locked.Conn)
		if err == nil {
			t.Fatal("Expected error from updateConnectionData")
		} else if globallock.IsRollbackError(err) {
			return err
		}

		err = createConnectionData(create, locked.Conn)
		if err != nil {
			if globallock.IsRollbackError(err) {
				return err
			}
			t.Fatal("Unexpected error:", err)
		}

		update := &internal.TestData{
			Values: map[string]interface{}{
				"name":     name,
				"to":       "bar",
				"username": "myuser",
			},
		}

		err = updateConnectionData(update, locked.Conn)
		if err != nil {
			if globallock.IsRollbackError(err) {
				return err
			}
			t.Fatal("Unexpexted error:", err)
		}

		read := &internal.TestData{
			Values: map[string]interface{}{
				"name": name,
			},
		}

		err = readConnectionData(read, locked.Conn)
		if err != nil {
			if globallock.IsRollbackError(err) {
				return err
			}
			t.Fatal("Unexpected error:", err)
		}
		to := read.Get("to")
		toString, _ := to.(string)
		if toString != "bar" {
			t.Fatalf("Unexpected to value %#v", to)
		}
		username := read.Get("username")
		usernameString, _ := username.(string)
		if usernameString != "myuser" {
			t.Fatalf("Unexpected to value %#v", username)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
