package resources

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
)

func TestCreateConnection(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()

	name := t.Name()

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
			"to":   "me",
		},
	}
	deleteConnectionData(create, locked.Conn)

	err := createConnectionData(create, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	if create.Id() != strings.ToUpper(name) {
		t.Fatal("Unexpected id:", create.Id())
	}

}

func TestDeleteConnection(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()

	name := t.Name()
	d := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	err := deleteConnectionData(d, locked.Conn)
	if err == nil {
		t.Fatal("Expected error")
	}

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
			"to":   "me",
		},
	}
	err = createConnectionData(create, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	err = deleteConnectionData(d, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestExistsConnection(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()

	name := t.Name()

	exists := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deleteConnectionData(exists, locked.Conn)
	e, err := existsConnectionData(exists, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if e {
		t.Fatal("Expected exist to be false")
	}

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
			"to":   "endpoint",
		},
	}

	err = createConnectionData(create, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	e, err = existsConnectionData(exists, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if !e {
		t.Fatal("Expected exist to be true")
	}
}

func TestReadConnection(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()

	name := t.Name()

	read := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deleteConnectionData(read, locked.Conn)
	err := readConnectionData(read, locked.Conn)
	if err == nil {
		t.Fatal("Expected error by readConnectionData")
	}

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
			"to":   "bar",
		},
	}

	err = createConnectionData(create, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	err = readConnectionData(read, locked.Conn)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	readTo := read.Get("to")
	readToString, _ := readTo.(string)
	if readToString != "bar" {
		t.Fatalf("Unexpected to value: %#v", readTo)
	}
}

func TestImportConnection(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()

	name := t.Name()

	deleteData := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	deleteConnectionData(deleteData, locked.Conn)

	imp := &internal.TestData{}
	imp.SetId(name)
	err := importConnectionData(imp, locked.Conn)
	if err == nil {
		t.Fatal("Expected error from importConnectionData")
	}

	stmt := fmt.Sprintf("CREATE OR REPLACE CONNECTION %s TO 'http://foo' USER 'foo' IDENTIFIED BY 'bar'", name)
	_, err = locked.Conn.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	err = importConnectionData(imp, locked.Conn)
	if err != nil {
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
}

func TestUpdateConnection(t *testing.T) {
	locked := exaClient.Lock()
	defer locked.Unlock()

	name := t.Name()

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
			"to":   "foo",
		},
	}

	deleteConnectionData(create, locked.Conn)

	err := updateConnectionData(create, locked.Conn)
	if err == nil {
		t.Fatal("Expected error from updateConnectionData")
	}

	err = createConnectionData(create, locked.Conn)
	if err != nil {
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
		t.Fatal("Unexpexted error:", err)
	}

	read := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	err = readConnectionData(read, locked.Conn)
	if err != nil {
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
}
