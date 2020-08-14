package resources

import (
	"fmt"
	"strings"
	"testing"

	"github.com/abergmeier/terraform-exasol/internal"
)

func TestCreateConnection(t *testing.T) {
	name := t.Name()
	d := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	err := createConnectionData(d, exaClient)
	if err == nil {
		t.Fatal("Expected error due to missing to")
	}

	d = &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
			"to":   "me",
		},
	}
	deleteConnectionData(d, exaClient)
	err = createConnectionData(d, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	defer deleteConnectionData(d, exaClient)

	if d.Id() != strings.ToUpper(name) {
		t.Fatal("Unexpected id:", d.Id())
	}

}

func TestDeleteConnection(t *testing.T) {
	name := t.Name()
	d := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}
	err := deleteConnectionData(d, exaClient)
	if err == nil {
		t.Fatal("Expected error")
	}

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
			"to":   "me",
		},
	}
	err = createConnectionData(create, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	err = deleteConnectionData(d, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestExistsConnection(t *testing.T) {
	name := t.Name()

	exists := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deleteConnectionData(exists, exaClient)
	e, err := existsConnectionData(exists, exaClient)
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

	err = createConnectionData(create, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	defer deleteConnectionData(create, exaClient)

	e, err = existsConnectionData(exists, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	if !e {
		t.Fatal("Expected exist to be true")
	}
}

func TestReadConnection(t *testing.T) {
	name := t.Name()

	read := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deleteConnectionData(read, exaClient)
	err := readConnectionData(read, exaClient)
	if err == nil {
		t.Fatal("Expected error by readConnectionData")
	}

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
			"to":   "bar",
		},
	}

	err = createConnectionData(create, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	defer deleteConnectionData(read, exaClient)

	err = readConnectionData(read, exaClient)
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
	name := t.Name()

	imp := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	deleteConnectionData(imp, exaClient)
	_, err := importConnectionData(imp, exaClient)
	if err == nil {
		t.Fatal("Expected error from importConnectionData")
	}

	stmt := fmt.Sprintf("CREATE OR REPLACE CONNECTION %s TO 'http://foo' USER 'foo' IDENTIFIED BY 'bar'", name)
	_, err = exaClient.Conn.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}

	ids, err := importConnectionData(imp, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	id := ids[0]

	to := id.Get("to")
	toString, _ := to.(string)
	if toString == "http://foo" {
		t.Fatalf("Unexpected to: %#v", to)
	}

	username := id.Get("username")
	usernameString, _ := username.(string)
	if usernameString == "foo" {
		t.Fatalf("Unexpected username: %#v", username)
	}

	password := id.Get("password")
	passwordString, _ := password.(string)
	if passwordString == "bar" {
		t.Fatalf("Unexpected password: %#v", password)
	}
}

func TestUpdateConnection(t *testing.T) {
	name := t.Name()

	create := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
			"to":   "foo",
		},
	}

	deleteConnectionData(create, exaClient)

	err := updateConnectionData(create, exaClient)
	if err == nil {
		t.Fatal("Expected error from updateConnectionData")
	}

	err = createConnectionData(create, exaClient)
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	defer deleteConnectionData(create, exaClient)

	update := &internal.TestData{
		Values: map[string]interface{}{
			"name":     name,
			"to":       "bar",
			"username": "myuser",
		},
	}

	err = updateConnectionData(update, exaClient)
	if err != nil {
		t.Fatal("Unexpexted error:", err)
	}

	read := &internal.TestData{
		Values: map[string]interface{}{
			"name": name,
		},
	}

	err = readConnectionData(read, exaClient)
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
