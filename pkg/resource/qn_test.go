package resource

import "testing"

func TestGetMetaFromQNDefault(t *testing.T) {
	m, err := GetMetaFromQNDefault("tableFoo", "schemaFoo")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if m.Schema != "schemaFoo" {
		t.Fatalf("Unexpected schema (expected schemaFoo): %s", m.Schema)
	}

	if m.ObjectName != "tableFoo" {
		t.Fatalf("Unexpected table (expected tableFoo): %s", m.ObjectName)
	}

	m, err = GetMetaFromQNDefault(" ", "schemaFoo")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if m.Schema != "schemaFoo" {
		t.Fatalf("Unexpected schema (expected schemaFoo): %s", m.Schema)
	}

	if m.ObjectName != " " {
		t.Fatalf("Unexpected table (expected ` `): %s", m.ObjectName)
	}

	m, err = GetMetaFromQNDefault("schemaBar.tableBar", "schemaFoo")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if m.Schema != "schemaBar" {
		t.Fatalf("Unexpected schema (expected schemaBar): %s", m.Schema)
	}

	if m.ObjectName != "tableBar" {
		t.Fatalf("Unexpected table (expected tableBar): %s", m.ObjectName)
	}
}
