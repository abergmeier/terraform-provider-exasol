package websocket

import (
	"io"
	"os"
	"path"
	"testing"
)

func tempRead(t *testing.T, tempDir string, p string) (io.ReadCloser, error) {
	wp := path.Join(tempDir, p)
	return os.Open(wp)
}

func tempWrite(t *testing.T, tempDir string, p string) io.WriteCloser {
	wp := path.Join(tempDir, p)
	dir := path.Dir(wp)
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		t.Fatalf("MkdirAll %s failed: %s", dir, err)
	}
	f, err := os.Create(wp)
	if err != nil {
		t.Fatalf("Creating %s failed: %s", wp, err)
	}
	return f
}

func TestStoreNil(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	c := JSONCache{
		openForWrite: func(p string) io.WriteCloser {
			return tempWrite(t, tempDir, p)
		},
	}

	err := c.store("23", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStoreAndRead(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	c := JSONCache{
		openForRead: func(p string) (io.ReadCloser, error) {
			return tempRead(t, tempDir, p)
		},
		openForWrite: func(p string) io.WriteCloser {
			return tempWrite(t, tempDir, p)
		},
	}

	err := c.store("64", map[string]interface{}{
		"foo": 42,
	}, map[string]interface{}{
		"bar": 128,
	})
	if err != nil {
		t.Fatal(err)
	}

	r, err := c.load("64", map[string]interface{}{
		"foo": 42,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("NIL")
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"bar":128}` {
		t.Fatalf(`Expected {"bar":128}. Loaded:\n%s`, string(b))
	}
}
