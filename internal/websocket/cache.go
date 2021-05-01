package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"cloud.google.com/go/storage"
	gws "github.com/gorilla/websocket"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
)

type JSONCache struct {
	Storage *storage.BucketHandle
	ws      *gws.Conn

	cachedResp  io.ReadCloser
	lastRequest interface{}
	currentId   string
	// Hack to not access Storage when testing
	openForRead  func(path string) (io.ReadCloser, error)
	openForWrite func(path string) io.WriteCloser
	caching      Caching
}

type Caching struct {
	id    string
	diags diag.Diagnostics
}

func (c *Caching) Close() error {
	return nil
}

func (c *JSONCache) Close() error {
	c.caching.id = ""
	c.lastRequest = nil
	if c.cachedResp != nil {
		c.cachedResp.Close()
		c.cachedResp = nil
	}
	return c.ws.Close()
}

func (c *JSONCache) Record(id string) *Caching {
	if c.caching.id != id {
		panic("Caching state corrupted in Record")
	}
	return &c.caching
}

func (c *JSONCache) Invalidate(id string) {
	c.delete(id)
}

func (c *JSONCache) EnableWriteCompression(b bool) {
	c.ws.EnableWriteCompression(b)
}

func (c *JSONCache) WriteJSON(request interface{}) error {
	c.lastRequest = request
	// Try read form cache first
	r := c.tryLoad(c.currentId, request)
	if r != nil {
		c.cachedResp = r
		return nil
	}
	// Write to Websocket
	return c.ws.WriteJSON(request)
}

func (c *JSONCache) ReadJSON(response interface{}) error {
	// Try write from cache first
	if c.cachedResp != nil {
		defer func() {
			c.cachedResp.Close()
			c.cachedResp = nil
		}()
		d := json.NewDecoder(c.cachedResp)
		return d.Decode(response)
	}
	// Read from Websocket
	err := c.ws.ReadJSON(response)
	if err != nil {
		c.warnf("Could not cache due to ReadJSON Error: %s", err)
		return err
	}
	r := reflect.Indirect(reflect.ValueOf(response))
	status := r.FieldByName("Status").String()
	if status != "ok" {
		err := reflect.Indirect(r.FieldByName("Exception")).
			FieldByName("Text").String()
		c.warnf("Could not cache due to Server Error: %s", err)
		return nil
	}
	c.tryStore(c.currentId, c.lastRequest, response)
	return nil
}

func (c *JSONCache) delete(id string) {

}

func (c *JSONCache) tryLoad(id string, request interface{}) io.ReadCloser {
	r, err := c.load(id, request)
	if err != nil {
		c.warnf("Could not load from cache due to: %s", err)
		return nil
	}
	return r
}

func (c *JSONCache) load(id string, request interface{}) (io.ReadCloser, error) {
	p, err := pathFromRequest(id, request)
	if err != nil {
		return nil, err
	}
	if c.openForRead == nil {
		return c.openForStorageRead(p)
	} else {
		return c.openForRead(p)
	}
}

func (c *JSONCache) openForStorageRead(path string) (io.ReadCloser, error) {
	h := c.Storage.Object(path)
	return h.NewReader(context.Background())
}

func (c *JSONCache) openForStorageWrite(path string) io.WriteCloser {
	h := c.Storage.Object(path)
	return h.NewWriter(context.Background())
}

func (c *JSONCache) tryStore(id string, request interface{}, response interface{}) {
	err := c.store(id, c.lastRequest, response)
	if err != nil {
		c.warnf("Could not store to cache due to: %s", err)
	}
}

func (c *JSONCache) store(id string, request interface{}, response interface{}) error {
	path, err := pathFromRequest(id, request)
	if err != nil {
		return err
	}
	jresp, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("cannot marshal response\n%#v:\n%s", response, err)
	}
	var w io.WriteCloser
	if c.openForWrite == nil {
		w = c.openForStorageWrite(path)
	} else {
		w = c.openForWrite(path)
	}
	defer w.Close()
	_, err = io.WriteString(w, string(jresp))
	if err != nil {
		return fmt.Errorf("write to %s failed: %s", path, err)
	}
	return nil
}

func (c *JSONCache) warnf(format string, a ...interface{}) {
	c.caching.diags = append(c.caching.diags, diag.Diagnostic{
		Severity: diag.Warning,
		Summary:  fmt.Sprintf(format, a...),
	})
}
