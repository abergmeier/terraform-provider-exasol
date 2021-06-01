package internal

import "reflect"

type TestData struct {
	id        string
	Values    map[string]interface{}
	NewValues map[string]interface{}
}

func (d *TestData) Get(name string) interface{} {
	v, ok := d.GetOk(name)
	if !ok {
		return nil
	}
	return v
}

func (d *TestData) GetOk(name string) (v interface{}, ok bool) {
	if d.NewValues == nil {
		v, ok = d.Values[name]
	} else {
		v, ok = d.NewValues[name]
	}
	return
}

func (d *TestData) Set(name string, value interface{}) error {
	if d.Values == nil {
		d.Values = map[string]interface{}{}
	}
	d.Values[name] = value
	return nil
}

func (d *TestData) SetId(id string) {
	d.id = id
}

func (d *TestData) Id() string {
	return d.id
}

func (d *TestData) HasChange(name string) bool {
	if d.NewValues == nil {
		return false
	}
	old, oldOk := d.Values[name]
	new, newOk := d.NewValues[name]
	if (oldOk && !newOk) || (!oldOk && !newOk) {
		return true
	}
	return !reflect.DeepEqual(old, new)
}

func (d *TestData) GetChange(name string) (interface{}, interface{}) {
	return d.Values[name], d.NewValues[name]
}
