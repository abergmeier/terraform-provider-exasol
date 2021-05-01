package test

type Data struct {
	id        string
	Values    map[string]interface{}
	NewValues map[string]interface{}
}

func (d *Data) Get(name string) interface{} {
	v, ok := d.Values[name]
	if !ok {
		return nil
	}
	return v
}

func (d *Data) GetOk(name string) (interface{}, bool) {
	v, ok := d.Values[name]
	return v, ok
}

func (d *Data) Set(name string, value interface{}) error {
	if d.Values == nil {
		d.Values = map[string]interface{}{}
	}
	d.Values[name] = value
	return nil
}

func (d *Data) SetId(id string) {
	d.id = id
}

func (d *Data) Id() string {
	return d.id
}

func (d *Data) HasChange(name string) bool {
	_, ok := d.NewValues[name]
	return ok
}

func (d *Data) GetChange(name string) (interface{}, interface{}) {
	return d.Values[name], d.NewValues[name]
}
