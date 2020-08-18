package internal

type TestData struct {
	id        string
	Values    map[string]interface{}
	NewValues map[string]interface{}
}

func (d *TestData) Get(name string) interface{} {
	v, ok := d.Values[name]
	if !ok {
		return nil
	}
	return v
}

func (d *TestData) Set(name string, value interface{}) error {
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
	_, ok := d.NewValues[name]
	return ok
}

func (d *TestData) GetChange(name string) (interface{}, interface{}) {
	return d.Values[name], d.NewValues[name]
}
