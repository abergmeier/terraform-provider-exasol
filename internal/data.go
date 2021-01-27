package internal

type Data interface {
	Get(name string) interface{}
	GetOk(name string) (interface{}, bool)
	Set(name string, d interface{}) error
	Id() string
	SetId(id string)
	HasChange(name string) bool
	GetChange(name string) (interface{}, interface{})
}
