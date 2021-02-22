package secretservice

import (
	"fmt"
	"strings"

	"github.com/godbus/dbus/v5"
	ss "github.com/zalando/go-keyring/secret_service"
)

func SetPassword(connection, username, password string) error {
	bus, err := dbus.SessionBus()
	if err != nil {
		return err
	}

	defer bus.Close()

	s, err := ss.NewSecretService()
	if err != nil {
		return err
	}

	sess, err := s.OpenSession()
	if err != nil {
		return err
	}

	coll := s.GetLoginCollection()
	err = s.Unlock(coll.Path())
	if err != nil {
		return err
	}

	username = strings.ToLower(username)
	attrs := map[string]string{
		"database":   "exasol",
		"connection": connection,
		"username":   username,
	}

	secret := ss.NewSecret(sess.Path(), password)
	return s.CreateItem(coll, fmt.Sprintf("Exasol Password to %s for %s", connection, username), attrs, secret)
}
