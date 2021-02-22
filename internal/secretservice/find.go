package secretservice

import (
	"strings"

	"github.com/godbus/dbus/v5"
	ss "github.com/zalando/go-keyring/secret_service"
)

type credentials struct {
	Username string
	Password string
}

type Password struct {
	Path  dbus.ObjectPath
	Value string
}

func SearchPassword(connection, username string) ([]Password, error) {

	bus, err := dbus.SessionBus()
	if err != nil {
		return nil, err
	}

	defer bus.Close()

	s, err := ss.NewSecretService()
	if err != nil {
		return nil, err
	}

	sess, err := s.OpenSession()
	if err != nil {
		return nil, err
	}

	cp, err := searchCredentialsByConnection(s, sess, connection, username)
	if err != nil {
		return nil, err
	}

	if cp == nil || len(cp) == 0 {
		cp, err = searchCredentialsByConnection(s, sess, "", username)
		if err != nil {
			return nil, err
		}
	}

	if cp == nil || len(cp) == 0 {
		return nil, dbus.ErrMsgNoObject
	}

	ret := make([]Password, len(cp))
	for i := range cp {
		sec, err := s.GetSecret(cp[i], sess.Path())
		if err != nil {
			return nil, err
		}
		ret[i].Path = cp[i]
		ret[i].Value = string(sec.Value)
	}
	return ret, nil
}

func searchCredentialsByConnection(s *ss.SecretService, sess dbus.BusObject, connection, username string) ([]dbus.ObjectPath, error) {
	coll := s.GetLoginCollection()

	search := map[string]string{
		"database":   "exasol",
		"connection": connection,
		"username":   strings.ToLower(username),
	}

	err := s.Unlock(coll.Path())
	if err != nil {
		return nil, err
	}

	return s.SearchItems(coll, search)
}
