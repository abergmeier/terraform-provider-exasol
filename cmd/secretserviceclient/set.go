package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"syscall"

	"github.com/abergmeier/terraform-provider-exasol/internal/secretservice"
	"golang.org/x/crypto/ssh/terminal"
)

var (
	setFlags = flag.NewFlagSet("set", flag.ContinueOnError)
)

func printSetUsage() {
	fmt.Fprintf(searchFlags.Output(), `usage: %s %s <connectionstring> <username>
       %s %s <username>
`,
		path.Base(os.Args[0]), searchFlags.Name(),
		path.Base(os.Args[0]), searchFlags.Name())
	setFlags.PrintDefaults()
}

func mustSetCommand(args []string) {
	setFlags.Usage = printSetUsage

	err := setCommand(args)
	if err != nil {
		println(err.Error())
		os.Exit(2)
	}
}

func setCommand(args []string) error {

	if len(args) == 0 {
		return errors.New("Missing username")
	}

	var connection string
	var username string
	if len(args) == 1 {
		username = args[0]
	} else {
		connection = args[0]
		username = args[1]
	}
	fmt.Printf("Enter password for user %s: ", username)

	bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return err
	}

	if len(args) == 1 {
		return secretservice.SetPassword("", username, string(bytePassword))
	} else {

		return secretservice.SetPassword(connection, username, string(bytePassword))
	}
}
