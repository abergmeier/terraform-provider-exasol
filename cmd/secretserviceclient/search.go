package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/abergmeier/terraform-provider-exasol/internal/secretservice"
)

var (
	searchFlags = flag.NewFlagSet("search", flag.ContinueOnError)
)

func printSearchUsage() {
	fmt.Fprintf(searchFlags.Output(), `usage: %s %s <connectionstring> <username>`,
		path.Base(os.Args[0]), searchFlags.Name())
	searchFlags.PrintDefaults()
}

func mustSearchCommand(args []string) {
	searchFlags.Usage = printSearchUsage

	err := searchCommand(args)
	if err != nil {
		println(err.Error())
		os.Exit(2)
	}
}

func searchCommand(args []string) error {

	if len(args) == 0 {
		return errors.New("Missing connection")
	}

	if len(args) == 1 {
		return errors.New("Missing username")
	}

	connection := args[0]
	username := args[1]

	ps, err := secretservice.SearchPassword(connection, username)
	if err != nil {
		return err
	}

	for _, p := range ps {
		println(p.Path)
	}
	return nil
}
