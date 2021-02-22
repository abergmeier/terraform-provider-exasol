package main

import (
	"flag"
	"fmt"
	"os"
	"path"
)

func main() {

	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprintf(flag.CommandLine.Output(), `usage: %s <command>
Available commands:
  %s
  %s
`,
			path.Base(os.Args[0]),
			searchFlags.Name(),
			setFlags.Name())
		flag.CommandLine.PrintDefaults()
		os.Exit(1)
	}

	switch flag.Arg(0) {
	case searchFlags.Name():
		mustSearchCommand(flag.Args()[1:])
	case setFlags.Name():
		mustSetCommand(flag.Args()[1:])
	}
}
