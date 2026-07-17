package main

import (
	"context"
	"fmt"
	"os"

	"github.com/muratmirgun/yordam/internal/buildinfo"
	"github.com/muratmirgun/yordam/internal/cli"
	"github.com/muratmirgun/yordam/internal/tui"
)

func main() {
	options, err := cli.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "yordam:", err)
		os.Exit(2)
	}
	if options.Version {
		fmt.Printf("yordam %s (%s, %s)\n", buildinfo.Version, buildinfo.Commit, buildinfo.Date)
		return
	}
	if err := tui.Run(context.Background(), options); err != nil {
		fmt.Fprintln(os.Stderr, "yordam:", err)
		os.Exit(1)
	}
}
