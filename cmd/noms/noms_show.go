// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"

	"github.com/stormasm/nomsredis/cmd/util"
	"github.com/stormasm/nomsredis/go/config"
	"github.com/stormasm/nomsredis/go/d"
	"github.com/stormasm/nomsredis/go/types"
	"github.com/stormasm/nomsredis/go/util/outputpager"
	"github.com/stormasm/nomsredis/go/util/verbose"
	flag "github.com/juju/gnuflag"
)

var nomsShow = &util.Command{
	Run:       runShow,
	UsageLine: "show <object>",
	Short:     "Shows a serialization of a Noms object",
	Long:      "See Spelling Objects at https://github.com/stormasm/nomsredis/blob/master/doc/spelling.md for details on the object argument.",
	Flags:     setupShowFlags,
	Nargs:     1,
}

func setupShowFlags() *flag.FlagSet {
	showFlagSet := flag.NewFlagSet("show", flag.ExitOnError)
	outputpager.RegisterOutputpagerFlags(showFlagSet)
	verbose.RegisterVerboseFlags(showFlagSet)
	return showFlagSet
}

func runShow(args []string) int {
	cfg := config.NewResolver()
	database, value, err := cfg.GetPath(args[0])
	d.CheckErrorNoUsage(err)
	defer database.Close()

	if value == nil {
		fmt.Fprintf(os.Stderr, "Object not found: %s\n", args[0])
		return 0
	}

	pgr := outputpager.Start()
	defer pgr.Stop()

	types.WriteEncodedValueWithTags(pgr.Writer, value)
	fmt.Fprintln(pgr.Writer)
	return 0
}
