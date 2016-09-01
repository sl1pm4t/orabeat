package main

import (
	"os"

	"github.com/elastic/beats/libbeat/beat"
	ob "github.com/sl1pm4t/orabeat/beater"
)

var Name = "orabeat"

func main() {
	if err := beat.Run(Name, "", ob.New()); err != nil {
		os.Exit(1)
	}
}
