package main

import (
	"github.com/josephjohncox/wallaby/tools/specaction"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(specaction.Analyzer)
}
