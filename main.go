package main

import (
  "flag"
  "log"
  "github.com/harrytruong/gtfs-sqlite/gtfsconv"
)

// opt: runtime config container
//      see gtfs.options
var opt gtfsconv.Options

// init parses CLI flags/args into "opt"
func init() {

  // get default values
  opt = gtfsconv.DefaultOptions()

  // setup flags from CLI
  flag.StringVar(&opt.Dir, "dir", opt.Dir,
    "Output file directory.")
  flag.StringVar(&opt.Name, "name", opt.Name,
    "Output sqlite filename.")
  flag.BoolVar(&opt.SkipExtras, "skip-extras", opt.SkipExtras,
    "Skip extra export file formats (csv, json, geojson, kml).")
  flag.BoolVar(&opt.Spatialite, "spatialite", opt.Spatialite,
    "Include spatialite-enabled sqlite tables.")
  flag.BoolVar(&opt.KeepDB, "keepdb", opt.KeepDB,
    "Reuse existing sqlite db, if exist.")

  flag.Parse() // parse cli flags

  opt.GTFS = flag.Arg(0) // set "gtfsFile" from first non-flag argument
}

// main runs gtfsconv from CLI.
func main() {

  // log: starting build
  log.Print("Building: This may take a while, please wait...")

  // run gtfsconv.Build
  if buildErr := gtfsconv.Build(opt); buildErr != nil {
    log.Fatalf("Build failed: %s", buildErr)
  }

  // log: yay, finished.
  log.Print("Building: Finished! Enjoy!")
}
