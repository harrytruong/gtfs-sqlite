package main

import (
  "flag"
  "time"
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
  flag.BoolVar(&opt.SkipClean, "skipclean", opt.SkipClean,
    "Skip applying agency-specific cleanup rules for irregular GTFS files.")

  flag.Parse() // parse cli flags

  opt.GTFS = flag.Arg(0) // set "gtfsFile" from first non-flag argument
}

// main runs gtfsconv from CLI.
func main() {
  start := time.Now()

  // starting build
  log.Print("Building: This may take a while, please wait...")
  if opt.Spatialite {
    log.Print("Building: With Spatialite enabled, this takes EXTRA long!")
    log.Print("Building: Please be patient! Good stuff is coming!")
  }

  // run gtfsconv.Build
  if buildErr := gtfsconv.GoBuild(opt); buildErr != nil {
    log.Fatalf("Build failed: %s", buildErr)
  }

  // yay, finished.
  end := time.Now()
  log.Printf("Building: Finished in %0.2fs! Enjoy!", end.Sub(start).Seconds())
}
