package main

import (
  "os"
  // "fmt"
  "database/sql"
  // "io/ioutil"
  // "encoding/json"
)

/**
 * GeoJSON export of GTFS files.
 */
func exportGeoJSON(dir string, db *sql.DB) error {
  dir += "geojson/" // export to "json" subdir
  shapesDir := dir+"shapes/"
  stopsDir := dir+"stops/"
  transfersDir := dir+"transfers/"

  // ensure dir (and extra subdirs) exists
  for _, d := range [...]string{dir, shapesDir, stopsDir, transfersDir} {
    if mkdirErr := os.MkdirAll(d, 0777); mkdirErr != nil {
      return mkdirErr
    }
  }

  // todo...

  return nil
}
