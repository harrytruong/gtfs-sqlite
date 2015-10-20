package gtfsconv

import (
  "os"
  "fmt"
  "database/sql"
  "io"
  "archive/zip"
)

// exportCSV uncompresses GTFS zip files
func exportCSV(dir string, gtfs *zip.Reader) error {
  dir += "csv/" // export to "csv" subdir

  // ensure dir exists
  if mkdirErr := os.MkdirAll(dir, 0777); mkdirErr != nil {
    return mkdirErr
  }

  // write each file directly (original csv format)
  for _, f := range gtfs.File {
    if valid, _ := isGTFS(f.Name); valid == false {
      continue // skip non-GTFS standard files
    }

    // create new csv file, with same name
    w, writeErr := os.Create(dir+f.Name)
    if writeErr != nil {
      return writeErr
    }

    // read the file from zip
    r, readErr := f.Open()
    if readErr != nil {
      return readErr
    }

    // copy straight from read to write
    if _, copyErr := io.Copy(w, r); copyErr != nil {
      return copyErr
    }
  }

  return nil
}

// exportJSON dumps basic JSON from sqlite db
func exportJSON(dir string, db *sql.DB) error {
  dir += "json/" // export to "json" subdir
  routeDir := dir+"routes/"
  stopsDir := dir+"stops/"
  tripsDir := dir+"trips/"

  // ensure dir (and extra subdirs) exists
  for _, d := range [...]string{dir, routeDir, stopsDir, tripsDir} {
    if mkdirErr := os.MkdirAll(d, 0777); mkdirErr != nil {
      return mkdirErr
    }
  }

  // list of gtfs tables to export
  gtfsTables := [...]string{
    "agency", "calendar", "calendar_dates", "fare_attributes",
    "fare_rules", "frequencies", "routes",

    // "shapes", "stop_times",
    // note: "shapes" and "stop_times" are intentionally skipped
    //       because they're not useful as basic json.

    "stops", "transfers", "trips",
  }

  // go through each sqlite gtfs table
  for _, tbl := range gtfsTables {

    // check if table exists
    if hasDBTable(db, tbl) == false {
      continue // ignore missing tables
    }

    // retrieve all rows
    rows, queryErr := db.Query(fmt.Sprintf("select * from %s;", tbl))
    if queryErr != nil {
      return fmt.Errorf("failed to select all from %s [%s]", tbl, queryErr)
    }
    defer rows.Close()

    // setup data container
    columns, _ := rows.Columns()
    values := make([]string, len(columns))
    scanner := make([]interface{}, len(columns))
    for i := range scanner {
      scanner[i] = &values[i]
    }

    // iterate over each query result row
    var jsonCol []jsony
    for rows.Next() {
      if scanErr := rows.Scan(scanner...); scanErr != nil {
        return fmt.Errorf("failed to scan %s query [%s]", tbl, scanErr)
      }

      // create json-y object for row
      jsonRow := toJSONy(columns, values)

      // append to json collection
      jsonCol = append(jsonCol, jsonRow)

      // write extra individual json files
      file := ""
      switch tbl {
        case "routes":  file = routeDir+jsonRow["route_id"].(string)+".json"
        case "stops":   file = stopsDir+jsonRow["stop_id"].(string)+".json"
        case "trips":   file = tripsDir+jsonRow["trip_id"].(string)+".json"
      }
      if file != "" {
        if wErr := writeJSON(file, jsonRow); wErr != nil {
          return fmt.Errorf("writeJSON() for row in %s [%s]", tbl, wErr)
        }
      }
    }

    // write json array file for all rows
    if wErr := writeJSON(dir + tbl + ".json", jsonCol); wErr != nil {
      return fmt.Errorf("writeJSON() for all rows in %s [%s]", tbl, wErr)
    }
  }

  return nil
}
