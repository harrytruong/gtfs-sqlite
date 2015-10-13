package main

import (
  "os"
  "fmt"
  "database/sql"
  "io"
  "io/ioutil"
  "archive/zip"
  "encoding/json"
)

/**
 * Basic CSV export of GTFS files.
 */
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

/**
 * Basic JSON export of GTFS files.
 */
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
  // note: "shapes" and "stop_times" are intentionally skipped
  //       because they're not useful as basic json.
  gtfsTables := [...]string{
    "agency", "calendar", "calendar_dates", "fare_attributes",
    "fare_rules", "frequencies", "routes",
    // "shapes", "stop_times",
    "stops", "transfers", "trips",
  }

  // go through each sqlite gtfs table
  for _, tbl := range gtfsTables {

    // check if table exists
    var tblExist string
    if tblErr := db.QueryRow(
      "select name from sqlite_master " +
      "where type='table' AND name='"+tbl+"';").Scan(&tblExist);
      tblErr != nil {
      continue
    }

    // create new general json file
    w, writeErr := os.Create(dir + tbl + ".json")
    if writeErr != nil {
      return writeErr
    }
    defer w.Close()

    // wrap entire json file with "[]",
    // to reprsent an array of objects,
    // one for each row
    if _, arrWErr := w.Write([]byte("[")); arrWErr != nil {
      return arrWErr
    }

    // retrieve all rows
    rows, queryErr := db.Query(fmt.Sprintf("select * from %s;", tbl))
    if queryErr != nil {
      return queryErr
    }
    defer rows.Close()

    // setup data container
    data := make(map[string]*string)
    columns, _ := rows.Columns()
    values := make([]string, len(columns))
    scanner := make([]interface{}, len(columns))
    for i := range scanner {
      data[columns[i]] = &values[i]
      scanner[i] = &values[i]
    }

    // iterate over each query result row
    writeComma := false
    for rows.Next() {
      if scanErr := rows.Scan(scanner...); scanErr != nil {
        return scanErr
      }

      // marshal data into JSON []byte
      jsonBytes, jsonErr := json.Marshal(data)
      if jsonErr != nil {
        return jsonErr
      }

      // write comma-separator
      if writeComma {
        if _, jsonWErr := w.Write([]byte(",")); jsonWErr != nil {
          return jsonWErr
        }
      } else {
        writeComma = true // for use with next row
      }

      // write to general json file
      if _, jsonWErr := w.Write(jsonBytes); jsonWErr != nil {
        return jsonWErr
      }

      // write extra individual json files
      switch tbl {
        case "routes":

          if jsonWriteErr := ioutil.WriteFile(
            routeDir+*data["route_id"]+".json",
            jsonBytes, 0666); jsonWriteErr != nil {
            return jsonWriteErr
          }

        case "stops":
          if jsonWriteErr := ioutil.WriteFile(
            stopsDir+*data["stop_id"]+".json",
            jsonBytes, 0666); jsonWriteErr != nil {
            return jsonWriteErr
          }

        case "trips":
          if jsonWriteErr := ioutil.WriteFile(
            tripsDir+*data["trip_id"]+".json",
            jsonBytes, 0666); jsonWriteErr != nil {
            return jsonWriteErr
          }
      }
    }

    // close json file with "]"
    // (for end array of objects)
    if _, arrWErr := w.Write([]byte("]")); arrWErr != nil {
      return arrWErr
    }
  }

  return nil
}
