package gtfsconv

import (
  "os"
  "encoding/json"
  "fmt"
  "database/sql"
  "io/ioutil"
)

// jsony Type Helper: json-like type pattern.
type jsony map[string]interface{}

// isGTFS Helper: Determines if valid GTFS file.
func isGTFS(name string) (valid bool, required bool) {
  switch name {

    // valid, required GTFS files
    case "agency.txt":
      fallthrough
    case "stops.txt":
      fallthrough
    case "routes.txt":
      fallthrough
    case "trips.txt":
      fallthrough
    case "stop_times.txt":
      fallthrough
    case "calendar.txt":
      required = true
      fallthrough

    // valid, optional GTFS files
    case "calendar_dates.txt":
      fallthrough
    case "fare_attributes.txt":
      fallthrough
    case "fare_rules.txt":
      fallthrough
    case "shapes.txt":
      fallthrough
    case "frequencies.txt":
      fallthrough
    case "transfers.txt":
      fallthrough
    case "feed_info.tx":
      valid = true
  }

  return
}

// isExistFile Helper: Check if file exists.
func isExistFile(path string) bool {
  _, err := os.Stat(path);
  return os.IsNotExist(err) == false
}

// toJSONy Helper: create jsony from key-values
func toJSONy(key []string, values []string) jsony {
  j := make(jsony)
  for i, k := range key {
    j[k] = values[i]
  }
  return j
}

// writeJSON Helper: json.Marshall(data) and write to file
func writeJSON(name string, data interface{}) error {

  // marshall into json []byte
  jsonbytes, jErr := json.Marshal(data)
  if jErr != nil {
    return fmt.Errorf("failed to json.Marshall data [%s]", jsonbytes)
  }

  // write geojson file
  if wErr := ioutil.WriteFile(name, jsonbytes, 0666);
    wErr != nil {
      return fmt.Errorf("failed to write json file [%s]", wErr)
  }

  return nil
}

// hasDBTable Helper: Check if table exists in sqlite db.
func hasDBTable(db *sql.DB, name string) bool {
  // Note: Must db.QueryRow & .Scan() in order
  //       for query error to be returned.
  var tableExists string
  if queryErr := db.QueryRow(
    "select name from sqlite_master " +
    "where type='table' AND name='"+name+"';").Scan(&tableExists);
    queryErr != nil {
    return false // could not find table
  }
  return true // table exists
}

// countDBTable Helper: Count field from table in sqlite db.
func countDBTable(db *sql.DB, field, table string) (int, error) {
  var num int // placeholder for row scan

  if err := db.QueryRow(fmt.Sprintf(
      "select count(%s) from %s;", field, table,
    )).Scan(&num); err != nil {

    return 0, fmt.Errorf("failed to count %s [%s]", table, err)
  }

  return num, nil
}

// hasDBSpatialite Helper: checks if spatialite is loaded
func hasDBSpatialite(db *sql.DB) bool {
  _, err := db.Exec("select spatialite_version();")
  return err == nil
}
