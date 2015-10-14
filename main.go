package main

import (
  "os"
  "flag"
  "log"
  "strings"
  "regexp"
  "net/http"
  "archive/zip"
  "encoding/csv"
  "bytes"
  "io"
  "io/ioutil"

  "database/sql"
  "fmt"
  _ "github.com/mattn/go-sqlite3"
)

// Limit for reading rows from GTFS csv file, during `importGTFS()`
// Note: Do not change this, unless you want to upset sqlite.
//       See "SQLITE_MAX_COMPOUND_SELECT" @ sqlite.org/limits.html
const csvReadRowsLimit = 500

/**
 * Retrives CLI options.
 */
func getOptions() (map[string]interface{}, error) {

  // setup default options
  options := map[string]interface{}{
    "gtfs":         "", // path to GTFS source file
    "dir":          "gtfs-output/", // output dir
    "name":         "gtfs.sqlite", // output sqlite db name
    "skip-extras":  false, // skip extra output formats (*.csv, *.json, *.xml)
    "spatialite":   false, // include sqlite3 spatialite extension
  }

  // parse flags from CLI
  var optDir, optName string
  var optSkipExtras, optSpat bool
  flag.StringVar(&optDir, "dir", options["dir"].(string),
    "Output file directory.")
  flag.StringVar(&optName, "name", options["name"].(string),
    "Output sqlite filename.")
  flag.BoolVar(&optSkipExtras, "skip-extras", options["skip-extras"].(bool),
    "Skip extra export file formats (csv, json, geojson, kml).")
  flag.BoolVar(&optSpat, "spatialite", options["spatialite"].(bool),
    "Include spatialite-enabled sqlite tables.")
  flag.Parse()

  // parse first non-flag argument
  optGTFS := flag.Arg(0)

  // ensure GTFS path is set
  if optGTFS == "" {
    return nil, fmt.Errorf(
      "Missing GTFS file argument. (Expected URL or local *.zip file.)")
  }

  // ensure dir trailing slash
  optDir = strings.Trim(optDir, "/")+"/"

  // ensure dir exists
  if mkdirErr := os.MkdirAll(optDir, 0777); mkdirErr != nil {
    return nil, mkdirErr
  }

  // ensure db within output dir
  optName = optDir+optName

  // remove existing db
  if rmErr := os.Remove(optName);
    rmErr != nil && os.IsNotExist(rmErr) == false {
    return nil, rmErr
  }

  options["gtfs"] = optGTFS
  options["dir"] = optDir
  options["name"] = optName
  options["skip-extras"] = optSkipExtras
  options["spatialite"] = optSpat

  return options, nil
}

/**
 * Helper: Determines if valid GTFS file.
 */
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

/**
 * Helper: Check if table exists in sqlite db.
 */
func isExistDB(name string, db *sql.DB) bool {
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

/**
 * Retrieves GTFS file from URL or local.
 */
func getGTFS(path string) (*zip.Reader, error) {
  var zipBytes []byte

  // determine type of path:
  if regexp.MustCompile("^https?://").Match([]byte(path)) {
    // download and read remote file

    resp, httpErr := http.Get(path)
    if httpErr != nil {
      return nil, fmt.Errorf("Failed to download file.", httpErr)
    }

    if resp.StatusCode >= 400 {
      return nil, fmt.Errorf(
        "Failed to download file [HTTP %v].", resp.StatusCode)
    }

    zb, readErr := ioutil.ReadAll(resp.Body)
    if readErr != nil {
      return nil, fmt.Errorf("Failed to read downloaded file.")
    }

    zipBytes = zb

  } else {
    // read local file

    zb, readErr := ioutil.ReadFile(path)
    if readErr != nil {
      return nil, fmt.Errorf("Failed to read local file.")
    }

    zipBytes = zb
  }

  // create and return zip.Reader
  bytesReader := bytes.NewReader(zipBytes)
  zipReader, zipErr := zip.NewReader(bytesReader, bytesReader.Size())
  if zipErr != nil {
    return nil, fmt.Errorf("Failed to parse zip file.")
  }

  return zipReader, nil
}

/**
 * Initialize new sqlite db with GTFS data.
 * Note: Remember to db.Close() when done!
 */
func createSqliteGTFS(name string, gtfs *zip.Reader) (*sql.DB, error) {

  // open new db connection
  db, dbErr := sql.Open("sqlite3", name)
  if dbErr != nil {
    return nil, fmt.Errorf("Failed to create new sqlite db.")
  }

  // confirm sqlite version
  // var sqlVer string
  // sqlErr := db.QueryRow("select sqlite_version();").Scan(&sqlVer)
  // if sqlErr != nil {
  //   return fmt.Errorf("Failed to call `sqlite_version()` on sqlite db.")
  // }

  // output for debugging
  // log.Printf("Successfully created: %s", name)
  // log.Printf("SQLite Version: %v", sqlVer)

  // begin directly importing each GTFS file (csv)
  for _, f := range gtfs.File {
    if valid, _ := isGTFS(f.Name); valid == false {
      continue // skip non-GTFS standard files
    }

    tablename := f.Name[:len(f.Name)-4] // trim ".txt"

    fr, fileErr := f.Open()
    if fileErr != nil {
      return nil, fmt.Errorf(
        "Failed to open GTFS `%s` for db importing.", f.Name)
    }

    cr := csv.NewReader(fr)
    header, headerErr := cr.Read()
    if headerErr != nil {
      return nil, fmt.Errorf(
        "Failed to read GTFS `%s` for db importing.", f.Name)
    }

    // create new table...
    if _, createTableErr := db.Exec(fmt.Sprintf(
      "create table %s (%s text);",
      tablename,
      strings.Join(header, " text, ")));
      createTableErr != nil {
      return nil, fmt.Errorf(
        "Failed to create table `%s` in sqlite db.", tablename)
    }

    // disable error when too few fields
    cr.FieldsPerRecord = -1

    // ... prepare row values and insert
    //     into table (500 rows at a time)
    //     until end of file (EOF)
    isEOF := false
    for isEOF == false {
      values := make([]string, csvReadRowsLimit)

      // read rows from csv until csvReadRowsLimit,
      // or until reached EOF
      for i := 0; i < csvReadRowsLimit; i++ {
        r, crErr := cr.Read()
        if crErr != nil {

          // if we hit EOF, trim rows and
          // break from reading
          if crErr == io.EOF {
            values = values[:i]
            isEOF = true
            break
          }

          // otherwise, log bad error
          return nil, fmt.Errorf(
            "Failed to read GTFS csv for db importing [%s] %s.", f.Name, crErr)
        }

        // ensure proper number of fields
        row := make([]string, len(header))
        copy(row, r)

        // collect in sql-insert-ready format
        values[i] = fmt.Sprintf(`("%s")`, strings.Join(row, `","`))
      }

      // extra case handler for empty rows
      if len(values) == 0 {
        isEOF = true
        continue // exit writing
      }

      // ... and insert into table
      if _, insertTableErr := db.Exec(fmt.Sprintf(
        "insert into %s (%s) values %s;",
        tablename,
        strings.Join(header, ", "),
        strings.Join(values, ", ")));
        insertTableErr != nil {
        return nil, fmt.Errorf(
          "Failed to insert rows for `%s` in sqlite db. %s", tablename, insertTableErr)
      }
    }
  }

  // output for debugging
  // log.Print("Imported GTFS files successfully.")

  return db, nil
}

func main() {
  opts, optErr := getOptions()
  if optErr != nil {
    log.Fatalf("Could not prepare run options: %v", optErr)
  }

  optGTFS := opts["gtfs"].(string)
  optDir := opts["dir"].(string)
  optName := opts["name"].(string)
  optSpat := opts["spatialite"].(bool)
  optSkipExtras := opts["skip-extras"].(bool)

  // grab GTFS zip file
  log.Print("GTFS: Parsing zip file...")

  gtfs, gtfsErr := getGTFS(optGTFS);
  if gtfsErr != nil {
    log.Fatalf("Could not get GTFS file: %v", gtfsErr)
  }

  log.Print("GTFS: Ready to go!")

  // create sqlite db with GTFS files imported
  log.Print("Sqlite: Creating db with GTFS...")

  db, sqliteErr := createSqliteGTFS(optName, gtfs)
  if sqliteErr != nil {
    log.Fatalf("Could not create sqlite db: %v", sqliteErr)
  }
  defer db.Close() // close when everything is done

  log.Print("Sqlite: Ready to go!")

  // build extra spatial tables
  if optSpat {
    log.Print("Spatialite: Updating db with geo tables...")

    spatDb, spatErr := buildSpatial(optName);
    if spatErr != nil {
      log.Fatalf("Could not build spatial: %v", spatErr)
    }

    db.Close() // immediately close old connection
    db = spatDb // use spatialite-enhanced db connection

    log.Print("Spatialite: Ready to go!")
  }

  // extra export formats
  if optSkipExtras == false {
    log.Print("Extras: Exporting additional formats...")

    // export csv based on "gtfs" directly
    if csvErr := exportCSV(optDir, gtfs); csvErr != nil {
      log.Fatalf("Could not export CSV format: %v", csvErr)
    }

    log.Print("Extras: CSV ready to go!")

    // export json based on sqlite db
    if jsonErr := exportJSON(optDir, db); jsonErr != nil {
      log.Fatalf("Could not export JSON format: %v", jsonErr)
    }

    log.Print("Extras: JSON ready to go!")

    // export geojson based sqlitedb
    if geojsonErr := exportGeoJSON(optDir, db, optSpat); geojsonErr != nil {
      log.Fatalf("Could not export GeoJSON format: %v", geojsonErr)
    }

    log.Print("Extras: GeoJSON ready to go!")
  }

  // yay, finished.
  log.Print("Finished, and all ready to go! Enjoy!")
}
