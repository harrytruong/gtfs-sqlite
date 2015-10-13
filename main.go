package main

import (
  "log"
  "strings"
  "os"
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
    "dir":        "gtfs-output/", // output dir
    "name":       "gtfs.sqlite", // output sqlite db name
    "extras":     true, // extra output formats (*.csv, *.json, *.xml)
    "spatialite": true, // include sqlite3 spatialite extension
    "server":     false, // start a basic HTTP server
  }

  // todo: parse flag options

  optDir := options["dir"].(string)
  optName := options["name"].(string)
  optExtras := options["extras"].(bool)
  optSpat := options["spatialite"].(bool)
  optServer := options["server"].(bool)

  // option cleanup
  optDir = strings.Trim(optDir, "/")+"/"  // dir trailing slash
  if mkdirErr := os.MkdirAll(optDir, 0777); mkdirErr != nil { // dir exists
    return nil, mkdirErr
  }
  optName = optDir+optName // db within output dir
  if rmErr := os.Remove(optName); // remove existing db
    rmErr !=nil && os.IsNotExist(rmErr) == false {
    return nil, rmErr
  }

  options["dir"] = optDir
  options["name"] = optName
  options["extras"] = optExtras
  options["spatialite"] = optSpat
  options["server"] = optServer

  return options, nil
}

/**
 * Retrieves GTFS file from URL or local.
 */
func getGTFS() (*zip.Reader, error) {

  // expect path to be passed as first argument
  if len(os.Args) <= 1 {
    return nil, fmt.Errorf(
      "Missing GTFS file argument. (Expected URL or local *.zip file.)")
  }

  // determine type of path
  path := os.Args[1]
  var zipBytes []byte

  // download and read remote file
  if regexp.MustCompile("^https?://").Match([]byte(path)) {

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

  } else { // read local file

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
            "Failed to read GTFS csv for db importing [%s].", f.Name)
        }

        // collect in sql-insert-ready format
        values[i] = fmt.Sprintf(`("%s")`, strings.Join(r, `","`))
      }

      // ... and insert into table
      if _, insertTableErr := db.Exec(fmt.Sprintf(
        "insert into %s (%s) values %s;",
        tablename,
        strings.Join(header, ", "),
        strings.Join(values, ", ")));
        insertTableErr != nil {
        return nil, fmt.Errorf(
          "Failed to insert rows for `%s` in sqlite db.", tablename)
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

  optDir := opts["dir"].(string)
  optName := opts["name"].(string)
  optSpat := opts["spatialite"].(bool)
  optExtras := opts["extras"].(bool)

  // grab GTFS zip file
  log.Print("GTFS: Parsing zip file...")

  gtfs, gtfsErr := getGTFS();
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

  // extra export formats
  if optExtras {
    log.Print("Extras: Exporting additional formats...")

    // csv export dumps GTFS zip files directly
    if csvErr := exportCSV(optDir, gtfs); csvErr != nil {
      log.Fatalf("Could not export CSV format: %v", csvErr)
    }
    log.Print("Extras: CSV ready to go!")

    // other formats require sqlite db connection
    // if jsonErr := exportJSON(optDir, db); jsonErr != nil {
    //   log.Fatalf("Could not export JSON format: %v", jsonErr)
    // }
    // log.Print("Extras: JSON ready to go!")
  }

  // build extra spatial tables
  if optSpat {
    log.Print("Spatialite: Updating db with geo tables...")

    if spatErr := buildSpatial(optName); spatErr != nil {
      log.Fatalf("Could not build spatial: %v", spatErr)
    }
    log.Print("Spatialite: Ready to go!")
  }
}
