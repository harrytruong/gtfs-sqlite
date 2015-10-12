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
  "github.com/mattn/go-sqlite3"
)

// Limit for reading rows from GTFS csv file, during `importGTFS()`
// Note: Do not change this, unless you want to upset sqlite.
//       See "SQLITE_MAX_COMPOUND_SELECT" @ sqlite.org/limits.html
const csvReadRowsLimit = 500

/**
 * Retrives CLI options.
 */
func getOptions() (options map[string]interface{}) {

  // setup default options
  options = map[string]interface{}{
    "dir":        "gtfs-output/", // output dir
    "name":       "gtfs.sqlite", // output sqlite db name
    "spatialite": true, // include sqlite3 spatialite extension
    "extras":     true, // extra output formats (*.csv, *.json, *.xml)
  }

  // todo: parse flag options

  optDir := options["dir"].(string)
  optName := options["name"].(string)
  optSpat := options["spatialite"].(bool)
  optExtras := options["extras"].(bool)

  // option cleanup
  optDir = strings.Trim(optDir, "/")+"/"  // dir trailing slash
  os.MkdirAll(optDir, 0777)  // dir exists
  optName = optDir+optName // db within output dir
  os.Remove(optName) // remove existing db

  options["dir"] = optDir
  options["name"] = optName
  options["extras"] = optExtras
  options["spatialite"] = optSpat

  return
}

/**
 * Retrieves GTFS file from URL or local.
 */
func getGTFS() (z *zip.Reader) {

  // expect path to be passed as first argument
  if len(os.Args) <= 1 {
    log.Fatal("Missing GTFS file argument. (Expected URL or local *.zip file.)")
  }

  // determine type of path
  path := os.Args[1]
  isHTTP := regexp.MustCompile("^https?://").Match([]byte(path))
  var zipBytes []byte

  if isHTTP { // download and read remote file

    resp, httpErr := http.Get(path)
    if httpErr != nil {
      log.Fatalf("Failed to download file.", httpErr)
    }

    if resp.StatusCode >= 400 {
      log.Fatalf("Failed to download file [HTTP %v].", resp.StatusCode)
    }

    zb, readErr := ioutil.ReadAll(resp.Body)
    if readErr != nil {
      log.Fatal("Failed to read downloaded file.")
    }

    zipBytes = zb

  } else { // read local file

    zb, readErr := ioutil.ReadFile(path)
    if readErr != nil {
      log.Fatal("Failed to read local file.")
    }

    zipBytes = zb
  }

  // create and return zip.Reader
  bytesReader := bytes.NewReader(zipBytes)
  z, zipErr := zip.NewReader(bytesReader, bytesReader.Size())
  if zipErr != nil {
    log.Fatal("Failed to parse zip file.")
  }

  return
}

/**
 * Initialize new sqlite (w/ spatialite ext) db.
 */
func newSqliteDB(name string, useSpatialite bool) (db *sql.DB) {
  sqlDriver := "sqlite3"

  if useSpatialite {

    // register sqlite driver, w/ spatialite ext
    sql.Register("spatialite",
      &sqlite3.SQLiteDriver{
        Extensions: []string{

          // note: needs to exist on system
          "libspatialite",
        },
      })

    sqlDriver = "spatialite"
  }

  // open new db connection
  db, dbErr := sql.Open(sqlDriver, name)
  if dbErr != nil {
    log.Fatal("Failed to create new sqlite db.")
  }

  // defer db.Close() // close later on (?)

  // confirm sqlite version
  var sqlVer string
  sqlErr := db.QueryRow("select sqlite_version();").Scan(&sqlVer)
  if sqlErr != nil {
    log.Fatal("Failed to call `sqlite_version()` on sqlite db.")
  }

  // output for debugging
  log.Printf("Successfully created: %s", name)
  log.Printf("SQLite Version: %v", sqlVer)

  return
}

/**
 * Import GTFS csv data into sqlite db.
 */
func importGTFS(db *sql.DB, gtfs *zip.Reader) {

  // begin directly importing each GTFS file (csv)
  for _, f := range gtfs.File {
    tablename := f.Name[:len(f.Name)-4] // trim ".txt"

    fr, fileErr := f.Open()
    if fileErr != nil {
      log.Fatal("Failed to open GTFS file for db importing.")
    }

    cr := csv.NewReader(fr)
    header, headerErr := cr.Read()
    if headerErr != nil {
      log.Fatalf("Failed to read GTFS csv for db importing [%s].", f.Name)
    }

    // create new table...
    _, createTableErr := db.Exec(fmt.Sprintf(
      "create table %s (%s text);",
      tablename,
      strings.Join(header, " text, "),
    ))
    if createTableErr != nil {
      log.Fatalf("Failed to create table `%s` in sqlite db.", tablename)
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
          log.Fatalf("Failed to read GTFS csv for db importing [%s].", f.Name)
        }

        // collect in sql-insert-ready format
        values[i] = fmt.Sprintf(`("%s")`, strings.Join(r, `","`))
      }

      // ... and insert into table
      _, insertTableErr := db.Exec(fmt.Sprintf(
        "insert into %s (%s) values %s;",
        tablename,
        strings.Join(header, ", "),
        strings.Join(values, ", "),
      ))
      if insertTableErr != nil {
        log.Fatalf("Failed to insert rows for `%s` in sqlite db.", tablename)
      }
    }
  }

  // output for debugging
  log.Print("Imported GTFS successfully.")
}


func main() {
  opts := getOptions()
  optDir := opts["dir"].(string)
  optName := opts["name"].(string)
  optSpat := opts["spatialite"].(bool)
  optExtras := opts["extras"].(bool)

  // grab GTFS zip file
  gtfs := getGTFS()

  // setup new sqlite db
  db := newSqliteDB(optName, optSpat)
  importGTFS(db, gtfs) // dump gtfs to db

  // build extra spatial tables
  if optSpat {
    buildSpatial(db, gtfs)
  }

  // extra export formats
  if optExtras {
    exportExtras(optDir, gtfs)
  }
}
