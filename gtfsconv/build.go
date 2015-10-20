// Package gtfsconv converts GTFS files into an SQLite3 db
// (w/ spatialite optional), and other extra file formats
// (csv/json/geojson/kml).
package gtfsconv

import (
  "fmt"
  "log"
  "os"
  "strings"
  "regexp"
  "net/http"
  "archive/zip"
  "encoding/csv"
  "bytes"
  "io"
  "io/ioutil"
  "unicode/utf8"

  "database/sql"
  "github.com/mattn/go-sqlite3"
)

// Options Type Helper: available runtime configs
type Options struct {
  GTFS        string  // path to GTFS source file
  Dir         string  // output dir
  Name        string  // output sqlite db name
  SkipExtras  bool    // skip extra output formats (*.csv, *.json, *.xml)
  Spatialite  bool    // include sqlite3 spatialite extension

  KeepDB      bool    // re-use existing sqlite db (skip creation), if exist
  SkipClean   bool    // skip agency-specific GTFS cleanup rules
}

// Default options for Build
var defaultOptions = Options{
  GTFS:         "",
  Dir:          "gtfs-output/",
  Name:         "gtfs.sqlite",
  SkipExtras:   false,
  Spatialite:   false,

  KeepDB:       false,
  SkipClean:    false,
}

// csvReadRowsLimit controls reading GTFS csv during `importGTFS()`
// Note: Do not change this, unless you want to upset sqlite.
//       See "SQLITE_MAX_COMPOUND_SELECT" @ sqlite.org/limits.html
const csvReadRowsLimit = 500

// sqliteGTFSDriverRegistered flag runs sql.Register() only once
var sqliteGTFSDriverRegistered = false

// sqliteGTFSConns collects all the sqlite3_gtfs connections
var sqliteGTFSConns = []*sqlite3.SQLiteConn{}

// DefaultOptions returns Options with default values.
func DefaultOptions() Options {
  return defaultOptions
}

// SetDefaultOptions sets defaultOptions with new default values.
func SetDefaultOptions(opt Options) {
  defaultOptions = opt
}

// Build will consume the GTFS file and export a sqlite3 db,
// in the target dir/filename, along with extra file formats.
func Build(opt Options, logger *log.Logger) error {

  // prepare options for building
  logger.Println("Preparing options...")
  if pErr := prepare(&opt); pErr != nil {
    return fmt.Errorf("prepare() %s", pErr)
  }

  // grab GTFS zip file
  logger.Println("Grabbing GTFS...")
  gtfs, gtfsErr := getGTFS(opt.GTFS);
  if gtfsErr != nil {
    return fmt.Errorf("getGTFS() %s", gtfsErr)
  }

  // setup sqlite db (create new, or keep existing)
  logger.Println("Setting up Sqlite DB...")
  db, dbErr := setupDB(opt, func(db *sql.DB) error {

    // import GTFS data
    logger.Println("Importing GTFS...")
    if iErr := importGTFS(db, gtfs); iErr != nil {
      return fmt.Errorf("importGTFS() %s", iErr)
    }

    // if not skipping, clean GTFS data
    if opt.SkipClean == false {
      logger.Println("Cleaning GTFS...")
      if cErr := cleanGTFS(db); cErr != nil {
        return fmt.Errorf("cleanGTFS() %s", cErr)
      }
    }

    // if enabled, build extra spatialite tables
    if opt.Spatialite {
      logger.Println("Building Spatialite...")
      if spErr := buildSpatialite(db); spErr != nil {
        return fmt.Errorf("buildSpatialite() %s", spErr)
      }
    }

    return nil
  })
  if dbErr != nil {
    return fmt.Errorf("setupDB() %s", dbErr)
  }
  defer db.Close()

  // if not skipped, export extra formats
  if opt.SkipExtras == false {

    // export csv based on "gtfs" directly
    logger.Println("Exporting CSV...")
    if csvErr := exportCSV(opt.Dir, gtfs); csvErr != nil {
      return fmt.Errorf("exportCSV() %s", csvErr)
    }

    // export json based on sqlite db
    logger.Println("Exporting JSON...")
    if jsonErr := exportJSON(opt.Dir, db); jsonErr != nil {
      return fmt.Errorf("exportJSON() %s", jsonErr)
    }

    // export geojson based sqlite db
    logger.Println("Exporting GeoJSON...")
    if geojsonErr := exportGeoJSON(opt.Dir, db);
      geojsonErr != nil {
      return fmt.Errorf("exportGeoJSON() %s", geojsonErr)
    }
  }

  logger.Println("Finished.")
  return nil
}

// GoBuild will run Build, with an dummy logger.
func GoBuild(opt Options) error {

  // setup dummy logger
  null, _ := os.Open(os.DevNull)
  logger := log.New(null, "", 0)

  return Build(opt, logger)
}

// prepare: reviews options for build.
func prepare(opt *Options) error {

  opt.Dir = strings.Trim(opt.Dir, "/")+"/"  // ensure dir trailing slash
  opt.Name = opt.Dir+opt.Name // ensure db within dir

  // ensure GTFS path is set
  if opt.GTFS == "" {
    return fmt.Errorf("missing gtfsFile (URL or path/to/gtfs.zip)")
  }

  // ensure dir exists
  if mkdirErr := os.MkdirAll(opt.Dir, 0777); mkdirErr != nil {
    return fmt.Errorf("could not create dir [%s]", mkdirErr)
  }

  // check for existing db file
  existDB := isExistFile(opt.Name)

  // if keepdb, but sqlite file does not exist
  if existDB == false && opt.KeepDB {
    opt.KeepDB = false // disable keepdb
  }

  // check existing sqlite db conflict
  if existDB && opt.KeepDB == false {
    return fmt.Errorf("sqlite db already exists [%s]", opt.Name)
  }

  return nil
}

// getGTFS retrieves GTFS zip file from URL or local path.
func getGTFS(path string) (*zip.Reader, error) {
  var zipBytes []byte

  // determine type of path:
  if regexp.MustCompile("^https?://").Match([]byte(path)) {

    // download and read remote file
    resp, httpErr := http.Get(path)
    if httpErr != nil {
      return nil, fmt.Errorf(
        "failed to download file [%s]", httpErr)
    }

    if resp.StatusCode >= 400 {
      return nil, fmt.Errorf(
        "failed to download file [HTTP %v]", resp.StatusCode)
    }

    zb, readErr := ioutil.ReadAll(resp.Body)
    if readErr != nil {
      return nil, fmt.Errorf(
        "failed to read downloaded file [%s]", readErr)
    }

    zipBytes = zb
  } else {

    // read from local file
    zb, readErr := ioutil.ReadFile(path)
    if readErr != nil {
      return nil, fmt.Errorf(
        "failed to read local file [%s]", readErr)
    }

    zipBytes = zb
  }

  // create and return zip.Reader
  bytesReader := bytes.NewReader(zipBytes)
  zipReader, zipErr := zip.NewReader(bytesReader, bytesReader.Size())
  if zipErr != nil {
    return nil, fmt.Errorf(
      "failed to parse zip file [%s]", zipErr)
  }

  return zipReader, nil
}

// setupDB prepares a new sqlitedb (or re-uses an existing db),
// runs a callback setupFn(), and then optimizes the final db.
// note: remember to call db.Close() when finished!
func setupDB(opt Options, setupFn func(*sql.DB)error) (*sql.DB, error) {
  dbexts := []string{}

  if opt.Spatialite { // add spatialite extension, if enabled
    dbexts = append(dbexts, "libspatialite") // must exist on host system!
  }

  // register a custom sqlite3 driver,
  // with additional extensions, and
  // hook to access all connections
  if sqliteGTFSDriverRegistered == false {
    sql.Register("sqlite3_gtfs",
      &sqlite3.SQLiteDriver{
        Extensions: dbexts,
        ConnectHook: func(conn *sqlite3.SQLiteConn) error {
          sqliteGTFSConns = append(sqliteGTFSConns, conn)
          return nil
        },
      })

    sqliteGTFSDriverRegistered = true
  }

  // set default db target to ":memory:"
  target := ":memory:"

  // if we're keeping the db,
  // use that existing db file instead
  if opt.KeepDB {
    target = opt.Name
  }

  // open db connection
  db, oErr := sql.Open("sqlite3_gtfs", target)
  if oErr != nil {
    return nil, fmt.Errorf("sql.Open() %s", oErr)
  }
  db.Ping() // actually makes connection

  // reference to the current db connection
  dbConn := sqliteGTFSConns[len(sqliteGTFSConns)-1]

  // run setup callback function to setup db
  if fnErr := setupFn(db); fnErr != nil {
    return nil, fmt.Errorf("setupFn() %s", fnErr)
  }

  // todo: run optimizations on db

  // if we're not keeping the existing db,
  // then we need to start saving the memory db into file
  if opt.KeepDB == false {

    // open a new connection to the destination file
    fileDB, foErr := sql.Open("sqlite3_gtfs", opt.Name)
    if foErr != nil {
      return nil, fmt.Errorf("sql.Open() %s", foErr)
    }
    fileDB.Ping() // actually make connection

    // reference to the new file db connection
    fileDBConn := sqliteGTFSConns[len(sqliteGTFSConns)-1]

    // proceed with backup
  	backup, bErr := fileDBConn.Backup("main", dbConn, "main")
  	if bErr != nil {
  		return nil, fmt.Errorf("dbconn.Backup() %s", bErr)
  	}

    // step into backup ("-1" indicates run all steps)
  	if _, bsErr := backup.Step(-1); bsErr != nil {
      return nil, fmt.Errorf("backup.Step() %s", bsErr)
    }

    backup.Finish() // wrap up backup process
    fileDB.Close() // ensure file DB conn is closed
  }

  // finished setting up db
  return db, nil
}

// importGTFS creates tables based on GTFS data.
func importGTFS(db *sql.DB, gtfs *zip.Reader) error {

  // ensure gtfs_metadata table
  if hasDBTable(db, "gtfs_metadata") == false {
    if _, cmErr := db.Exec("create table gtfs_metadata " +
      "(tablename text, imported_at text, cleaned text);");
      cmErr != nil {
      return fmt.Errorf("failed to create gtfs_metadata table [%s]", cmErr)
    }
  }

  // begin directly importing each GTFS file (csv)
  for _, f := range gtfs.File {
    if valid, _ := isGTFS(f.Name); valid == false {
      continue // skip non-GTFS standard files
    }

    tablename := f.Name[:len(f.Name)-4] // trim ".txt"

    // check if this table already successfully imported
    ims, imsErr := countDBTable(db, "*",
      "gtfs_metadata where tablename = '" + tablename + "'")
    switch {
      case imsErr != nil: return fmt.Errorf("countDBTable() %s", imsErr)
      case ims > 0: continue // skip importing file
    }

    fr, oErr := f.Open()
    if oErr != nil {
      return fmt.Errorf("failed to open %s file [%s]", f.Name, oErr)
    }

    cr := csv.NewReader(fr)
    header, hErr := cr.Read()
    if hErr != nil {
      return fmt.Errorf("failed to read %s file [%s]", f.Name, hErr)
    }

    // trim whitespace from each header
    for i, v := range header {
      if i == 0 {  // trim utf8 bom!
        v = strings.Trim(v, string([]byte{239, 187, 191}))
      }
      header[i] = strings.TrimSpace(v)
    }

    // prepare create table statement
    ctStmt := fmt.Sprintf(
      "drop table if exists %s; " +
      "create table %s (%s text);", tablename,
      tablename, strings.Join(header, " text, "))

    // ensure valid utf8
    if utf8.ValidString(ctStmt) == false {
      return fmt.Errorf("invalid utf8 in header")
    }

    // create new table with headers...
    if _, ctErr := db.Exec(ctStmt); ctErr != nil {
      return fmt.Errorf("failed create table %s [%s]", tablename, ctErr)
    }

    cr.FieldsPerRecord = -1 // disable too few fields error
    cr.TrimLeadingSpace = true // cleanup whitespace
    cr.LazyQuotes = true // allow weirdly placed (unescaped) quotes

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
          return fmt.Errorf("failed to read %s file [%s]", f.Name, crErr)
        }

        // ensure proper number of fields
        row := make([]string, len(header))
        copy(row, r)

        // trim whitespace from each value
        for i, v := range row {
          row[i] = strings.TrimSpace(v)
        }

        // collect in sql-insert-ready format
        values[i] = fmt.Sprintf(`(#!%s#!)`, strings.Join(row, `#!,#!`))

        // ensure valid utf8
        if utf8.ValidString(values[i]) == false {
          return fmt.Errorf("encountered invalid utf8 in row [%s]", values[i])
        }
      }

      // extra case handler for empty rows
      if len(values) == 0 {
        isEOF = true
        continue // exit writing
      }

      // ... and insert into table
      cleanValuesStr := strings.Join(values, ", ")
      cleanValuesStr = strings.Replace(cleanValuesStr, `'`, `''`, -1)
      cleanValuesStr = strings.Replace(cleanValuesStr, `#!`, `'`, -1)
      if _, itErr := db.Exec(fmt.Sprintf(
        "insert into %s (%s) values %s;",
        tablename,
        strings.Join(header, ", "),
        cleanValuesStr));
        itErr != nil {
        return fmt.Errorf("failed to insert %s [%s]", tablename, itErr)
      }
    }

    // add indexes to table
    var iStmt string
    switch tablename {
      case "routes":
        iStmt = "create unique index route_idx on routes (route_id);"
      case "shapes":
        iStmt = "create index shape_idx on shapes (shape_id);"
      case "stop_times":
        iStmt = `create index st_trip_idx on stop_times (trip_id);
                 create index st_stop_idx on stop_times (stop_id);
                 create index stop_times_idx on stop_times (trip_id,stop_id);`
      case "stops":
        iStmt = "create unique index stop_idx on stops (stop_id);"
      case "transfers":
        iStmt = `create index trans_from_idx on transfers (from_stop_id);
                 create index trans_to_idx on transfers (to_stop_id);
                 create index trans_idx on transfers (from_stop_id,to_stop_id);`
      case "trips":
        iStmt = `create unique index trip_idx on trips (trip_id);
                 create index t_shape_idx on trips (shape_id);
                 create index route_dir_idx on trips (route_id,direction_id);`
    }

    if iStmt != "" {
      if _, ciErr := db.Exec(iStmt); ciErr != nil {
        return fmt.Errorf("failed add index(es) to %s [%s]", tablename, ciErr)
      }
    }

    // indicate gtfs import success for this table
    if _, imErr := db.Exec(
      "insert into gtfs_metadata (tablename, imported_at) " +
      "values (?, datetime('now'));", tablename); imErr != nil {
      return fmt.Errorf("failed to note successful import [%s]", imErr)
    }
  }

  return nil
}
