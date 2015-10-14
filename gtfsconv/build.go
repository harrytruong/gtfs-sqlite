// Package gtfsconv converts GTFS files into an SQLite3 db
// (w/ spatialite optional), and other extra file formats
// (csv/json/geojson/kml).
package gtfsconv

import (
  "os"
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

  // self-registers "sqlite3" with database/sql/driver
  _ "github.com/mattn/go-sqlite3"
)

// Options Type Helper: available runtime configs
type Options struct {
  GTFS        string  // path to GTFS source file
  Dir         string  // output dir
  Name        string  // output sqlite db name
  SkipExtras  bool    // skip extra output formats (*.csv, *.json, *.xml)
  Spatialite  bool    // include sqlite3 spatialite extension

  KeepDB      bool    // re-use existing sqlite db (skip creation), if exist
}

// Default options for Build
var defaultOptions = Options{
  GTFS:         "",
  Dir:          "gtfs-output/",
  Name:         "gtfs.sqlite",
  SkipExtras:   false,
  Spatialite:   false,

  KeepDB:       false,
}

// Limit for reading rows from GTFS csv file, during `importGTFS()`
// Note: Do not change this, unless you want to upset sqlite.
//       See "SQLITE_MAX_COMPOUND_SELECT" @ sqlite.org/limits.html
const csvReadRowsLimit = 500

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
func Build(opt Options) error {

  // prepare options for building
  if pErr := prepare(&opt); pErr != nil {
    return fmt.Errorf("prepare() %s", pErr)
  }

  // if keepdb, but sqlite file does not exist
  if opt.KeepDB && isExistFile(opt.Name) == false {
    opt.KeepDB = false // disable keepdb
  }

  // grab GTFS zip file
  gtfs, gtfsErr := getGTFS(opt.GTFS);
  if gtfsErr != nil {
    return fmt.Errorf("getGTFS() %s", gtfsErr)
  }

  // open new sqlite db connection
  db, sErr := sql.Open("sqlite3", opt.Name)
  if sErr != nil {
    return fmt.Errorf("sql.Open() %s", sErr)
  }
  defer db.Close() // close when everything is done

  // if not keeping existing db, import GTFS
  if opt.KeepDB == false {
    if iErr := importGTFS(db, gtfs); iErr != nil {
      return fmt.Errorf("importGTFS() %s", iErr)
    }
  }

  // if enabled, build extra spatialite tables
  if opt.Spatialite {
    spDb, spErr := buildSpatialite(opt.Name);
    if spErr != nil {
      return fmt.Errorf("buildSpatialite() %s", spErr)
    }

    db.Close() // immediately close old connection
    db = spDb // replace with spatialite-enhanced db connection
  }

  // if not skipped, export extra formats
  if opt.SkipExtras == false {

    // export csv based on "gtfs" directly
    if csvErr := exportCSV(opt.Dir, gtfs); csvErr != nil {
      return fmt.Errorf("exportCSV() %s", csvErr)
    }

    // export json based on sqlite db
    if jsonErr := exportJSON(opt.Dir, db); jsonErr != nil {
      return fmt.Errorf("exportJSON() %s", jsonErr)
    }

    // export geojson based sqlite db
    if geojsonErr := exportGeoJSON(opt.Dir, db);
      geojsonErr != nil {
      return fmt.Errorf("exportGeoJSON() %s", geojsonErr)
    }
  }

  return nil
}

// GoBuild will run Build with default options.
func GoBuild(gtfsFile string) error {
  options := DefaultOptions()
  options.GTFS = gtfsFile
  return Build(options)
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

  // check existing sqlite db conflict
  if opt.KeepDB == false && isExistFile(opt.Name) {
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

// importGTFS creates tables based on GTFS data.
func importGTFS(db *sql.DB, gtfs *zip.Reader) error {

  // begin directly importing each GTFS file (csv)
  for _, f := range gtfs.File {
    if valid, _ := isGTFS(f.Name); valid == false {
      continue // skip non-GTFS standard files
    }

    tablename := f.Name[:len(f.Name)-4] // trim ".txt"

    fr, oErr := f.Open()
    if oErr != nil {
      return fmt.Errorf("failed to open %s file [%s]", f.Name, oErr)
    }

    cr := csv.NewReader(fr)
    header, hErr := cr.Read()
    if hErr != nil {
      return fmt.Errorf("failed to read %s file [%s]", f.Name, hErr)
    }

    // create new table with headers...
    if _, ctErr := db.Exec(fmt.Sprintf(
      "create table %s (%s text);", tablename,
      strings.Join(header, " text, "))); ctErr != nil {
      return fmt.Errorf("failed to create table %s [%s]", tablename, ctErr)
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
          return fmt.Errorf("failed to read %s file [%s]", f.Name, crErr)
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
      if _, itErr := db.Exec(fmt.Sprintf(
        "insert into %s (%s) values %s;",
        tablename,
        strings.Join(header, ", "),
        strings.Join(values, ", ")));
        itErr != nil {
        return fmt.Errorf("failed to insert %s [%s]", tablename, itErr)
      }
    }
  }

  return nil
}
