package main

import (
  "log"
  "os"
  "database/sql"
  "io/ioutil"
  "archive/zip"
)

/**
 * Build spatialite-enhanced sqlite tables.
 */
func buildSpatial(db *sql.DB, gtfs *zip.Reader) {

  // confirm spatialite loaded
  var spatVer string
  spatErr := db.QueryRow("select spatialite_version();").Scan(&spatVer)
  if spatErr != nil {
    log.Fatal("Failed to call `spatialite_version()` on sqlite db.")
  }

  // initialize spatialite metadata
  _, spatInitErr := db.Exec("select InitSpatialMetaData();")
  if spatInitErr != nil {
    log.Fatal("Failed to call `InitSpatialMetaData()` on sqlite db.")
  }

  // output for debugging
  log.Printf("Spatialite Version: %s", spatVer)
  log.Print("Successfully initialized metadata.")

  buildSpatialStops(db)
  buildSpatialShapes(db)

  log.Print("Successfully built spatial ('_geo') tables.")

  return
}

/**
 * Helper: Build "shapes_geo" spatialite table.
 */
func buildSpatialShapes(db *sql.DB) {
  var err error

  // create new "shapes_geo" table
  _, err = db.Exec("create table shapes_geo (shape_id text);")
  if err != nil {
    log.Fatal("Failed to create table `shapes_geo` in sqlite db.")
  }

  // add spatialite geometry column
  _, err = db.Exec("select AddGeometryColumn" +
                   "('shapes_geo', 'geom', 4326, 'LINESTRING');")
  if err != nil {
    log.Fatal("Failed to add column `shapes_geo.geom` in sqlite db.")
  }

  // process each existing "shapes.shape_id" into "shapes_geo"
  _, err = db.Exec("insert into shapes_geo " +
                   "select shape_id, geomfromtext(" +
                     "'LINESTRING(' || " +
                       "group_concat(shape_pt_lon || ' ' || shape_pt_lat) " +
                     " || ')', " +
                   "4326) as geom " +
                   "from shapes group by shape_id;")
  if err != nil {
    log.Fatal("Failed to insert rows into `shapes_geo`.")
  }
}

/**
 * Helper: Build "stops_geo" spatialite table.
 */
func buildSpatialStops(db *sql.DB) {
  var err error

  // create new "stops_geo" table
  _, err = db.Exec("create table stops_geo (stop_id text);")
  if err != nil {
    log.Fatal("Failed to create table `stops_geo` in sqlite db.")
  }

  // add spatialite geometry column
  _, err = db.Exec("select AddGeometryColumn" +
                    "('stops_geo', 'geom', 4326, 'POINT');")
  if err != nil {
    log.Fatal("Failed to add column `stops_geo.geom` in sqlite db.")
  }

  // process each existing "stops.stop_id" into "stops_geo"
  _, err = db.Exec("insert into stops_geo (stop_id, geom) " +
                    "select stop_id, geomfromtext(" +
                      "'POINT('||stop_lat||' '||stop_lon||')'" +
                    ", 4326) from stops;")
  if err != nil {
    log.Fatal("Failed to insert rows into `stops_geo`.")
  }
}


/**
 * Export extra file formats (*.csv, *.json, *.xml).
 */
func exportExtras(dir string, gtfs *zip.Reader) {
  data := make(map[string][]byte)
  for _, f := range gtfs.File {

    fr, fileErr := f.Open()
    if fileErr != nil {
      log.Fatal("Failed to open GTFS file for exporting.")
    }

    fb, readErr := ioutil.ReadAll(fr)
    if readErr != nil {
      log.Fatal("Failed to read GTFS file for exporting.")
    }

    data[f.Name] = fb
  }

  exportCSV(dir, data)
  // exportJSON(dir, data)

  return
}


/**
 * Basic CSV export of GTFS files.
 */
func exportCSV(dir string, data map[string][]byte) {
  dir = dir+"csv/" // export to "csv" subdir
  os.MkdirAll(dir, 0777) // ensure exists

  for name, fb := range data {
    ioutil.WriteFile(dir+name, fb, 0444)
  }

  return
}

/**
 * Basic JSON export of GTFS files.
 *
func exportJSON(dir string, data map[string][]byte) {
  dir = dir+"json/" // export to "json" subdir
  os.MkdirAll(dir, 0777) // ensure exists


	// setup parsers for each file
	parseFn := map[string]func(csvFile) jsony{
		"stops.txt":  parseStops,
		"shapes.txt": parseShapes,
	}

	for k, v := range  {
		data[k] = v // collect parsed data
	}
  for _, f := range gtfs.File {

    fr, fileErr := f.Open()
    if fileErr != nil {
      log.Fatal("Failed to open GTFS file for exporting.")
    }

    // todo: improve below to stream/pipe directly into file

    fb, readErr := ioutil.ReadAll(fr)
    if readErr != nil {
      log.Fatal("Failed to read GTFS file for exporting.")
    }

    ioutil.WriteFile(dir+f.Name, fb, 0444)
  }

  return
}
*/
