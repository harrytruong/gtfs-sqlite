package main

import (
  "os"
  "fmt"
  "database/sql"
  "github.com/mattn/go-sqlite3"
)

/**
 * Build spatialite-enhanced sqlite tables.
 */
func buildSpatial(name string) error {

  // ensure sqlite db exists
  if _, err := os.Stat(name); os.IsNotExist(err) {
    return fmt.Errorf(
      "Failed to build spatialite table (sqlite db not found).")
  }

  // register sqlite driver, w/ spatialite ext
  sql.Register("spatialite",
    &sqlite3.SQLiteDriver{
      Extensions: []string{

        // note: needs to exist on system
        "libspatialite",
      },
    })

  // open new db connection
  db, dbErr := sql.Open("spatialite", name)
  if dbErr != nil {
    return fmt.Errorf(
      "Failed to open sqlite db with spatialite driver.")
  }
  defer db.Close()

  // confirm spatialite loaded
  var spatVer string
  if spatErr := db.QueryRow("select spatialite_version();").Scan(&spatVer);
    spatErr != nil {
    return fmt.Errorf(
      "Failed to call `spatialite_version()` on sqlite db.")
  }

  // initialize spatialite metadata
  if _, spatInitErr := db.Exec("select InitSpatialMetaData();");
    spatInitErr != nil {
    return fmt.Errorf(
      "Failed to call `InitSpatialMetaData()` on sqlite db.")
  }

  // output for debugging
  // log.Printf("Spatialite Version: %s", spatVer)
  // log.Print("Successfully initialized metadata.")

  if spatStopsErr := buildSpatialStops(db); spatStopsErr != nil {
    return fmt.Errorf(
      "Failed to build `stops_geo` table on sqlite db: %v", spatStopsErr)
  }

  var tblExist string
  if err := db.QueryRow( // check if "shapes" table exists
    "select name from sqlite_master "+
    "where type='table' AND name='shapes';").Scan(&tblExist); err == nil {
    if spatShapesErr := buildSpatialShapes(db); spatShapesErr != nil {
      return fmt.Errorf(
        "Failed to build `shapes_geo` table on sqlite db: %v", spatShapesErr)
    }
  }

  // log.Print("Successfully built spatialite tables.")

  return nil
}

/**
 * Helper: Build "shapes_geo" spatialite table.
 */
func buildSpatialShapes(db *sql.DB) error {

  // create new "shapes_geo" table
  if _, cErr := db.Exec("create table shapes_geo (shape_id text);");
    cErr != nil {
    return fmt.Errorf(
      "Failed to create table `shapes_geo` in sqlite db.")
  }

  // add spatialite geometry column
  if _, gErr := db.Exec("select AddGeometryColumn" +
                      "('shapes_geo', 'geom', 4326, 'LINESTRING');");
    gErr != nil {
    return fmt.Errorf(
      "Failed to add column `shapes_geo.geom` in sqlite db.")
  }

  // process each existing "shapes.shape_id" into "shapes_geo"
  if _, iErr := db.Exec("insert into shapes_geo " +
                     "select shape_id, geomfromtext(" +
                       "'LINESTRING(' || " +
                         "group_concat(shape_pt_lon || ' ' || shape_pt_lat) " +
                       " || ')', " +
                     "4326) as geom " +
                     "from shapes group by shape_id;");
    iErr != nil {
    return fmt.Errorf(
      "Failed to insert rows into `shapes_geo`.")
  }

  return nil
}

/**
 * Helper: Build "stops_geo" spatialite table.
 */
func buildSpatialStops(db *sql.DB) error {

  // create new "stops_geo" table
  if _, cErr := db.Exec("create table stops_geo (stop_id text);");
    cErr != nil {
    return fmt.Errorf(
      "Failed to create table `stops_geo` in sqlite db.")
  }

  // add spatialite geometry column
  if _, gErr := db.Exec("select AddGeometryColumn" +
                      "('stops_geo', 'geom', 4326, 'POINT');");
    gErr != nil {
    return fmt.Errorf(
      "Failed to add column `stops_geo.geom` in sqlite db.")
  }

  // process each existing "stops.stop_id" into "stops_geo"
  if _, iErr := db.Exec("insert into stops_geo (stop_id, geom) " +
                      "select stop_id, geomfromtext(" +
                        "'POINT('||stop_lat||' '||stop_lon||')'" +
                      ", 4326) from stops;");
    iErr != nil {
    return fmt.Errorf(
      "Failed to insert rows into `stops_geo`.")
  }

  return nil
}
