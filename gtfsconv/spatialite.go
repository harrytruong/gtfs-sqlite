package gtfsconv

import (
  "fmt"
  "database/sql"
  "github.com/mattn/go-sqlite3"
)

// buildSpatialite enables Spatialite SQLite extension,
// and creates additional spatial-enhanced tables.
func buildSpatialite(name string) (*sql.DB, error) {

  // ensure sqlite db exists
  if isExistFile(name) == false {
    return nil, fmt.Errorf("failed to find %s sqlite db", name)
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
    return nil, fmt.Errorf(
      "failed to open existing sqlite db with spatialite [%s]", dbErr)
  }
  if hasDBSpatialite(db) == false {
    return nil, fmt.Errorf("spatialite not loaded")
  }

  // check for spatialite metadata tables
  if hasDBTable(db, "geometry_columns") == false ||
     hasDBTable(db, "spatial_ref_sys") == false {

     // drop existing tables (for safety)
     _, dgcErr := db.Exec("drop table if exists geometry_columns;")
     _, dsrsErr := db.Exec("drop table if exists spatial_ref_sys;")
    if dgcErr != nil || dsrsErr != nil {
       return nil, fmt.Errorf(
         "failed to drop prev spatial metadata tables [%s, %s]",
         dgcErr, dsrsErr)
     }

     // initialize spatialite metadata
     if _, initErr := db.Exec("select InitSpatialMetaData();");
       initErr != nil {
       return nil, fmt.Errorf(
         "failed to call `InitSpatialMetaData()` [%s]", initErr)
     }
  }

  if stopsErr := buildSpatialStops(db); stopsErr != nil {
    return nil, fmt.Errorf("buildSpatialStops() %s", stopsErr)
  }

  if hasDBTable(db, "shapes") { // only build, if "shapes" table exists
    if shapesErr := buildSpatialShapes(db); shapesErr != nil {
      return nil, fmt.Errorf("buildSpatialShapes() %s", shapesErr)
    }
  }

  return db, nil
}

// buildSpatialShapes Helper: Build "shapes_geo" spatialite table.
// note: "shapes" table must exist in db!
func buildSpatialShapes(db *sql.DB) error {

  // count current number of shapes, for sanity checking,
  numShapes, nsErr := countDBTable(db, "distinct(shape_id)", "shapes")
  if nsErr != nil {
    return fmt.Errorf("countDBTable() %s", nsErr)
  }

  // if "shapes_geo" already exists
  if hasDBTable(db, "shapes_geo") {

    // count current number of shapes_geo
    numGeo, ngErr := countDBTable(db, "*", "shapes_geo")
    if ngErr != nil {
      return fmt.Errorf("countDBTable() %s", ngErr)
    }

    // if complete table, do nothing
    if numGeo == numShapes {
      return nil
    }

    // otherwise, drop for rebuilding
    if _, dgErr := db.Exec("drop table shapes_geo;"); dgErr != nil {
      return fmt.Errorf("failed to drop prev shapes_geo table [%s]", dgErr)
    }
  }

  // create new "shapes_geo" table
  if _, cErr := db.Exec("create table shapes_geo (shape_id text);");
    cErr != nil {
    return fmt.Errorf("failed to create table `shapes_geo` [%s]", cErr)
  }

  // add spatialite geometry column
  if _, gErr := db.Exec("select AddGeometryColumn" +
                      "('shapes_geo', 'geom', 4326, 'LINESTRING');");
    gErr != nil {
    return fmt.Errorf("failed to add column `shapes_geo.geom` [%s]", gErr)
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
    return fmt.Errorf("failed to insert rows into `shapes_geo` [%s]", iErr)
  }

  // count "shapes_geo" for final sanity check
  numGeo, ngErr := countDBTable(db, "*", "shapes_geo")
  if ngErr != nil {
    return fmt.Errorf("countDBTable() %s", ngErr)
  }

  // confirm proper number of rows
  if numGeo != numShapes {
    return fmt.Errorf(
      "failed to sanity check shapes_geo rows: %v expected, %v actual",
      numShapes, numGeo)
  }

  return nil
}

// buildSpatialStops Helper: Build "stops_geo" spatialite table.
func buildSpatialStops(db *sql.DB) error {

  // count current number of stops, for sanity checking,
  numStops, nsErr := countDBTable(db, "*", "stops")
  if nsErr != nil {
    return fmt.Errorf("countDBTable() %s", nsErr)
  }

  // if "stops_geo" already exists
  if hasDBTable(db, "stops_geo") {

    // count current number of shapes_geo
    numGeo, ngErr := countDBTable(db, "*", "stops_geo")
    if ngErr != nil {
      return fmt.Errorf("countDBTable() %s", ngErr)
    }

    // if complete table, do nothing
    if numGeo == numStops {
      return nil
    }

    // otherwise, drop for rebuilding
    if _, dgErr := db.Exec("drop table stops_geo;"); dgErr != nil {
      return fmt.Errorf("failed to drop prev stops_geo table [%s]", dgErr)
    }
  }

  // create new "stops_geo" table
  if _, cErr := db.Exec("create table stops_geo (stop_id text);");
    cErr != nil {
    return fmt.Errorf("failed to create table `stops_geo` [%s]", cErr)
  }

  // add spatialite geometry column
  if _, gErr := db.Exec("select AddGeometryColumn" +
                      "('stops_geo', 'geom', 4326, 'POINT');");
    gErr != nil {
    return fmt.Errorf("failed to add column `stops_geo.geom` [%s]", gErr)
  }

  // process each existing "stops.stop_id" into "stops_geo"
  if _, iErr := db.Exec("insert into stops_geo (stop_id, geom) " +
                      "select stop_id, geomfromtext(" +
                        "'POINT('||stop_lat||' '||stop_lon||')'" +
                      ", 4326) from stops;");
    iErr != nil {
    return fmt.Errorf("failed to insert rows into `stops_geo` [%s]", iErr)
  }

  // count "stops_geo" for final sanity check
  numGeo, ngErr := countDBTable(db, "*", "stops_geo")
  if ngErr != nil {
    return fmt.Errorf("countDBTable() %s", ngErr)
  }

  // confirm proper number of rows
  if numGeo != numStops {
    return fmt.Errorf(
      "failed to sanity check stops_geo rows: %v expected, %v actual",
      numStops, numGeo)
  }

  return nil
}
