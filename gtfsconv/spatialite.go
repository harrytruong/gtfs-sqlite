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
     if _, initErr := db.Exec(

       // drop existing meta tables (for safety)
       "drop table if exists geometry_columns;" +
       "drop table if exists spatial_ref_sys;" +

       // initialize spatialite metadata
       "select InitSpatialMetaData();");
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

    // if routesErr := buildSpatialRoutes(db); routesErr != nil {
    //   return nil, fmt.Errorf("buildSpatialRoutes() %s", routesErr)
    // }
  }

  return db, nil
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
    switch {
      case ngErr != nil: return fmt.Errorf("countDBTable() %s", ngErr)
      case numGeo == numStops: return nil // if complete table, do nothin
    }

    // otherwise, drop for rebuilding
    if _, dgErr := db.Exec(
      "select DiscardGeometryColumn('stops_geo', 'geom'); " +
      "drop table stops_geo;"); dgErr != nil {
      return fmt.Errorf("failed to drop prev stops_geo table [%s]", dgErr)
    }
  }

  // create new "stops_geo" table
  // with spatialite geometry column
  if _, cErr := db.Exec(
    "create table stops_geo (stop_id text);" +
    "select AddGeometryColumn('stops_geo', 'geom', 4326, 'POINT');");
    cErr != nil {
    return fmt.Errorf("failed to create table `stops_geo` [%s]", cErr)
  }

  // process each existing "stops.stop_id" into "stops_geo"
  if _, iErr := db.Exec("insert into stops_geo (stop_id, geom) " +
                      "select stop_id, geomfromtext(" +
                        "'POINT('||stop_lon||' '||stop_lat||')'" +
                      ", 4326) from stops;");
    iErr != nil {
    return fmt.Errorf("failed to insert rows into `stops_geo` [%s]", iErr)
  }

  // count "stops_geo" for final sanity check
  numGeo, ngErr := countDBTable(db, "*", "stops_geo")
  switch {
    case ngErr != nil: return fmt.Errorf("countDBTable() %s", ngErr)
    case numGeo != numStops: return fmt.Errorf(
      "failed to sanity check stops_geo rows: %v expected, %v actual",
      numStops, numGeo)
  }

  return nil
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
    switch {
      case ngErr != nil: return fmt.Errorf("countDBTable() %s", ngErr)
      case numGeo == numShapes: return nil // if complete table, do nothing
    }

    // otherwise, drop for rebuilding
    if _, dgErr := db.Exec(
      "select DiscardGeometryColumn('shapes_geo', 'geom'); " +
      "drop table shapes_geo;"); dgErr != nil {
      return fmt.Errorf("failed to drop prev shapes_geo table [%s]", dgErr)
    }
  }

  // create new "shapes_geo" table
  // with spatialite geometry column
  if _, cErr := db.Exec(
    "create table shapes_geo (shape_id text);" +
    "select AddGeometryColumn('shapes_geo', 'geom', 4326, 'LINESTRING');");
    cErr != nil {
    return fmt.Errorf("failed to create table `shapes_geo` [%s]", cErr)
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
  switch {
    case ngErr != nil: return fmt.Errorf("countDBTable() %s", ngErr)
    case numGeo != numStops: return fmt.Errorf(
      "failed to sanity check shapes_geo rows: %v expected, %v actual",
      numShapes, numGeo)
  }

  return nil
}

// buildSpatialRoutes Helper: Build "routes_geo" spatialite table.
// note: "shapes" table must exist in db!
func buildSpatialRoutes(db *sql.DB) error {

  // count current number of routes, for sanity checking,
  numRoutes, nsErr := countDBTable(db, "distinct(route_id)", "routes")
  if nsErr != nil {
    return fmt.Errorf("countDBTable() %s", nsErr)
  }

  // if "routes_geo" already exists
  if hasDBTable(db, "routes_geo") {

    // count current number of routes_geo
    numGeo, ngErr := countDBTable(db, "*", "routes_geo")
    switch {
      case ngErr != nil: return fmt.Errorf("countDBTable() %s", ngErr)
      case numGeo == numRoutes: return nil // if complete table, do nothing
    }

    // otherwise, drop for rebuilding
    if _, dgErr := db.Exec(
      "select DiscardGeometryColumn('routes_geo', 'geom'); " +
      "drop table routes_geo;"); dgErr != nil {
      return fmt.Errorf("failed to drop prev routes_geo table [%s]", dgErr)
    }
  }

  // create new "routes_geo" table
  // with spatialite geometry column
  if _, cErr := db.Exec(
    "create table routes_geo (route_id text);" +
    "select AddGeometryColumn('routes_geo', 'geom', 4326, 'MULTILINESTRING');");
    cErr != nil {
    return fmt.Errorf("failed to create table `routes_geo` [%s]", cErr)
  }

  // st_union all "shapes_geo.geom" into a single multilinestringline,
  // then st_linemerge all segments into minimal multilinestring,
  // then st_union all "stops_geo.geom" into a single multipoint,
  // then st_linescutatnodes multilinestring segments (shapes)
  //      against multipoints (stops)
  // and save/insert the final result

  // count "routes_geo" for final sanity check
  numGeo, ngErr := countDBTable(db, "*", "routes_geo")
  switch {
    case ngErr != nil: return fmt.Errorf("countDBTable() %s", ngErr)
    case numGeo != numRoutes: return fmt.Errorf(
        "failed to sanity check routes_geo rows: %v expected, %v actual",
        numRoutes, numGeo)
  }

  return nil
}
