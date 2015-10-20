package gtfsconv

import (
  "fmt"
  "database/sql"
)

// buildSpatialite enables Spatialite SQLite extension,
// and creates additional spatial-enhanced tables.
func buildSpatialite(db *sql.DB) error {

  // sanity check that spatialite is loaded
  if hasDBSpatialite(db) == false {
    return fmt.Errorf("spatialite is not loaded")
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
       return fmt.Errorf(
         "failed to call `InitSpatialMetaData()` [%s]", initErr)
     }
  }

  if stopsErr := buildSpatialStops(db); stopsErr != nil {
    return fmt.Errorf("buildSpatialStops() %s", stopsErr)
  }

  if hasDBTable(db, "shapes") { // only build, if "shapes" table exists
    if shapesErr := buildSpatialShapes(db); shapesErr != nil {
      return fmt.Errorf("buildSpatialShapes() %s", shapesErr)
    }

    if routesErr := buildSpatialRoutes(db); routesErr != nil {
      return fmt.Errorf("buildSpatialRoutes() %s", routesErr)
    }
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

  // add unique index
  if _, ciErr := db.Exec(
    "create unique index stopgeo_idx on stops_geo (stop_id);"); ciErr != nil {
    return fmt.Errorf("failed add index(es) to stops_geo [%s]", ciErr)
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

  // add unique index
  if _, ciErr := db.Exec(
    "create unique index shapegeo_idx on shapes_geo (shape_id);");
    ciErr != nil {
    return fmt.Errorf("failed add index(es) to shapes_geo [%s]", ciErr)
  }

  // count "shapes_geo" for final sanity check
  numGeo, ngErr := countDBTable(db, "*", "shapes_geo")
  switch {
    case ngErr != nil: return fmt.Errorf("countDBTable() %s", ngErr)
    case numGeo != numShapes: return fmt.Errorf(
      "failed to sanity check shapes_geo rows: %v expected, %v actual",
      numShapes, numGeo)
  }

  return nil
}

// buildSpatialRoutes Helper: Build "routes_geo" spatialite table.
// note: "shapes" table must exist in db!
func buildSpatialRoutes(db *sql.DB) error {

  // count current number of routes, for sanity checking,
  numRoutes, nsErr := countDBTable(db,
    "distinct(route_id||direction_id)", "trips")
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
    if _, dgErr := db.Exec(`
      select DiscardGeometryColumn('routes_geo', 'geom'),
        DiscardGeometryColumn('routes_geo', 'stopgeom'),
        DiscardGeometryColumn('routes_geo', 'pathgeom');
      drop table routes_geo;`); dgErr != nil {
      return fmt.Errorf("failed to drop prev routes_geo table [%s]", dgErr)
    }
  }

  // create new "routes_geo" table
  // with spatialite geometry column
  if _, cErr := db.Exec(`
  create table routes_geo (route_id text, direction_id text);
  select AddGeometryColumn('routes_geo', 'geom', 4326, 'MULTILINESTRING'),
    AddGeometryColumn('routes_geo', 'stopgeom', 4326, 'MULTIPOINT'),
    AddGeometryColumn('routes_geo', 'pathgeom', 4326, 'MULTILINESTRING');`);
    cErr != nil {
    return fmt.Errorf("failed to create table `routes_geo` [%s]", cErr)
  }

  // select all distinct route:direction
  rts, rErr := db.Query("select distinct route_id, direction_id from trips;")
  if rErr != nil {
    return fmt.Errorf("failed to query distinct trips [%s]", rErr)
  }

  var routes [][2]string
  var rid, did string
  for rts.Next() {
    if sErr := rts.Scan(&rid, &did); sErr != nil {
      return fmt.Errorf("failed to scan routes [%s]", sErr)
    }
    routes = append(routes, [2]string{rid, did})
  }
  rts.Close()

  for _, rt := range routes {
    rid = rt[0]
    did = rt[1]

    // generate and insert new routes_geo rows
    if _, irErr := db.Exec(fmt.Sprintf(`
    insert into routes_geo
      (route_id, direction_id, geom, stopgeom, pathgeom)

    select
      '%s', '%s',
      castToMulti(SHAPES.geom),
      castToMulti(STOPS.geom),
      castToMulti(st_linescutatnodes(
        st_linemerge(snap(SHAPES.geom, STOPS.geom, 0.0005)), STOPS.geom))

    from
      (select linemerge(st_union(ts.geom)) as geom from
        (select distinct(t.shape_id), sg.geom as geom
        from trips t left join shapes_geo sg on t.shape_id = sg.shape_id
        where t.route_id = '%s' and t.direction_id = '%s') ts) SHAPES

      left join
      (select st_union(ts.geom) as geom from
        (select distinct(sg.stop_id), sg.geom as geom
        from trips t
          left join stop_times st on t.trip_id = st.trip_id
          left join stops_geo sg on st.stop_id = sg.stop_id
        where t.route_id = '%s' and t.direction_id = '%s') ts) STOPS
      on 1=1;`, rid, did, rid, did, rid, did)); irErr != nil {
      return fmt.Errorf("failed to generate new row [%s]", irErr)
    }
  }

  // add unique index
  if _, ciErr := db.Exec(`
    create index routegeo_idx on routes_geo (route_id);
    create unique index routegeo_dir_idx on routes_geo (route_id, direction_id);`); ciErr != nil {
    return fmt.Errorf("failed add index(es) to routes_geo [%s]", ciErr)
  }

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
