package gtfsconv

import (
  "os"
  "fmt"
  "database/sql"
  "io/ioutil"
)

// exportGeoJSON creates geojson files from sqlite db.
func exportGeoJSON(dir string, db *sql.DB) error {
  dir += "geojson/" // export to "geojson" subdir
  stopsDir := dir+"stops/"
  shapesDir := dir+"shapes/"
  routesDir := dir+"routes/"
  transfersDir := dir+"transfers/"

  // ensure dir (and extra subdirs) exists
  for _, d := range [...]string{
    dir, shapesDir, stopsDir, transfersDir, routesDir} {
    if mkdirErr := os.MkdirAll(d, 0777); mkdirErr != nil {
      return mkdirErr
    }
  }

  if stopsErr := exportGeoJSONStops(stopsDir, db); stopsErr != nil {
    return fmt.Errorf("exportGeoJSONStops() %s", stopsErr)
  }

  if hasDBTable(db, "shapes") { // only export, if "shapes" table exists
    if shapesErr := exportGeoJSONShapes(shapesDir, db); shapesErr != nil {
      return fmt.Errorf("exportGeoJSONShapes() %s", shapesErr)
    }

    // only export if spatialite is enabled, and "routes_geo" table exists
    if hasDBSpatialite(db) && hasDBTable(db, "routes_geo") {
      if routesErr := exportGeoJSONRoutes(routesDir, db); routesErr != nil {
        return fmt.Errorf("exportGeoJSONRoutes() %s", routesErr)
      }
    }
  }

  if hasDBTable(db, "transfers") { // only export, if table exists
    if transErr := exportGeoJSONTransfers(transfersDir, db); transErr != nil {
      return fmt.Errorf("exportGeoJSONTransfers() %s", transErr)
    }
  }

  return nil
}

// exportGeoJSONShapes Helper: Export GeoJSON for "shapes" table.
func exportGeoJSONShapes(dir string, db *sql.DB) error {

  // retrieve all unique shapes
  shapes, shapesErr := db.Query(
    "select distinct(shape_id) as id from shapes;")
  if shapesErr != nil {
    return fmt.Errorf("failed to select `distinct(shape_id)` [%s]", shapesErr)
  }
  defer shapes.Close()

  var id string // placeholder for "shape_id" col
  var features []jsony
  for shapes.Next() {
    if scanErr := shapes.Scan(&id); scanErr != nil {
      return fmt.Errorf("failed to scan shapes query [%s]", scanErr)
    }

    // retrive all points for this shape
    var shapeLine [][2]float64
    var lat, lng float64 // placeholder for "lat", "lon" col
    points, ptErr := db.Query(
      "select shape_pt_lat, shape_pt_lon from shapes " +
      "where shape_id = ? order by cast(shape_pt_sequence as int) asc;", id)
    if ptErr != nil {
      return fmt.Errorf("failed to select shape points [%s]", ptErr)
    }
    defer points.Close()
    for points.Next() {
      if ptscanErr := points.Scan(&lat, &lng); ptscanErr != nil {
        return fmt.Errorf("failed to scan shapes points query [%s]", ptscanErr)
      }
      shapeLine = append(shapeLine, [2]float64{lng, lat})
    }

    // create final geojson "Feature"
    feature := jsony{
      "type": "Feature",
      "properties": jsony{
        "shape_id": id,
      },
      "geometry": jsony{
        "type": "LineString",
        "coordinates": shapeLine,
      },
    }

    // write geojson "Feature"
    if wjErr := writeJSON(dir+"shape."+id+".geojson", feature);
      wjErr != nil {
      return fmt.Errorf("writeJSON() %s", wjErr)
    }

    // and append for later featureCol
    features = append(features, feature)
  }

  // create and write geojson "FeatureCollection"
  if wjErr := writeJSON(dir+"all-shapes.geojson", jsony{
      "type": "FeatureCollection",
      "features": features,
    }); wjErr != nil {
    return fmt.Errorf("writeJSON() %s", wjErr)
  }

  return nil
}

// exportGeoJSONStops Helper: Export GeoJSON for "stops" table.
func exportGeoJSONStops(dir string, db *sql.DB) error {

  // retrieve all stops
  stops, stopsErr := db.Query(
    "select stop_id, stop_name, stop_lat, stop_lon from stops;")
  if stopsErr != nil {
    return fmt.Errorf("failed to select stops [%s]", stopsErr)
  }
  defer stops.Close()

  var id, name string
  var lat, lng float64 // placeholder for cols
  var features []jsony
  for stops.Next() {
    if scanErr := stops.Scan(&id, &name, &lat, &lng);
      scanErr != nil {
        return fmt.Errorf("failed to scan stops query [%s]", scanErr)
    }

    // create geojson "Feature"
    feature := jsony{
      "type": "Feature",
      "properties": jsony{
        "stop_id":   id,
        "stop_name": name,
      },
      "geometry": jsony{
        "type": "Point",
        "coordinates": [2]float64{lng, lat},
      },
    }

    // write geojson "Feature"
    if wjErr := writeJSON(dir+"stop."+id+".geojson", feature);
      wjErr != nil {
      return fmt.Errorf("writeJSON() %s", wjErr)
    }

    // and append for later featureCol
    features = append(features, feature)
  }

  // create and write geojson "FeatureCollection"
  if wjErr := writeJSON(dir+"all-stops.geojson", jsony{
      "type": "FeatureCollection",
      "features": features,
    }); wjErr != nil {
    return fmt.Errorf("writeJSON() %s", wjErr)
  }

  return nil
}

// exportGeoJSONRoutes Helper: Export GeoJSON for special
// intersections of "stops" against "routes"+"trips"+"shapes" data.
// note: Spatialite extension must be enabled!
func exportGeoJSONRoutes(dir string, db *sql.DB) error {

  // confirm that spatialite extension is loaded
  if hasDBSpatialite(db) == false {
    return fmt.Errorf("spatialite is not loaded")
  }

  routes, rErr := db.Query(
    "select route_id, direction_id, asGeoJSON(geom)," +
    "asGeoJSON(stopgeom), asGeoJSON(pathgeom) from routes_geo;")
  if rErr != nil {
    return fmt.Errorf("could not query routes_geo [%s]", rErr)
  }

  var id, direc, rteGeo, rteStopGeo, rtePathGeo string
  for routes.Next() {
    if sErr := routes.Scan(&id, &direc, &rteGeo, &rteStopGeo, &rtePathGeo);
      sErr != nil {
      return fmt.Errorf("could not scan routes_geo [%s]", sErr)
    }

    // write route line geojson
    if wlErr := ioutil.WriteFile(fmt.Sprintf(
      "%sroute-%s-dir-%s-line.geojson",
      dir, id, direc), []byte(rteGeo), 0666);
      wlErr != nil {
        return fmt.Errorf("failed to write route line geojson file [%s]", wlErr)
    }

    // write route stop geojson
    if wsErr := ioutil.WriteFile(fmt.Sprintf(
      "%sroute-%s-dir-%s-stop.geojson",
      dir, id, direc), []byte(rteStopGeo), 0666);
      wsErr != nil {
        return fmt.Errorf("failed to write route stop geojson file [%s]", wsErr)
    }

    // write route path geojson
    if wpErr := ioutil.WriteFile(fmt.Sprintf(
      "%sroute-%s-dir-%s-path.geojson",
      dir, id, direc), []byte(rtePathGeo), 0666);
      wpErr != nil {
        return fmt.Errorf("failed to write route path geojson file [%s]", wpErr)
    }
  }

  return nil
}

// exportGeoJSONTransfers Helper: Export GeoJSON for "transfers" table.
func exportGeoJSONTransfers(dir string, db *sql.DB) error {

  // retrieve all transfers w/ stop
  transfers, transErr := db.Query(
    "select t.'from_stop_id', t.'to_stop_id', t.'transfer_type', " +
    "sf.'stop_lat' as sflat, sf.'stop_lon' as sflon, " +
    "st.'stop_lat' as stlat, st.'stop_lon' as stlon " +
    "from 'transfers' t " +
    "left join 'stops' sf on t.'from_stop_id' = sf.'stop_id' " +
    "left join 'stops' st on t.'to_stop_id' = st.'stop_id' " +
    "where t.'from_stop_id' != t.'to_stop_id';")
  if transErr != nil {
    return fmt.Errorf("failed to select transfers joined stops [%s]", transErr)
  }
  defer transfers.Close()

  var from, to string
  var trans int
  var flat, flng, tlat, tlng float64 // placeholder for cols
  var features []jsony
  for transfers.Next() {
    if scanErr := transfers.Scan(
      &from, &to, &trans, &flat, &flng, &tlat, &tlng);
      scanErr != nil {
        return fmt.Errorf("failed to scan transfers query [%s]", scanErr)
    }

    // create geojson "Feature"
    feature := jsony{
      "type": "Feature",
      "properties": jsony{
        "from_stop_id": from,
        "to_stop_id": to,
        "transfer_type": trans,
      },
      "geometry": jsony{
        "type": "LineString",
        "coordinates": [2][2]float64{
          [2]float64{flng, flat},
          [2]float64{tlng, tlat},
        },
      },
    }

    // write geojson "Feature"
    if wjErr := writeJSON(dir+"transfer."+from+"-"+to+".geojson", feature);
      wjErr != nil {
      return fmt.Errorf("writeJSON() %s", wjErr)
    }

    // and append to featureCol
    features = append(features, feature)
  }

  // create and write geojson "FeatureCollection"
  if wjErr := writeJSON(dir+"all-transfers.geojson", jsony{
      "type": "FeatureCollection",
      "features": features,
    }); wjErr != nil {
    return fmt.Errorf("writeJSON() %s", wjErr)
  }

  return nil
}
