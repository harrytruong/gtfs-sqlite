package main

import (
  "os"
  "fmt"
  "database/sql"
  "io/ioutil"
  "encoding/json"
)

// Helper: json-like type pattern.
type jsony map[string]interface{}

/**
 * GeoJSON export of GTFS files.
 */
func exportGeoJSON(dir string, db *sql.DB, spatialite bool) error {
  dir += "geojson/" // export to "geojson" subdir
  stopsDir := dir+"stops/"
  shapesDir := dir+"shapes/"
  pathsDir := dir+"paths/"
  transfersDir := dir+"transfers/"

  // ensure dir (and extra subdirs) exists
  for _, d := range [...]string{
    dir, shapesDir, stopsDir, transfersDir, pathsDir} {
    if mkdirErr := os.MkdirAll(d, 0777); mkdirErr != nil {
      return mkdirErr
    }
  }

  if stopsErr := exportGeoJSONStops(stopsDir, db); stopsErr != nil {
    return fmt.Errorf("Failed to export stops: %s", stopsErr)
  }

  if isExistDB("shapes", db) { // only export, if table exists
    if shapesErr := exportGeoJSONShapes(shapesDir, db); shapesErr != nil {
      return fmt.Errorf("Failed to export shapes: %s", shapesErr)
    }

    if spatialite { // only export if spatialite is enabled
      if pathsErr := exportGeoJSONPaths(pathsDir, db); pathsErr != nil {
        return fmt.Errorf("Failed to export paths: %s", pathsErr)
      }
    }
  }

  if isExistDB("transfers", db) { // only export, if table exists
    if transErr := exportGeoJSONTransfers(transfersDir, db); transErr != nil {
      return fmt.Errorf("Failed to export transfers: %s", transErr)
    }
  }

  return nil
}

/**
 * Helper: Export GeoJSON for "shapes" table.
 */
func exportGeoJSONShapes(dir string, db *sql.DB) error {

  // retrieve all unique shapes
  shapes, shapesErr := db.Query(
    "select distinct(shape_id) as id from shapes;")
  if shapesErr != nil {
    return shapesErr
  }
  defer shapes.Close()

  var id string // placeholder for "shape_id" col
  var features []jsony
  for shapes.Next() {
    if scanErr := shapes.Scan(&id); scanErr != nil {
      return scanErr
    }

    // retrive all points for this shape
    var shapeLine [][2]float64
    var lat, lng float64 // placeholder for "lat", "lon" col
    points, ptErr := db.Query(
      "select shape_pt_lat, shape_pt_lon from shapes " +
      "where shape_id = ? order by cast(shape_pt_sequence as int) asc;", id)
    if ptErr != nil {
      return ptErr
    }
    defer points.Close()
    for points.Next() {
      if ptscanErr := points.Scan(&lat, &lng); ptscanErr != nil {
        return ptscanErr
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

    features = append(features, feature)

    // marshall into json []byte
    fjson, fjErr := json.Marshal(feature)
    if fjErr != nil {
      return fjErr
    }

    // write "Feature" geojson file
    if wErr := ioutil.WriteFile(dir+"shape."+id+".geojson", fjson, 0666);
      wErr != nil {
      return wErr
    }
  }

  // create geojson "FeatureCollection",
  // and marshall into json []byte
  fjson, fjErr := json.Marshal(jsony{
    "type": "FeatureCollection",
    "features": features,
  })
  if fjErr != nil {
    return fjErr
  }

  // write "FeatureCollection" geojson file
  if wErr := ioutil.WriteFile(dir+"all-shapes.geojson", fjson, 0666);
    wErr != nil {
    return wErr
  }

  return nil
}

/**
 * Helper: Export GeoJSON for "stops" table.
 */
func exportGeoJSONStops(dir string, db *sql.DB) error {

  // retrieve all stops
  stops, stopsErr := db.Query(
    "select stop_id, stop_name, stop_lat, stop_lon from stops;")
  if stopsErr != nil {
    return stopsErr
  }
  defer stops.Close()

  var id, name string
  var lat, lng float64 // placeholder for cols
  var features []jsony
  for stops.Next() {
    if scanErr := stops.Scan(&id, &name, &lat, &lng);
      scanErr != nil {
      return scanErr
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

    // marshall into json []byte
    fjson, fjErr := json.Marshal(feature)
    if fjErr != nil {
      return fjErr
    }

    // write "Feature" geojson file
    if wErr := ioutil.WriteFile(dir+"stop."+id+".geojson", fjson, 0666);
      wErr != nil {
      return wErr
    }

    // and append to featureCol
    features = append(features, feature)
  }

  // create geojson "FeatureCollection",
  // and marshall into json []byte
  fjson, fjErr := json.Marshal(jsony{
    "type": "FeatureCollection",
    "features": features,
  })
  if fjErr != nil {
    return fjErr
  }

  // write "FeatureCollection" geojson file
  if wErr := ioutil.WriteFile(dir+"all-stops.geojson", fjson, 0666);
    wErr != nil {
    return wErr
  }

  return nil
}

/**
 * Helper: Export GeoJSON for special intersections of
 *         "stops" with "shapes" data.
 *
 * Note: This requires spatialite to be enabled.
 */
func exportGeoJSONPaths(dir string, db *sql.DB) error {

  // todo: retrieve all stops and hashmap by coord
  // todo: create new geometry that merges all shapes (lines)
  // todo: create new geometry that splits all shapes by stops (multilines)
  // todo: map each section of multiline against stop coord hashmap
  // todo: export geojson of each path between stops (lines)
  // todo: export geojson col for all paths (col lines)

  // todo: create new geometry to offset each path between stops (lines)
  // todo: export geojson col for all paths offsets (col lines)

  return nil
}
/**
 * Helper: Export GeoJSON for "transfers" table.
 */
func exportGeoJSONTransfers(dir string, db *sql.DB) error {

  // retrieve all transfers w/ stop
  transfers, transfersErr := db.Query(
    "select t.'from_stop_id', t.'to_stop_id', t.'transfer_type', " +
    "sf.'stop_lat' as sflat, sf.'stop_lon' as sflon, " +
    "st.'stop_lat' as stlat, st.'stop_lon' as stlon " +
    "from 'transfers' t " +
    "left join 'stops' sf on t.'from_stop_id' = sf.'stop_id' " +
    "left join 'stops' st on t.'to_stop_id' = st.'stop_id' " +
    "where t.'from_stop_id' != t.'to_stop_id';")
  if transfersErr != nil {
    return transfersErr
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
      return scanErr
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

    // marshall into json []byte
    fjson, fjErr := json.Marshal(feature)
    if fjErr != nil {
      return fjErr
    }

    // write "Feature" geojson file
    if wErr := ioutil.WriteFile(
      dir+"transfer."+from+"-"+to+".geojson", fjson, 0666);
      wErr != nil {
      return wErr
    }

    // and append to featureCol
    features = append(features, feature)
  }

  // create geojson "FeatureCollection",
  // and marshall into json []byte
  fjson, fjErr := json.Marshal(jsony{
    "type": "FeatureCollection",
    "features": features,
  })
  if fjErr != nil {
    return fjErr
  }

  // write "FeatureCollection" geojson file
  if wErr := ioutil.WriteFile(dir+"all-transfers.geojson", fjson, 0666);
    wErr != nil {
    return wErr
  }

  return nil
}
