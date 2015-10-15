package gtfsconv

import (
  "database/sql"
  "fmt"
  "regexp"
  "strings"
)

// define map of agency GTFS fixes
var cleanAgencyGTFS = map[string]func(*sql.DB)error{
  "mta-nyct-mta-new-york-city-transit" : cleanMTANYCT,
}

// getAgency Helper: returns list of GTFS agencies,
// with a simplified identity hash.
func getAgencies(db *sql.DB) ([]string, error) {
  reAlpha := regexp.MustCompile("[^a-z]")
  var agencies []string

  // retrieve all agencies
  ag, err := db.Query("select agency_id || ' ' || agency_name from agency;")
  if err != nil {
    return nil, fmt.Errorf("failed to query for agencies [%s]", err)
  }

  // hash agencies into clean, unique identities
  var agency string // Scan placeholder
  for ag.Next() {
    if sErr := ag.Scan(&agency); sErr != nil {
      return nil, fmt.Errorf("failed to scan agencies [%s]", sErr)
    }

    agency = strings.ToLower(agency) // lower case
    agency = reAlpha.ReplaceAllString(agency, "-") // alpha only (otherwise "-")
    agencies = append(agencies, agency)
  }

  return agencies, nil
}

// cleanGTFS implements special, agency-specific
// fixes for irregular GTFS sources.
func cleanGTFS(db *sql.DB) error {

  // determine agencies for this GTFS
  agencies, aErr := getAgencies(db)
  if aErr != nil {
    return fmt.Errorf("getAgencies() %s", aErr)
  }

  // if available, apply each agency's cleanup
  for _, agency := range agencies {
    if fn := cleanAgencyGTFS[agency]; fn != nil {
      if cErr := fn(db); cErr != nil {
        return fmt.Errorf("%s: %s", agency, cErr)
      }
    }
  }

  return nil
}

// cleanMTANYCT cleanup for MTA NYCT:
//  =>  "trips" is missing "shapes_id", although "shapes" is provded.
//      ->  update "trips" to generate "shapes_id" based on ending of
//          "trip_id" (e.g., "R..S95R", "SI.N30R", "6..N52X010")
func cleanMTANYCT(db *sql.DB) error {

  // sanity check that problem exists
  c, cErr := countDBTable(db, "*", "trips where shape_id in ('', null)")
  if cErr != nil {
    return fmt.Errorf("countDBTable() %s", cErr)
  }

  // no missing shapes, nothing to do!
  if c == 0 {
    return nil
  }

  // determine a source for fixing
  // the irregular GTFS data
  sqlMatch :=
    `select ` +

      // this is the target row to update
      `t.trip_id, ` +

      `substr(t.trip_id, 21) as fuzzy,
      substr(t.trip_id, 21, 4) as fuzzy_wuzzy,
      substr(s.shape_id, 0, 5) as wuzzy,
      (fuzzy == s.shape_id) as strong_match,
      (fuzzy_wuzzy == wuzzy) as weak_match, ` +

      // this is new shape_id to fix with
      `s.shape_id as new_shape_id ` +

    `from trips t left join ` +
    `(select distinct(shape_id) as shape_id from shapes) s ` +

      // join "trips" against "shapes_geo", based on a "fuzzy" id
      // (substr of "trip_id", starting from 21st char) matching "shape_id".
      // This is considered a "strong_match".
      `on strong_match or ` +

      // join "trips" against "shapes_geo", based on a "fuzzy_wuzzy" id
      // (substr of "trip_id", starting from 21st char, for length 4 chars)
      // matching a "wuzzy" id" (substr of "shape_id" for first 4 chars).
      // This is considered a "weak_match".
      `weak_match ` +

    `where t.shape_id in ('', null);`

  // do match query
  match, mErr := db.Query(sqlMatch)
  if mErr != nil {
    return fmt.Errorf("failed on query match [%s]", mErr)
  }
  defer match.Close()

  // add new flag indicator, for future reference about cleaning
  if hasDBTableCol(db, "trips", "x_clean") == false {
    sqlFlag := `alter table trips add column x_clean text;`
    if _, fErr := db.Exec(sqlFlag); fErr != nil {
      return fmt.Errorf("failed on adding flag [%s]", fErr)
    }
  }

  var ( // scan placeholders
    trip, newShape, fuzzy, fuzzyWuz string
    strong, weak bool
  )
  for match.Next() {
    if sErr := match.Scan(&trip, &fuzzy, &fuzzyWuz, &strong, &weak, &newShape);
      sErr != nil {
      return fmt.Errorf("failed on scanning matches [%s]", sErr)
    }

    // determine clean flag
    xClean := "ERROR"
    switch {
      case strong: xClean = "shape_id_fix_strong_match"
      case weak: xClean = "shape_id_fix_weak_match"
    }

    // patch sanity check
    if newShape == "" || xClean == "" {
      return fmt.Errorf("failed to determine new shape for trip %s", trip)
    }

    // do patch query, one row at a time
    sqlPatch := "update trips set shape_id = ?, x_clean = ? " +
                "where trip_id = ?;"
    if _, pErr := db.Exec(sqlPatch, newShape, xClean, trip); pErr != nil {
      return fmt.Errorf("failed to patch with new shape [%s]", pErr)
    }
  }

  // final sanity check that cleaning sucecessful
  cs, csErr := countDBTable(db, "*", "trips where shape_id in ('', null)")
  if csErr != nil {
    return fmt.Errorf("countDBTable() %s", csErr)
  }

  if cs > 0 {
    return fmt.Errorf("failed to fully patch shapes: %v still missing", cs)
  }

  // indicate we succssfully ran a cleaning on this table
  if _, ciErr := db.Exec(
    "update gtfs_metadata set cleaning = 1 " +
    "where tablename = trips;", ); ciErr != nil {
    return fmt.Errorf("failed to note successful cleaning [%s]", ciErr)
  }

  return nil
}
