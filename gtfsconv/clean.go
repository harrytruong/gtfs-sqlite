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
  errorCount, cErr := countDBTable(db, "*",
    "trips where shape_id = '' or shape_id is null")
  if cErr != nil {
    return fmt.Errorf("countDBTable() %s", cErr)
  }

  // no missing shapes, nothing to do!
  if errorCount == 0 {
    return nil
  }

  // collect a target list of "trips" rows to fix
  target, tErr := db.Query(
    "select distinct substr(trip_id, 21,4) " +
    "from trips where shape_id = '' or shape_id is null;")
  if tErr != nil {
    return fmt.Errorf("failed on query target [%s]", tErr)
  }

  var shapes []string
  for target.Next() {
    var genshp string // scan placeholder
    if sErr := target.Scan(&genshp); sErr != nil {
      return fmt.Errorf("failed to scan bad trips [%s]", sErr)
    }
    shapes = append(shapes, genshp)
  }
  target.Close()

  // for each missing general shape,
  for _, shp := range shapes {
    var simShp string

    // determine the LONGEST similar shape
    if simErr := db.QueryRow(fmt.Sprintf(`
      select shape_id from shapes where shape_id like '%s%%'
      group by shape_id order by count(shape_id) desc limit 1;`,
      shp)).Scan(&simShp); simErr != nil {
      return fmt.Errorf("failed to find similar shape [%s]", simErr)
    }

    // update all irregular trips to use this shape
    if _, uErr := db.Exec(fmt.Sprintf(`
      update trips set shape_id = '%s'
      where (shape_id = '' or shape_id is null) and trip_id like '%%%s%%';`,
      simShp, shp)); uErr != nil {
      return fmt.Errorf("failed to update with general shape [%s]", uErr)
    }
  }

  // add new flag indicator, for future reference about cleaning
  // if hasDBTableCol(db, "trips", "x_clean") == false {
  //   sqlFlag := `alter table trips add column x_clean text;`
  //   if _, fErr := db.Exec(sqlFlag); fErr != nil {
  //     return fmt.Errorf("failed on adding flag [%s]", fErr)
  //   }
  // }

  // final sanity check that cleaning sucecessful
  cs, csErr := countDBTable(db, "*",
    "trips where shape_id = '' or shape_id is null")
  switch {
    case csErr != nil: return fmt.Errorf("countDBTable() %s", csErr)
    case cs > 0: return fmt.Errorf(
      "failed to fully patch shapes: %v still missing", cs)
  }

  // indicate we succssfully ran a cleaning on this table
  if _, ciErr := db.Exec(
    "update gtfs_metadata set cleaned = 1 " +
    "where tablename = 'trips';"); ciErr != nil {
    return fmt.Errorf("failed to note successful cleaning [%s]", ciErr)
  }

  return nil
}
