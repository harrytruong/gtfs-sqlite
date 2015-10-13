# gtfs-sqlite
GTFS to SQLite3 (+ Spatialite) (+ CSV / JSON / GeoJSON / KML)

## Installation
todo.

## Usage

**`$ gtfs-sqlite [options] zipFile`**

```
  zipFile`
      Path to local GTFS file, or URL for an external file.
      e.g., "http://example.com/google_transit.zip",
            "local/path/to/google_transit.zip"

  options:

      -dir
        	Output file directory. (default "gtfs-output/")

      -name
        	Output sqlite filename. (default "gtfs.sqlite")

      -server
        	Run HTTP server to demo output files.

      -skip-extras
        	Skip extra export file formats (csv, json, geojson, kml).

      -spatialite
        	Include spatialite-enabled sqlite tables.
```
