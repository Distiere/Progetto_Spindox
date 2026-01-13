import duckdb

con = duckdb.connect("data/warehouse.duckdb", read_only=True)

sql = r"""
WITH base AS (
    SELECT
        c.incident_number,
        c.call_number,

        -- calls location
        c.address AS c_address,
        c.city AS c_city,
        c.zipcode_of_incident AS c_zipcode,
        c.neighborhoods_analysis_boundaries AS c_neighborhood,
        c.battalion AS c_battalion,
        c.station_area AS c_station_area,
        c.supervisor_district AS c_supervisor_district,
        c.fire_prevention_district AS c_fire_prevention_district,
        c.box AS c_box,
        c.location AS c_location_point,

        -- incidents fallback location
        i.address AS i_address,
        i.city AS i_city,
        i.zipcode AS i_zipcode,
        i.neighborhood_district AS i_neighborhood,
        i.battalion AS i_battalion,
        i.station_area AS i_station_area,
        i.supervisor_district AS i_supervisor_district,
        i.box AS i_box,
        i.location AS i_location_point

    FROM silver.calls_clean c
    LEFT JOIN silver.incidents_clean i
      ON i.incident_number = c.incident_number
),
keys AS (
    SELECT
        incident_number,
        call_number,

        -- coalesce + NORMALIZZAZIONE (molto importante)
        NULLIF(TRIM(COALESCE(c_address, i_address)), '') AS address,
        NULLIF(TRIM(COALESCE(c_city, i_city)), '') AS city,
        COALESCE(c_zipcode, i_zipcode) AS zipcode,
        NULLIF(TRIM(COALESCE(c_neighborhood, i_neighborhood)), '') AS neighborhood,
        NULLIF(TRIM(COALESCE(c_battalion, i_battalion)), '') AS battalion,
        NULLIF(TRIM(COALESCE(c_station_area, i_station_area)), '') AS station_area,
        COALESCE(c_supervisor_district, i_supervisor_district) AS supervisor_district,
        NULLIF(TRIM(c_fire_prevention_district), '') AS fire_prevention_district,

        -- QUI: box cast a VARCHAR (il tuo mismatch)
        NULLIF(TRIM(COALESCE(CAST(c_box AS VARCHAR), CAST(i_box AS VARCHAR))), '') AS box,

        NULLIF(TRIM(COALESCE(c_location_point, i_location_point)), '') AS location_point
    FROM base
)
SELECT
    k.*
FROM keys k
LEFT JOIN gold.dim_location dl
  ON dl.address IS NOT DISTINCT FROM k.address
 AND dl.city IS NOT DISTINCT FROM k.city
 AND dl.zipcode IS NOT DISTINCT FROM k.zipcode
 AND dl.neighborhood IS NOT DISTINCT FROM k.neighborhood
 AND dl.battalion IS NOT DISTINCT FROM k.battalion
 AND dl.station_area IS NOT DISTINCT FROM k.station_area
 AND dl.supervisor_district IS NOT DISTINCT FROM k.supervisor_district
 AND dl.fire_prevention_district IS NOT DISTINCT FROM k.fire_prevention_district
 AND dl.box IS NOT DISTINCT FROM k.box
 AND dl.location_point IS NOT DISTINCT FROM k.location_point
WHERE dl.location_id IS NULL
LIMIT 25;
"""

print(con.execute(sql).fetchdf())
con.close()
