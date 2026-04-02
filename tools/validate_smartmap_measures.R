## Validation ##
#library(knitr) # knitr for kable tables
library(kableExtra) # pretty tables### UPDATE -- add library AY
library(glue)
library(dplyr)
library(leaflet)
library(raster)
library(sf)
library(DBI)

## make a test point dataset:
f_create_validation_schema <- function(){
    sql_create_schema <- 'CREATE SCHEMA if not exists validation'
    O <- dbExecute(conn = U, statement = sql_create_schema)
    
    ## create directory
    if(!file.exists('data/validation_data')){dir.create('data/validation_data')}
}
f_create_validation_points_df <- function(con_str = con_str, 
                                          conn = U,
                                          validation_points_filepath = 'H:/projects/beh_nwi_smartmap/validation_points.csv'){
  
  # import validation points csv
  validation.points <- read.csv(validation_points_filepath)
  validation.points <- st_as_sf(validation.points, coords = c('lon', 'lat'), crs = 4326)
  
  # check points on a map
  # leaflet() %>% addTiles() %>% addCircleMarkers(data = validation.points)
  #st_write(validation.points, glue('data/validation_data/validation_points_{state_fips}.gpkg'), append = FALSE)
  
  st_write(obj = validation.points, 
           dsn = con_str, 
           layer = "validation_points", 
           layer_options = c("geometry_name=geometry", "SCHEMA=tmp", "OVERWRITE=YES"), append=FALSE)
    
    # create partitioned table by year
    sql1 <- "DROP TABLE IF EXISTS validation.validation_points CASCADE;"
    O <- dbExecute(conn = conn, statement = sql1)
    
    sql2 <- "CREATE TABLE validation.validation_points (
    ogc_fid serial PRIMARY KEY, 
    pid double precision, 
    geom geometry(Point, 4326)
    );"
  #    "create table if not exists validation.validation_points AS SELECT ogc_fid, pid, ST_SetSRID(ST_GeomFromWKB(decode(geometry, 'hex')), 4326) AS geom FROM tmp.validation_points;"
    O <- dbExecute(conn = U, statement = sql2)

# insert data
  O <- dbExecute(
    conn = U, 
    statement = "
    INSERT INTO validation.validation_points (pid, geom)
SELECT pid, geometry
FROM tmp.validation_points;"
  )
 
}

f_create_validation_buffers <- function(conn = U){
  sql_drop_table <- 'DROP TABLE IF EXISTS validation.buffers CASCADE;'
  O <- dbExecute(conn, sql_drop_table)
  
  sql_create_tbl <- 'CREATE TABLE validation.buffers AS 
  SELECT
  ogc_fid, 
  pid, 
  ST_Buffer(ST_Transform(geom, 26910), 1000, 40)::geometry(polygon, 26910) AS geom
  FROM validation.validation_points;'
  
  dbExecute(conn, sql_create_tbl)
  
  # Add geom_4326 column
  dbExecute(conn, "ALTER TABLE validation.buffers ADD COLUMN geom_4326 geometry(polygon, 4326);")
  
  # Update geom_4326
  dbExecute(conn, "UPDATE validation.buffers SET geom_4326 = ST_Transform(geom, 4326);")
  
  # Add geom_4269 column
  dbExecute(conn, "ALTER TABLE validation.buffers ADD COLUMN geom_4269 geometry(polygon, 4269);")
  
  # Update geom_4269
  dbExecute(conn, "UPDATE validation.buffers SET geom_4269 = ST_Transform(geom, 4269);")
  
  # Add geog_4269 column
  dbExecute(conn, "ALTER TABLE validation.buffers ADD COLUMN geog_4269 geography(polygon, 4269);")
  
  # Update geog_4269
  dbExecute(conn, "UPDATE validation.buffers SET geog_4269 = geom_4269::geography;")
  
  # create spatial indices for db
  O <- dbExecute(conn, 'CREATE INDEX idx_validation_buffer ON validation.buffers USING gist (geom);')
  O <- dbExecute(conn, 'CREATE INDEX idx_validation_buffer_4326 ON validation.buffers USING gist (geom_4326);')
  O <- dbExecute(conn, 'CREATE INDEX idx_validation_buffer_4269 ON validation.buffers USING gist (geom_4269);')
  O <- dbExecute(conn, 'CREATE INDEX idx_validation_buffer_geog_4269 ON validation.buffers USING gist (geog_4269);')
}

## get non-water land area buffer
f_validation_land_area <- function(conn = U, year = 2016, state_fips = '53'){
  O <- dbExecute(conn, 'DROP TABLE IF EXISTS validation.land_area;')
  
  O <- dbExecute(conn, 'CREATE TABLE validation.land_area AS WITH 
                 -- tracts
                 t AS (SELECT geom_4326 FROM tract_nowater.tract_nowater),
                 -- buffers
                 b AS (SELECT pid, geom_4326 AS geom FROM validation.buffers),
                 -- intersection of tracts and buffers
                 i AS (SELECT
                         b.pid
                         , st_transform(st_intersection(t.geom_4326, b.geom), 26910)::geometry(MULTIPOLYGON, 26910) AS geom
                         FROM
                         t
                         , b
                         WHERE
                         st_intersects(t.geom_4326, b.geom))
                 --dissolve withih a single buffer
                 , d AS (SELECT pid, st_union(geom)::geometry(MULTIPOLYGON, 26910) AS geom FROM i GROUP BY pid)
                 SELECT *
                   , st_transform(geom, 4269)::geometry(MULTIPOLYGON, 4269)                           AS geom_4269
                 , st_transform(geom, 4269)::geometry(MULTIPOLYGON, 4269)::geometry(MULTIPOLYGON, 4269) AS geog_4269
                 , st_transform(geom, 4326)::geometry(MULTIPOLYGON, 4326)                           AS geom_4326
                 , st_area(d.geom)                                                             AS area_m
                 FROM
                 d;')
  
  O <- dbExecute(conn,
                 'CREATE INDEX idx_land_area_geom_4269 ON validation.land_area USING gist (geom_4269);')
  O <- dbExecute(conn, 
                 'CREATE INDEX idx_land_area_geom ON validation.land_area USING gist (geom);')
  O <- dbExecute(conn, 
                 'CREATE INDEX idx_land_area_geog_4269 ON validation.land_area USING gist (geog_4269);')
                 
  
}

###
library(raster)

f_count_intersections <- function(conn = U, year = 2016, state_fips = '53'){
  O <- dbExecute(conn,  
  '--count intersections
  DROP TABLE IF EXISTS validation.intersections;')
  O <- dbExecute(conn, 
  glue('CREATE TABLE validation.intersections AS
  WITH
  -->= 3-way intersections
  d AS (SELECT
        oid
        , tlid
        , n_points
        , year
        , geom_snap_26910
        FROM
       edges.intersections_3plus_{state_fips}_{year})
  --buffer land area
  , b AS (SELECT pid, geom FROM validation.land_area)
  --get intersections overlapping buffer
  , i AS (SELECT
          b.pid
          , oid
          , tlid
          , n_points
          FROM
          d
          , b
          WHERE
          st_intersects(d.geom_snap_26910, b.geom))
  SELECT *
    FROM
  i;')
  )
  
  O <- dbExecute(conn, 
  "--summarize intersections
  DROP TABLE IF EXISTS validation.sum_intersections;")
  
  O <- dbExecute(conn, 
  "CREATE TABLE validation.sum_intersections AS
  WITH
  i AS (SELECT pid, COUNT(*) AS n_intersections FROM validation.intersections GROUP BY pid)
  , l AS (SELECT pid, area_m FROM validation.land_area)
  , f0 AS (SELECT
           pid
           , n_intersections
           , area_m
           , n_intersections / area_m         AS intersections_dens_m2
           , n_intersections / area_m * 10000 AS intersections_dens_ha
           FROM
           i
           JOIN l USING (pid))
  SELECT *
    FROM
  f0
  ORDER BY
  pid;")
  
  }

f_count_businesses <- function(conn = U, year = 2016, state_fips = '53'){
  
  business_raster <- rast(glue('data/state_raster/business/{year}/sum/state_raster_business_density_{year}_{state_fips}.tif'))
  pts_buffers <-  st_read(con_str, query = ('SELECT * from validation.land_area'))

  # get count of businesses in the points buffers
  sum_business <- extract(business_raster, pts_buffers, fun = sum, na.rm=TRUE)
  
  O <- dbExecute(conn, 
                 "--count businesses
  DROP TABLE IF EXISTS validation.businesses;")
  
  O <- dbExecute(conn, 
  glue("CREATE TABLE validation.businesses AS
  WITH
  -- walkable businesses
  d AS (SELECT
        geom
        , company
        , state
        , primary_sic_code
        , archive_version_year AS year
        FROM
        dataaxle.walkable_business
        WHERE
        archive_version_year = {year})
  -- land area in buffer
  , b AS (SELECT gid, geom_4326 AS geom FROM validation.land_area)
  -- businesses intersecting
  , i AS (SELECT
          b.gid
          , d.company
          , d.state
          , d.primary_sic_code
          , year
          , st_transform(st_intersection(d.geom, b.geom), 26910)::geometry(point, 26910) AS geom
          FROM
          d
          , b
          WHERE
          st_intersects(d.geom, b.geom))
  SELECT *
    FROM
  i;")
  )
  
#   --summarize businesses
#   DROP TABLE IF EXISTS validation.sum_businesses;
#   CREATE TABLE validation.sum_businesses AS
#   WITH
#   i AS (SELECT gid, COUNT(*) AS n_businesses FROM validation.businesses GROUP BY gid)
#   , l AS (SELECT gid, area_m FROM validation.land_area)
#   , f0 AS (SELECT
#            gid
#            , n_businesses
#            , area_m
#            , n_businesses / area_m         AS businesses_dens_m2
#            , n_businesses / area_m * 10000 AS businesses_dens_ha
#            FROM
#            i
#            JOIN l USING (gid))
#   SELECT *
#     FROM
#   f0
#   ORDER BY
#   gid;
 }

f_validate_smartwalk <- function(dsn = con_str,
                                 year = 2016, 
                                 state_fips = '53'
                                 ){
  
  state_abbr <- unique(tolower(fips_codes$state[which(fips_codes$state_code == state_fips)]))
  pts <- st_read(dsn = dsn, layer = 'validation.validation_points')
  pts_land_area <- st_read(dsn = dsn, layer = 'validation.land_area') %>% dplyr::rename(gid = pid)
  
  ## land area from smart map db
  land_area_file <- glue('H:/projects/beh_nwi_smartmap/data/state_raster/land_area/{year}/ha/state_raster_land_area_ha_{year}_{state_fips}.tif')
  land_area_data <- rast(land_area_file)
  pts_26910 <- st_transform(pts, crs(land_area_data))
  pts_26910 <- vect(pts_26910)
  lat <- terra::extract(x = land_area_data,
                            y = pts_26910, df = TRUE) 
  lat <- lat %>% dplyr::rename(c(gid = ID, smartmap_area_ha = focal_sum))
  
  ## add pts buffer back in
  lat <- merge(pts_land_area, lat, by = 'gid')
  
  # buffer area
  ba <- dbGetQuery(conn = U, statement = "select pid as gid, st_area(geom) / 10000 as buffer_area_ha from validation.land_area")

   # join and print
  lat <- lat %>% left_join(ba, by = 'gid') %>%
    mutate('% diff' = (((smartmap_area_ha - buffer_area_ha)/buffer_area_ha) * 100) %>% round(2))
  
  ###########################################
  # get pop density
  r_popdens <- terra::rast(x = glue('H:/projects/beh_nwi_smartmap/data/state_raster/population_density/{year}/ha/state_raster_pop_density_ha_{year}_{state_fips}.tif'))
  # popdens from smartmap raster
  
  popdens_rast_pts <- terra::extract(x = r_popdens, y = pts_26910, df = TRUE) %>% 
    dplyr::rename(gid = ID, 
           pop_density_ha = focal_sum)
  
  ############################################
  ## Intersection density
  # read in a raster
  r_intdens <- terra::rast(x = glue("H:/projects/beh_nwi_smartmap/data/state_raster/intersections/{year}/ha/state_raster_intersection_density_{year}_{state_fips}.tif"))
  
  # intersection density from smartmap raster
  intdens_rast_pts <- terra::extract(x = r_intdens, y = pts_26910, df = TRUE) %>% 
    dplyr::rename(gid = ID, 
           intersection_density_ha = intersection_density)
  
  #############################################
  # get business density
  r_busdens <- terra::rast(x = glue('H:/projects/beh_nwi_smartmap/data/state_raster/business/{year}/ha/state_raster_business_density_{year}_{state_fips}.tif'))
  
  busdens_rast_pts <- terra::extract(x = r_busdens, y = pts_26910, df = TRUE) %>%
    dplyr::rename(gid = ID)
  names(busdens_rast_pts)[2] <- 'bus_density_ha'
  
  ############################################3
  # get composite walk score
  r_composite_walk <- terra::rast(x = glue('H:/projects/beh_nwi_smartmap/data/state_raster/walk_index/{year}/walk_index_{year}_{state_fips}.tif'))

  walk_rast_pts <- terra::extract(x = r_composite_walk, y = pts_26910, df = TRUE) %>%
    dplyr::rename(gid = ID)
  names(walk_rast_pts)[2] <- 'walk_score_z' 
  
  # join them all
  lat <- lat %>%
    left_join(walk_rast_pts, by = 'gid') %>%
    left_join(intdens_rast_pts, by = "gid") %>% 
    left_join(busdens_rast_pts, by = "gid") %>% 
    left_join(popdens_rast_pts, by = "gid") %>% 
    mutate(across(.cols = 2:6, \(x) .fns = round(x, 2))) 
  
  st_geometry(lat) <- lat$geom_4326
  
  pal <- colorNumeric(
    palette = 'viridis', 
    domain = lat$walk_score_z, 
    na.color = 'transparent'
  )
  
  lat_map <- lat %>% leaflet() %>% addTiles() %>% 
    addPolygons(data = lat, 
                label = ~gid, 
                fillColor = ~pal(walk_score_z), 
                fillOpacity = 0.7, 
                color = 'white', 
                weight = 1)
    #addMarkers(data = lat, label = ~as.character(gid), popup = ~as.character(intersection_density_ha))
  lat_map
  # Return both objects for display in R Markdown
  list(data = lat, map = lat_map)
}


f_vector_validation <-function(dsn = con_str, 
                               year = 2016, 
                               state_fips = '53'){
  
  state_abbr <- unique(tolower(fips_codes$state[which(fips_codes$state_code == state_fips)]))
  pts <- st_read(dsn = dsn, layer = 'validation.validation_points')
  #pts_buffers <- st_read(dsn = dsn, layer = 'validation.buffers') %>% rename(gid = pid)
  pts_land_area <- st_read(dsn = dsn, layer = 'validation.land_area') %>% dplyr::rename(gid = pid)
  
  # get summary
  valsum <- dbGetQuery(conn = U, statement = "select * from validation.sum_all;")
  valsum %<>% dplyr::select(!contains("_m")) %>% 
    #mutate(across(.cols = c(2,4,6:8), .fns = round, 2))
    mutate_at(.vars = c(2,4,6:8), .funs = round, 2)
  
  # rename columns
  cn <- colnames(valsum) %>% 
    str_replace_all(pattern = "intersections", replacement = "int") %>% 
    str_replace_all(pattern = "businesses", replacement = "bus") %>% 
    str_replace_all(pattern = "population", replacement = "pop")
  colnames(valsum) <- cn
  
  # round
  valsum %>% 
    mutate_at(c(2,4,5:7), round, 2) %>% 
    kable(caption = "Vector validation summary") %>% 
    kable_styling(bootstrap_options = c("striped", "hover", "condensed"),  full_width = F, position = "left")    
  
  
}

