## Create Intersections ##
library(tidycensus)
library(glue)
library(DBI)

# pull the start and end point from each TIGER/Line edge from the edges matching the criteria for walkable streets. 
build_endpoints <- function(conn = U, year = 2016, state_abbr = 'wa'){
  
  #get state fips
  state_fips <- unique(fips_codes$state_code[fips_codes$state == toupper(state_abbr)])
  
  # Find edges endpoints of roads ('S.*')
  # --- 1. Endpoints ---
  sql_drop_table <- glue("DROP TABLE IF EXISTS edges.endpoints_{state_fips}_{year}")
  sql_create_table <-glue("
    CREATE TABLE edges.endpoints_{state_fips}_{year} AS
    WITH filtered_edges AS (
    SELECT
      tlid,
      mtfcc,
      year,
      geom,
      ST_Length(ST_Transform(geom, 26910)) AS seg_length, 
      
      -- snapped start/end once
      ST_SnapToGrid(ST_Transform(ST_StartPoint(geom), 26910), 0.1)::geometry(point, 26910) AS p0_geom, 
      
      ST_SnapToGrid(ST_Transform(ST_EndPoint(geom), 26910), 0.1)::geometry(point, 26910) AS p1_geom
      
    FROM edges_state.edge_{year}_{state_fips}
    WHERE mtfcc ~* 'S.*'
      AND mtfcc NOT IN ('S1100','S1500','S1630','S1640','S1730','S1740','S1750','S1780')
    ), 
    
    non_loop_edges AS (
      SELECT *, 
      ST_Distance(p0_geom, p1_geom) AS pnt_distance
      FROM filtered_edges
      WHERE ST_Distance(p0_geom, p1_geom) > 20 -- remove loops and short U-shapes
    )
    
    SELECT
      tlid, 
      mtfcc, 
      year, 
      'p0' AS start_end,
      pnt_distance,
      p0_geom AS geom_snap_26910,
      seg_length
    FROM non_loop_edges
    
UNION ALL

SELECT
    tlid,
    mtfcc,
    year,
    'p1' AS start_end,
    pnt_distance,
    p1_geom AS geom_snap_26910, 
    seg_length
FROM non_loop_edges;")

  dbExecute(conn, sql_drop_table)
  dbExecute(conn, sql_create_table)

  
}

# get degrees (non-distinct endpoints) -- want to include cul-de-sacs?
build_degrees <- function(conn = U, year = 2016, state_abbr = 'wa'){
  
  #get state fips
  state_fips <- unique(fips_codes$state_code[fips_codes$state == toupper(state_abbr)])
  
  # Find edges endpoints of roads ('S.*')
  # --- 1. Endpoints ---
  sql_drop_table <- glue("DROP TABLE IF EXISTS edges.node_degrees_{state_fips}_{year}")
 
  sql_create_table <-glue("
        CREATE TABLE edges.node_degrees_{state_fips}_{year} AS
        SELECT
          geom_snap_26910, 
          COUNT(*) AS degree
          FROM edges.endpoints_{state_fips}_{year}
          GROUP BY (geom_snap_26910);")
  
  dbExecute(conn, sql_drop_table)
  dbExecute(conn, sql_create_table)
  
  # --- 2. join degrees back to endpoints --- 
  sql_drop_table <- glue("DROP TABLE IF EXISTS edges.endpoints_with_degree_{state_fips}_{year}")
  sql_create_table <- glue("
  CREATE TABLE edges.endpoints_with_degree_{state_fips}_{year} AS
  SELECT
    e.*,
    d.degree
  FROM edges.endpoints_{state_fips}_{year} e
  LEFT JOIN edges.node_degrees_{state_fips}_{year} d
  ON e.geom_snap_26910 = d.geom_snap_26910;
  ")

  dbExecute(conn, sql_drop_table)
  dbExecute(conn, sql_create_table)
  
  # --- 3. Collapse to segment-level ---
  ## check indexing for endpoints_with_degree
  
  index_query = glue("SELECT *
    FROM pg_indexes
    WHERE schemaname = 'edges'
    AND tablename = 'endpoints_with_degree_{state_fips}_{year}'
    AND indexdef ILIKE '%tlid%';")
  
  index_query_n <- DBI::dbGetQuery(conn = U, 
                                  "SELECT * 
                                  FROM pg_indexes
                                  WHERE schemaname = 'edges'
                                    AND tablename = 'endpoints_with_degree_{state_fips}_{year}';")
                                  #index_query)
  if(nrow(index_query_n) == 0){
    create_index_query = glue("
                              CREATE INDEX idx_endpoints_with_degree_tlid
                              ON edges.endpoints_with_degree_{state_fips}_{year} (tlid);"
    )
    
   # dbExecute(conn = U, create_index_query)

  }
  
  sql_drop_table <- glue("DROP TABLE IF EXISTS edges.segment_degrees_{state_fips}_{year}")
  
  sql_create_table <- glue("
                           CREATE TABLE edges.segment_degrees_{state_fips}_{year} AS
                           SELECT
                           e0.tlid,
                           e0.degree AS degree_p0,
                           e1.degree AS degree_p1,
                           e0.seg_length AS seg_length,
                           CASE 
                           WHEN (e0.degree = 1 OR e1.degree = 1)
                           THEN 1
                           ELSE 0
                           END AS is_dangler
                           FROM edges.endpoints_with_degree_{state_fips}_{year} e0
                           JOIN edges.endpoints_with_degree_{state_fips}_{year} e1
                           ON e0.tlid = e1.tlid
                           WHERE e0.start_end = 'p0'
                           AND e1.start_end = 'p1';
                           ")
  
  dbExecute(conn, sql_drop_table)
  dbExecute(conn, sql_create_table)
  
}

# get intersections (overlapping endpoints, not counting duplicate endpoints or endpoints of danglers (dead-ends <=30m length)
build_intersections <- function(conn = U, state_abbr = 'wa', year = 2016,
                                 dangler_length_m = NA){
  
  #get state fips
  state_fips <- unique(fips_codes$state_code[fips_codes$state == toupper(state_abbr)])
  sql_drop_tbl <- glue('DROP TABLE IF EXISTS edges.intersections_{state_fips}_{year};')
  
  ## get intersections (3+ endpoints)
  ## updated to only count distinct tlids, which prevents loops (i.e., cul-de-sacs) from contributing multiple endpoints to create an intersection.
  ## update to pull from endpoints with degrees, and remove danglers (dead ends)
  if(is.na(dangler_length_m)){
    sql_create_tbl <- glue("
                           CREATE TABLE edges.intersections_{state_fips}_{year} AS
                           WITH endpoints_filtered AS (
                             SELECT e.*
                               FROM edges.endpoints_{state_fips}_{year} e
                             JOIN edges.segment_degrees_{state_fips}_{year} s
                             ON e.tlid = s.tlid
                             WHERE s.is_dangler = 0
                           )
                           SELECT
                           ROW_NUMBER() OVER() AS oid
                           , ARRAY_AGG(DISTINCT tlid::int) AS tlid 
                           , COUNT(Distinct tlid::int) AS n_points
                           , year
                           , st_union(geom_snap_26910) as geom_snap_26910
                           FROM 
                           endpoints_filtered
                           GROUP BY 
                           year, geom_snap_26910;")
  } else {
  sql_create_tbl <-glue("
        CREATE TABLE edges.intersections_{state_fips}_{year} AS
        WITH endpoints_filtered AS (
          SELECT e.*
          FROM edges.endpoints_{state_fips}_{year} e
          JOIN edges.segment_degrees_{state_fips}_{year} s
          ON e.tlid = s.tlid
          WHERE s.is_dangler = 0 OR
          (s.is_dangler = 1 AND s.seg_length > {dangler_length_m}) 
        )
        SELECT
          ROW_NUMBER() OVER() AS oid
          , ARRAY_AGG(DISTINCT tlid::int) AS tlid 
          , COUNT(Distinct tlid::int) AS n_points
          , year
          , st_union(geom_snap_26910) as geom_snap_26910
          FROM 
            endpoints_filtered
            GROUP BY 
              year, geom_snap_26910;")
  }
  dbExecute(conn = U, statement = sql_drop_tbl)
  dbExecute(conn = U, statement = sql_create_tbl)

  
  ## drop / create 3-way intersection tables ####
  sql_3way_int_drop <- glue("
                            DROP TABLE IF EXISTS edges.intersections_3plus_{state_fips}_{year}")
  
  ## create 1000m buffer around each 3-way intersection ####
  sql_3way_int_create <- glue("
                              CREATE TABLE edges.intersections_3plus_{state_fips}_{year} as 
                              SELECT *, 
                                st_buffer(geom_snap_26910, 1000, 40)::geometry(polygon, 26910) as geom_snap_buffer_26910
                                from edges.intersections_{state_fips}_{year} WHERE n_points > 2;")
  
  dbExecute(conn = U, statement = sql_3way_int_drop)
  dbExecute(conn = U, statement = sql_3way_int_create)
  
}





              