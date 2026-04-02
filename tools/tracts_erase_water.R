# erase area_water from tracts
# Phil Hurvitz phurvitz@uw.edu 2022-09-19

library(tidyverse)
library(tigris)
library(magrittr)
library(tidyr)


f_create_water_years_table <- function(years = years, 
                                       state_abbr_list = state_abbr_list){
    # years
    # water years
    water_years <- data.frame(water = TRUE, water_year = c(2011:2025))
    # tract years
    tract_years <- data.frame(tract = TRUE, tract_year = c(2000:2025))
    # join and assign proper water year to each tract year
    years_full <- left_join(tract_years, water_years, by = c("tract_year" = "water_year"))
    years_full %<>% mutate(
    water_year = tract_year
      )
    years_full %<>% mutate(
      water_year = case_when(
        is.na(water) ~ 2011,
        .default = water_year
      )
    )
    years_full %<>%
      dplyr::select(tract_year, water_year) %>% 
      filter(tract_year %in% years)

    # these next create data frames enumerating the tables to be created
    # year-state-county
    counties_fips <- fips_codes %>% dplyr::filter(tolower(state) %in% state_abbr_list) %>% 
     crossing(years_full) 

    # year
    year_df <- tract_years %>% 
     as.data.frame() %>% 
     mutate(
       tablename = str_c("tract_nowater", str_c("tract_nowater", tract_year, sep="_"), sep = ".")
      ) %>%
      filter(tract_year %in% years)

    # year-state
    states_df <- counties_fips %>% 
     dplyr::select(tract_year, water_year, state_code) %>% 
      distinct() %>% 
      mutate(tablename = str_c("tract_nowater_state", str_c("tract_nowater", tract_year, state_code, sep="_"), sep = "."),
             tract_tablename = str_c("tract_state", str_c("tract", tract_year, state_code, sep="_"), sep="."),
            water_tablename = str_c("area_water_state", str_c("area_water", water_year, state_code, sep="_"), sep = ".")) %>% 
      arrange(tract_year, state_code)

    # year-state-county
    counties_df <- counties_fips %>% 
     dplyr::select(tract_year, water_year, state_code, county_code) %>% 
      distinct() %>% 
      mutate(tablename = str_c("tract_nowater_county", str_c("tract_nowater", tract_year, state_code, county_code, sep="_"), sep = "."),
             tract_tablename = str_c("tract_county", str_c("tract", tract_year, state_code, county_code, sep="_"), sep = "."),
           water_tablename = str_c("area_water_county",str_c("area_water", water_year, state_code, county_code, sep="_"), sep = ".")) %>% 
      arrange(tract_year, state_code, county_code)
    
    return(list(counties_df = counties_df, year_df = year_df, states_df = states_df))

}

# create schemas
f_nowater_create_schemas <- function() {

  ## UPDATE - AY to separate statements
  sq1<-'create schema if not exists tract_nowater;'
  O <- dbExecute(conn = U, statement = sq1)
  sq2 <- 'create schema if not exists tract_nowater_state;'
  O <- dbExecute(conn = U, statement = sq2)
  sq3<-   'create schema if not exists tract_nowater_county;'
  O <- dbExecute(conn = U, statement = sq3)
  
}
# create table structures
# template
f_nowater_tract <- function(){
  sql1 <- "create table if not exists tract_nowater.tract_nowater (like tract.tract including all) partition by list(year);"
  O <- dbGetQuery(conn = U, statement = sql1)
}
# tracts
f_nowater_partitions_year <- function(year_df){
  
  for(i in nrow(year_df)){
    YR <- year_df$tract_year[i]
    # each year's table, partitioned by state
    sql2 <- "create table if not exists tract_nowater.tract_nowater_xYRx partition of tract_nowater.tract_nowater for values in (xYRx) partition by list(statefp);" %>%
      str_replace_all(pattern = "xYRx", replacement = YR %>% as.character())
    message(sql2)
    O <- dbGetQuery(conn = U, statement = sql2)
  }
}

# tracts-years-states
f_nowater_partitions_year_state <- function(states_df){
  # create each year-state table
  for (i in 1:nrow(states_df)) {
    YR <- states_df$tract_year[i]
    tablename <- states_df$tablename[i]
    state_code <- states_df$state_code[i]
    sql3 <- "create table if not exists xTNx partition of tract_nowater.tract_nowater_xYRx for values in ('xSTx') partition by list(countyfp);" %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xYRx", replacement = YR %>% as.character()) %>%
      str_replace_all(pattern = "xSTx", state_code)
    message(sql3)
    O <- dbGetQuery(conn = U, statement = sql3)
  }
}
# tracts-years-states
f_nowater_partitions_year_state_county <- function(counties_df){
  # create each year-state table
  for (i in 1:nrow(counties_df)) {
    #for (i in 1) {
    YR <- counties_df$tract_year[i]
    tablename <- counties_df$tablename[i]
    state_code <- counties_df$state_code[i]
    county_code <- counties_df$county_code[i]
    sql4 <- "create table if not exists xTNx partition of tract_nowater_state.tract_nowater_xYRx_xSTx for values in ('xCTx');" %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xYRx", replacement = YR %>% as.character()) %>%
      str_replace_all(pattern = "xSTx", state_code) %>% 
      str_replace_all(pattern = "xCTx", county_code)
    message(sql4)
    O <- dbGetQuery(conn = U, statement = sql4)
  }
}
f_nowater_create_partitions <- function(years, 
                                        state_abbr_list){
  tract_water_table<-f_create_water_years_table(years = years, 
                                                state_abbr_list = state_abbr_list)
  
  year_list <- tract_water_table$year_df
  counties_list <- tract_water_table$counties_df
  states_list <- tract_water_table$states_df
  
  f_nowater_partitions_year(year_df = year_list)
  f_nowater_partitions_year_state(states_df = states_list)
  f_nowater_partitions_year_state_county(counties_df = counties_list)
}
# erase frome one state-county-year
f_nowater_erase_0one <- function(i, vrb = TRUE, 
                                 counties_df = counties_df){
  # data from counties data frame
  out_table <- counties_df$tablename[i]
  tract_table <- counties_df$tract_tablename[i]
  water_table <- counties_df$water_tablename[i]
  if(vrb){
    message(paste0("    creating ", out_table))
  }
  
  if(vrb){
    message("    erasing water....")
  }
  
  ## UPDATE -- rewrite statements to run one at a time AY: 
  
#   # SQL to create the temp table
  sql_erase <- "DROP TABLE IF EXISTS tmp.w;"
  sql_create <- "
CREATE TABLE tmp.w AS
WITH t AS (SELECT * FROM xTTx)
   , w AS (SELECT ST_Union(geom_4326) AS geom_4326 FROM xWTx)
   ,
   --no water at all
    nw AS (SELECT t.*
           FROM t
           LEFT JOIN w
            ON ST_Intersects(t.geom_4326, w.geom_4326)
           WHERE w.geom_4326 IS NULL)
   ,
   --water-intersected tracts
    yw AS (SELECT t.ogc_fid
                , t.uid
                , t.year
                , t.statefp
                , t.countyfp
                , t.tractce
                , t.geoid
                , t.name
                , t.namelsad
                , t.mtfcc
                , t.funcstat
                , t.aland
                , t.awater
                , t.intptlat
                , t.intptlon
                , ST_Difference(t.geom_4326, w.geom_4326) AS geom_4326
           FROM t
           CROSS JOIN w
            WHERE ST_Intersects(t.geom_4326, w.geom_4326))
--union all
    , u AS (
          SELECT 
        ogc_fid, uid, year, statefp, countyfp, tractce, geoid, name, namelsad, 
        mtfcc, funcstat, aland, awater, intptlat, intptlon,
        ST_Multi(geom_4326)::geometry(MULTIPOLYGON, 4326) AS geom_4326 
        FROM nw
          UNION ALL
          SELECT 
        ogc_fid, uid, year, statefp, countyfp, tractce, geoid, name, namelsad, 
        mtfcc, funcstat, aland, awater, intptlat, intptlon,
        ST_Multi(geom_4326)::geometry(MULTIPOLYGON, 4326) AS geom_4326 
        FROM yw
)
SELECT *
FROM u;" %>%
    str_replace_all(pattern = "xTTx", tract_table) %>%
    str_replace_all(pattern = "xWTx", water_table)

dbExecute(conn = U, statement = sql_erase)
dbExecute(conn = U, statement = sql_create)

  
 # upsert
  if(vrb){
    message("    upsert....")
  }
  
  #O <- dbExecute(conn = U, statement = "insert into tract_nowater.tract_nowater (ogc_fid,uid,year,statefp,countyfp,tractce,geoid,name,namelsad,mtfcc,funcstat,aland,awater,intptlat,intptlon,ST_Multi(geom_4326)::geometry(MULTIPOLYGON) select * from tmp.w on conflict do nothing;")
 dbExecute(conn = U, statement = "
  INSERT INTO tract_nowater.tract_nowater
    SELECT ogc_fid, uid, year, statefp, countyfp, tractce, geoid, name, namelsad,
      mtfcc, funcstat, aland, awater, intptlat, intptlon,
      ST_Multi(geom_4326)::geometry(MULTIPOLYGON,4326) AS geom_4326
    FROM tmp.w
    ON CONFLICT (year, statefp, countyfp, tractce)
    DO UPDATE SET
      geom_4326 = EXCLUDED.geom_4326,
      awater = EXCLUDED.awater,
      aland = EXCLUDED.aland")

  
  if(vrb){
    message("    drop idle connections....")
  }    
  # drop idles in db activity
  #dbGetQuery(conn = U, statement = glue('SELECT * from {out_table};'))
  O <- dbGetQuery(conn = U, statement = "SELECT pg_terminate_backend(pid) from (select pid, state from pg_stat_activity where state ~ 'idle' and query ~ '^SELECT') as foo;")    
  
  }
f_nowater_erase_1all <- function(startnum = 1, 
                         years = c(2016), 
                         state_abbr = c('wa'),
                         verbose = TRUE){
  # timing and iteration
  t0 <- Sys.time()
  idx <- 0
  
  #get all counties for all states
  counties_fips <- fips_codes %>% dplyr::filter(tolower(state) %in% state_abbr)
  
  tract_water_table<-f_create_water_years_table(years = years, 
                                                state_abbr_list = state_abbr)
  
  year_df <- tract_water_table$year_df
  counties_df <- tract_water_table$counties_df
  states_df <- tract_water_table$states_df
  
  # run over all entries in county data frame
  for(i in startnum:nrow(counties_df)){
    idx <- idx + 1
    if(verbose){
      message(paste(i, "of",  nrow(counties_df)))
    }
    # run it
    f_nowater_erase_0one(i = i, vrb = verbose, counties_df = counties_df)
    # elapsed
    elapsed <- difftime(Sys.time(), t0, units = "secs") %>% as.numeric() %>% round(2)
    # time per run
    t.run.s <- (elapsed / idx) %>% round(2)
    # estimated time to completion
    total.hours <- (t.run.s * (nrow(counties_df) - i) / 3600) %>%  round(2)
    if(verbose){
      message(paste("    Run:", idx, "; elapsed s:", elapsed, "; s per run:", t.run.s, "; completion:", total.hours, "h"))
    }        
  }
}
