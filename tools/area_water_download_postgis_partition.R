# download water for many years over entire US into partitions 
# copied from tracts_download_postgis_partition
## Updated and edited by Amy Youngbloom 09-25-2025

library(tidyverse)
library(magrittr)
library(tidycensus)
library(tigris)
library(sf)
library(tidyr)
library(dplyr)

# PostGIS db (docker)
#library(postGIStools) # pull PostGIS into sf
#source("dbconnect.R")
#U <- us_census <- connectdb()

# write to tmp
#dsn = "PG:host=db dbname=walk_db user=aybloom password=census port=5432"
# tigris
options(tigris_use_cache = TRUE)
tcachedir <- "gisdata/us_census/tract"
# set cache dir if necessary
if(Sys.getenv("TIGRIS_CACHE_DIR") != tcachedir){
  tigris_cache_dir(path = tcachedir)
  readRenviron('~/.Renviron')
}
f_download_reformat_water <- function(year = 2020, state = counties_fips$state_code[1], 
                                      county = counties_fips$county_code[1]){
 
  # water only available for 2011 and later: 
  if(year < 2011){
    year <- 2011
  }
  print(year)
  # get data
  x <- area_water(state = state, county = county, year = year) 
  
  # decade
  decade <- year - 2000
  
  # drop superfluous columns
  cn <- colnames(x)
  
  # do the decade columns exist?
  if(str_detect(cn, pattern = regex(paste0("STATEFP", decade))) %>% any()){
    x %<>% dplyr::select(-c("STATEFP", "COUNTYFP"))
  }
  
  # lowercase, drop numeric suffixes
  colnames(x) %<>% str_to_lower() %>%
    str_replace_all(pattern = "[[:digit:]]*$", replacement = "")
  
  # year
  x %<>% mutate(year = year %>% as.integer())
  # geoid - county, add state, county fp
  x %<>% mutate(statefp = state, 
                countyfp = county,
    county_geoid = str_c(statefp, countyfp, sep = ""))
  # SRID
  x %<>% st_transform(4326)
  x$geometry<-st_cast(x$geometry, 'MULTIPOLYGON')
  
  # unique identifier
  x %<>% mutate(uid = str_c(year, state, county)%>% as.numeric())
  
  # order
  x %<>% dplyr::select(uid, hydroid, year, statefp, countyfp,
                      county_geoid, fullname, aland, awater, intptlat, intptlon, geometry)
}
# download a single county dataset for area water
W <- function(overwrite = FALSE){
  w1 <- f_download_reformat_water()
  
  # write to tmp
  # dsn = "PG:host=db dbname=walk_db user=aybloom password=census port=5432"
  
  st_write(obj = w1, dsn = dsn, layer = "area_water", layer_options = c("geometry_name=geom_4326", "SCHEMA=tmp", "OVERWRITE=YES"), delete_layer = TRUE)
  
  # constraint 
  O <- dbGetQuery(conn = U, statement = "alter table tmp.area_water drop constraint if exists area_water_pkey;")
  O <- dbGetQuery(conn = U, statement = "ALTER TABLE tmp.area_water ADD CONSTRAINT cnst_year_county_goeid_hydroid unique(year, statefp, countyfp, hydroid);")
  O <- dbGetQuery(conn = U, statement = "ALTER TABLE tmp.area_water alter column uid set not null;")
  
  # table exists?
  #if(tExists(conn = U, table_schema = "tract", table_name = "tract")){
  if(dbExistsTable(U, Id(schema = 'area_water', table = 'area_water'))){
    if(!overwrite){
      message("area_water.area_water exists. consider running with overwrite = TRUE")
      return(invisible())
    }
    
  }
  # create partitioned table by year
  sql1 <- "drop table if exists area_water.area_water cascade;"
  O <- dbGetQuery(conn = U, statement = sql1)
  sql2 <- "create table if not exists area_water.area_water (like tmp.area_water including all) partition by list(year);"
  O <- dbGetQuery(conn = U, statement = sql2)
  sql3 <- "create index water_uid on area_water.area_water using btree(uid);"
  O <- dbGetQuery(conn = U, statement = sql3)
}

# create the water schema
f_water_create_schema <- function(overwrite = FALSE) {
  # drop the schema if overwriting
  if (overwrite) {
    O <- dbGetQuery(
      conn = U,
      statement = "drop schema if exists area_water cascade;"    )
  }
  schemas <- c("area_water", "area_water_state", "area_water_county")
  
  for (s in schemas) {
    dbExecute(U, paste0("CREATE SCHEMA IF NOT EXISTS ", s, ";"))
  }
  
  
  # base table
  # create partitioned table by year (also separated, can't handle multiple schemas)
  sql1 <- "create table if not exists area_water.area_water (like tmp.area_water) partition by list(year);"
  sql2<-  "create index idx_area_water_tract_year_state_county on area_water.area_water using btree(year, statefp, countyfp);"
  O <- dbGetQuery(conn = U, statement = sql1)
  O <- dbGetQuery(conn = U, statement = sql2)
}

# create year and year-state partitions
f_water_partitions_year <- function(years){
  for (YR in years) {
    if(YR < 2011){
      YR = 2011
    }
    # each year's table, partitioned by state
    sql2 <- "create table if not exists area_water.area_water_xYRx partition of area_water.area_water for values in (xYRx) partition by list(statefp);" %>%
      str_replace_all(pattern = "xYRx", replacement = YR %>% as.character())
    message(sql2)
    O <- dbGetQuery(conn = U, statement = sql2)
  }
}

# create year-state partitions
f_water_partitions_year_state <- function(years, states_fips){
  for (YR in years) {
    if(YR < 2011){
      YR = 2011
    }
    # create each year-state table
    for (ST in states_fips) {
      sql3 <- "create table if not exists area_water_state.area_water_xYRx_xSTx partition of area_water.area_water_xYRx for values in ('xSTx') partition by list(countyfp);" %>%
        str_replace_all(pattern = "xYRx", replacement = YR %>% as.character()) %>%
        str_replace_all(pattern = "xSTx", ST)
      message(sql3)
      O <- dbGetQuery(conn = U, statement = sql3)
    }
  }
}

# create year-state-couty tables
f_water_partitions_year_state_county <- function(years, counties_fips){
  for (YR in years) {
    if(YR < 2011){
      YR = 2011
    }
    # for each yer-state-county
    for (CTY in 1:nrow(counties_fips)) {
      #        for(CTY in 1){
      state_fips <- counties_fips$state_code[CTY]
      county_fips <- counties_fips$county_code[CTY]
      # message(paste(YR, ST, state_fips, county_fips))
      sql4 <- "create table if not exists area_water_county.area_water_xYRx_xSTx_xCTx partition of area_water_state.area_water_xYRx_xSTx for values in ('xCTx');" %>%
        str_replace_all(pattern = "xYRx", replacement = YR %>% as.character()) %>%
        str_replace_all(pattern = "xSTx", state_fips) %>%
        str_replace_all(pattern = "xCTx", county_fips)
      message(sql4)
      O <- dbGetQuery(conn = U, statement = sql4)
    }
  }
}

f_water_create_partitions <- function(years, 
                                      states_fips, 
                                      counties_fips){
  
  f_water_partitions_year(years = years)
  f_water_partitions_year_state(years = years, states_fips)
  f_water_partitions_year_state_county(years = years, counties_fips)
}

# area_water for one year-county
f_water_getdata_0one <- function(year = 2020, state = counties_fips$state_code[1], county = counties_fips$county_code[1], verbose = TRUE){
  t0 <- Sys.time()
  if(verbose){
    message(paste0("    year = ", year, ", state = '", state, "', county = '", county, "'"))
  }
  # get a data set
  x <- f_download_reformat_water(year, state, county) %>% 
    mutate(intptlat = as.numeric(intptlat), 
           intptlon = as.numeric(intptlon))
  
  # write to tmp
  if(verbose){
    message("    ... writing tmp file")
  }
  
  st_write(obj = x, dsn = dsn, layer = "area_water", layer_options = c("geometry_name=geom_4326", "SCHEMA=tmp", "OVERWRITE=YES"), append=FALSE)
  
  # upsert
  if(verbose){
    message("    ... upsert")
  }

  O <- tryCatch({
    dbExecute(
    conn = U,
    statement = "
    insert into area_water.area_water
    (ogc_fid, uid, hydroid, year, statefp, countyfp, county_geoid, fullname, aland, awater, intptlat, intptlon, geom_4326)
    select ogc_fid, uid, hydroid, year, statefp, countyfp, county_geoid, fullname, aland, awater,
           intptlat::DOUBLE PRECISION,
           intptlon::DOUBLE PRECISION,
           geom_4326
    from tmp.area_water;"
  ) 
    message("Insert successful")
  }, error = function(e) {
    if (grepl("duplicate key value violates unique constraint", e$message)) {
      message("Duplicate key detected — skipping insert.")
    } else {
      stop(e)  # rethrow if it's some other error
    }
  })

  # # drop idles in db activity
  O <- dbGetQuery(conn = U, statement = "SELECT pg_terminate_backend(pid) from (select pid, state from pg_stat_activity where state ~ 'idle' and query ~ '^SELECT') as foo;")
  
  # timing
  if(verbose){
    message(paste0("    ... ", format(Sys.time() - t0)))
  }
}

#year = 2016; state = '02'; county = '063'

f_water_getdata_1all <- function(startrun = 1, 
                                 years = c(2006, 2016), 
                                 state_abbr = c('wa', 'mn'),
                                 vrb = TRUE){
  # timing and iteration
  t0 <- Sys.time()
  idx <- 0
  
  #get all counties for all states
  counties_fips <- fips_codes %>% dplyr::filter(tolower(state) %in% state_abbr)
  
  # construct a data frame of years x counties
  control.df <- data.frame(year = years) %>% 
    crossing(counties_fips) %>% 
    arrange(year, state_code, county_code)
  
  
  # for each year-state-county
  for (i in startrun:nrow(control.df)) {
    vrb<-vrb
    idx <- idx + 1
    message(paste(i, "of",  nrow(control.df)))
    # year
    YR <- control.df$year[i]
    # state and county
    state_fips <- control.df$state_code[i]
    county_fips <- control.df$county_code[i]
    
    ## check for existing data in the db
    query<-sprintf("
    SELECT county_geoid
     FROM area_water.area_water
    WHERE statefp = '%s'
    AND countyfp = '%s'
    AND year = %s
", state_fips, county_fips, YR)
    
    if(nrow(dbGetQuery(U, query))>0) {
      print(paste('Data for state:', state_fips, 'county:', county_fips, 'and year:', YR, 'already in table'))
      next}
    # upsert
    tryCatch(
      {
        f_water_getdata_0one(year = YR, state = state_fips, county = county_fips, verbose = vrb)
      },
      error = function(e) {
        message("Error fetching data for year ", YR, 
                ", state ", state_fips, 
                ", county ", county_fips, ": ", e$message)
        return(NULL)  # or any default/fallback value
      }
    ) 
    
    # elapsed
    elapsed <- difftime(Sys.time(), t0, units = "secs") %>% as.numeric() %>% round(2)
    # time per run
    t.run.s <- (elapsed / idx) %>% round(2)
    # estimated time to completion
    total.hours <- (t.run.s * (nrow(control.df) - i) / 3600) %>%  round(2)
    if(vrb){
      message(paste("    Run:", idx, "; elapsed s:", elapsed, "; s per run:", t.run.s, "; completion:", total.hours, "h"))
    }
  }
}


