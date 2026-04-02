# download tracts for many years over entire US into partitioned tables
# Phil Hurvitz phurvitz@uw.edu 2022-09-19

library(tidyverse)
library(magrittr)
library(tidycensus)
library(tigris)
library(sf)
library(tidyr)
library(dplyr)
library(glue)
library(DBI)

# tigris
options(tigris_use_cache = TRUE)
tcachedir <- "gisdata/us_census/tract"
# set cache dir if necessary
if(Sys.getenv("TIGRIS_CACHE_DIR") != tcachedir){
  tigris_cache_dir(path = tcachedir)
  readRenviron('~/.Renviron')
}

f_download_reformat_tract <- function(year = 2020, state, 
                                      county_fips){
  print(year)
  print(state)
  print(county_fips)
  # get data
  if(year == 2007){
    
    state_county <-fips_codes %>% dplyr::select(state_code, state_name, county_code, county) %>% 
      dplyr::filter(state_code == as.character(state) & county_code == county_fips) %>% unique()
    state_name_caps <- toupper(unique(state_county$state_name))
    county_fips <- state_county$county_code
    county_name <- str_replace_all(state_county$county, "\\s+", "_")
    # remove '.'
    county_name <- str_replace_all(county_name, "\\.", "")
    county_name_br <- str_replace(county_name, '_County$', '')
    
    message(glue('... Downloading data for {state_name_caps}, {county_name}'))
    tract_url <- glue('https://www2.census.gov/geo/tiger/TIGER2007FE/{state}_{state_name_caps}/{state}{county_fips}_{county_name}/fe_2007_{state}{county_fips}_tract00.zip')
    
   d <-  tryCatch({
      download.file(url = tract_url, 
                  destfile = glue('gisdata/us_census/tract/tl_2007_{state}{county_fips}_tract.zip') 
                  )
    }, error = function(e) {
      message("Error fetching data", e$message, '| ...Trying another url....')
      return(NULL)  # or any default/fallback value
    })
    if(is.null(d)){
      tract_url <- glue('https://www2.census.gov/geo/tiger/TIGER2007FE/{state}_{state_name_caps}/{state}{county_fips}_{county_name_br}/fe_2007_{state}{county_fips}_tract00.zip')
      d <- download.file(url = tract_url, 
                         destfile = glue('gisdata/us_census/tract/tl_2007_{state}{county_fips}_tract.zip') )
     
    }
    
    zip_tract_file <- glue('gisdata/us_census/tract/tl_2007_{state}{county_fips}_tract.zip')
    message(zip_tract_file)
    
    unzip(zipfile = zip_tract_file, 
          exdir = 'gisdata/us_census/tract/'
          )
    x <- st_read(glue('gisdata/us_census/tract/fe_2007_{state}{county_fips}_tract00.shp'))
    
    x <- x %>% 
      mutate(aland = as.numeric(''), 
             awater = as.numeric(''), 
             intptlat = NA, 
             intptlon = NA
             )
  }
  else if(year >=2010){
  x <- tracts(state = state, county = county_fips, cb = FALSE, year = year) 
  }
  # decade
  decade <- as.numeric(year) - 2000
  
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
  # geoid
  x %<>% mutate(geoid = str_c(statefp, countyfp, tractce, sep = ""))
  # SRID
  x %<>% st_transform(4326)
  
  # unique identifier
  x %<>% mutate(uid = str_c(year, statefp, countyfp) %>% as.numeric())

  # order
  x %<>% dplyr::select(any_of(c('uid', 'year', 'statefp', 'countyfp', 'tractce', 'geoid', 'name', 'namelsad', 'mtfcc', 'funcstat', 'aland', 'awater', 'intptlat', 'intptlon', 'geometry')))
}
T <- function(overwrite = FALSE) {
  # download one county data
  t1 <- f_download_reformat_tract()
  
  st_write(obj = t1, dsn = dsn, layer = "tract", layer_options = c("geometry_name=geom_4326", "SCHEMA=tmp", "OVERWRITE=YES"), delete_layer = TRUE)
  
  # constraint 
  O <- dbGetQuery(conn = U, statement = "alter table tmp.tract drop constraint if exists tract_pkey;")
  O <- dbGetQuery(conn = U, statement = "ALTER TABLE tmp.tract ADD CONSTRAINT cnst_year_geoid unique(year, statefp, countyfp, tractce);")
  O <- dbGetQuery(conn = U, statement = "ALTER TABLE tmp.tract alter column uid set not null;")
  
  # table exists?
  #if(tExists(conn = U, table_schema = "tract", table_name = "tract")){
  if(dbExistsTable(U, Id(schema = 'tract', table = 'tract'))){
    if(!overwrite){
      message("tract.tract exists. consider running with overwrite = TRUE")
      return(invisible())
    }
  }
  # create partitioned table by year
  sql1 <- "drop table if exists tract.tract cascade;"
  O <- dbGetQuery(conn = U, statement = sql1)
  sql2 <- "create table if not exists tract.tract (like tmp.tract including all) partition by list(year);"
  O <- dbGetQuery(conn = U, statement = sql2)
  sql3 <- "create index tract_uid on tract.tract using btree(uid);"
  O <- dbGetQuery(conn = U, statement = sql3)
}
# create the tract schema
f_tract_create_schema <- function(overwrite = FALSE) {
  # drop the schema if overwriting
  if (overwrite) {
    O <- dbGetQuery(
      conn = U,
      statement = "drop schema if exists tract cascade;"    )
  }

  schemas <- c("tract", "tract_state", "tract_county", "tmp")
  
  for (s in schemas) {
    dbExecute(U, paste0("CREATE SCHEMA IF NOT EXISTS ", s, ";"))
  }
  
  
  # base table
  # create partitioned table by year (also separated, can't handle multiple schemas)
  sql1 <- "create table if not exists tract.tract (like tmp.tract) partition by list(year);"
  sql2<-  "create index idx_tract_tract_year_state_county on tract.tract using btree(year, statefp, countyfp);"
  O <- dbGetQuery(conn = U, statement = sql1)
  O <- dbGetQuery(conn = U, statement = sql2)
}
f_tract_create_partitions <- function(years, 
                                      states_fips, 
                                      counties_fips){
  f_tract_partitions_year(years = years)
  f_tract_partitions_year_state(years = years, states_fips = states_fips)
  f_tract_partitions_year_state_county(years= years, counties_fips = counties_fips)
}
# create year and year-state partitions
f_tract_partitions_year <- function(years = years){
  for (YR in years) {
    # each year's table, partitioned by state
    sql2 <- "create table if not exists tract.tract_xYRx partition of tract.tract for values in (xYRx) partition by list(statefp);" %>%
      str_replace_all(pattern = "xYRx", replacement = YR %>% as.character())
    message(sql2)
    O <- dbGetQuery(conn = U, statement = sql2)
  }
}
# create year-state partitions
f_tract_partitions_year_state <- function(years = years, 
                                          states_fips){
  for (YR in years) {
    # create each year-state table
    for (ST in states_fips) {
      sql3 <- "create table if not exists tract_state.tract_xYRx_xSTx partition of tract.tract_xYRx for values in ('xSTx') partition by list(countyfp);" %>%
        str_replace_all(pattern = "xYRx", replacement = YR %>% as.character()) %>%
        str_replace_all(pattern = "xSTx", ST)
      message(sql3)
      O <- dbGetQuery(conn = U, statement = sql3)
    }
  }
}
# create year-state-couty tables
f_tract_partitions_year_state_county <- function(years = years, 
                                                 counties_fips = counties_fips){
  for (YR in years) {
    # for each yer-state-county
    for (CTY in 1:nrow(counties_fips)) {
      #        for(CTY in 1){
      state_fips <- counties_fips$state_code[CTY]
      county_fips <- counties_fips$county_code[CTY]
      # message(paste(YR, ST, state_fips, county_fips))
      sql4 <- "create table if not exists tract_county.tract_xYRx_xSTx_xCTx partition of tract_state.tract_xYRx_xSTx for values in ('xCTx');" %>%
        str_replace_all(pattern = "xYRx", replacement = YR %>% as.character()) %>%
        str_replace_all(pattern = "xSTx", state_fips) %>%
        str_replace_all(pattern = "xCTx", county_fips)
      message(sql4)
      O <- dbGetQuery(conn = U, statement = sql4)
    }
  }
}
# data for one year-county
f_tract_getdata_0one <- function(year = 2020, state = counties_fips$state_code[1], 
                                 county = counties_fips$county_code[1], verbose = TRUE){
  t0 <- Sys.time()
  if(verbose){
    message(paste0("    year = ", year, ", state = '", state, "', county = '", county, "'"))
  }
  # get a data set
  x <- f_download_reformat_tract(year, state, county_fips = county) 
  
  if('intptlat' %in% colnames(x))
  {x<-x %>% 
    mutate(intptlat = as.numeric(intptlat), 
           intptlon = as.numeric(intptlon))
  }
  
  ## write temp file to tmp.tct in the db
  dbExecute(U, "DROP TABLE IF EXISTS tmp.tct;")
  st_write(obj = x, dsn = dsn, layer = "tct", layer_options = c("geometry_name=geom_4326", "SCHEMA=tmp", "OVERWRITE=YES"),
           delete_layer = TRUE)

  # column name -- can skip already named ogc_fid
  #  O <- dbExecute(conn = U, statement = "alter table tmp.tct rename column fid to ogc_fid;")
  # upsert
  if(verbose){
    message("    ... upsert")
  }

  O <- tryCatch({
          dbExecute(
    conn = U,
    statement = "
    insert into tract.tract
    (ogc_fid, uid, year, statefp, countyfp, tractce, geoid, name, namelsad,
     mtfcc, funcstat,
     aland, 
     awater, intptlat, intptlon, geom_4326)
    select ogc_fid, uid, year, statefp, countyfp, tractce, geoid, name, namelsad,
           mtfcc, funcstat, 
           aland::DOUBLE PRECISION, 
           awater::DOUBLE PRECISION,
           intptlat::DOUBLE PRECISION,
           intptlon::DOUBLE PRECISION,
           geom_4326
    from tmp.tct;"
  )  
    message("Insert successful")
  }, error = function(e) {
    if(grepl('duplicate key value violates unique constraint', e$message)) {
      message("Duplicate key detected -- skipping insert.")
  } else{
      stop(e)
  }
})

  O <- dbGetQuery(conn = U, statement = "SELECT pg_terminate_backend(pid) from (select pid, state from pg_stat_activity where state ~ 'idle' and query ~ '^SELECT') as foo;")
  
  # timing
  if(verbose){
    message(paste0("    ... ", format(Sys.time() - t0)))
  }
}
# loop to run for all year-countyvrb in the counties_fips dataframe
f_tract_getdata_1all <- function(startrun = 1, 
                                 years = c(2006, 2016), 
                                 state_abbr = c('wa', 'mn'),
                                 vrb = TRUE){
  
  message(glue('.....Downloading data for {state_abbr} for years {years}'))
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
    SELECT geoid
     FROM tract.tract
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
        f_tract_getdata_0one(year = YR, state = state_fips, county = county_fips, verbose = vrb)
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
