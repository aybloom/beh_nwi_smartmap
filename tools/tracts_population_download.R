##download tract population
# Phil Hurvitz phurvitz@uw.edu 2022-09-19

library(tidyverse)
library(tigris)
library(magrittr)
library(tidyr)
library(tidycensus)

### UPDATE -- add library AY
#source("dbconnect.R")
U <- us_census <- connectdb()
### UPDATE -- add docker connection to db AY

# load variables for selecting
#l_sf1_2000 <- load_variables(year = 2000, dataset = "sf1")
#l_sf3_2000 <- load_variables(year = 2000, dataset = "sf3")
#l_acs5_2010 <- load_variables(2010, "acs5")

f_create_tract_var_table <- function(years = years, state_abbr_list = state_abbr_list){
  
  # year-state-county
  counties_fips <- fips_codes %>%
    dplyr::filter(state %in% toupper(state_abbr_list)) %>%
    crossing(years)
  
  # set counties, states, years
  # tracts-years-states
  counties_df <- counties_fips %>%
    dplyr::select(years, state_code, county_code) %>%
    distinct() %>%
    mutate(tablename = str_c("tract_variables_county.tract_variables", years, state_code, county_code, sep = "_")) %>%
    # drop alaska
    filter(state_code != "02") %>%
    arrange(state_code, years, county_code) %>%
    mutate(rn = 1:nrow(.)) %>%
    dplyr::select(rn, everything())
  
  return(counties_df)
}

# function to get tracts and add year
f_tract_download_variables <- function(year = 2010, state_fips = "02", county_fips = "020", verbose = FALSE) {
  ERR <- ERR2 <- ERR3 <- FALSE
  if (verbose) {
    message(paste("   getting data for", year, state_fips, county_fips))
  }
  if (ERR) {
    ERR2 <- FALSE
    tryCatch(
      expr = {
        v <- c(
          totalpop = "P006001",
          white = "P006003",
          black = "P006004",
          aian = "P006005",
          asian = "P006006",
          nhpi = "P006007"
        )
        Y <- get_decennial(
          geography = "tract",
          variables = v,
          state = state_fips,
          county = county_fips,
          year = year,
          output = "wide",
          geometry = FALSE,
          show_call = TRUE,
          sumfile = "sf3"
        )
      },
      error = function(e) {
        ERR2 <<- TRUE
        message("FAIL 2?")
      },
      warning = function(w) {
        message("warning")
      },
      finally = {
        message("    2, done trying to download")
      }
    )
  }
  
  # if (year == 2024) {
  #   acs_year = 2023 # placeholder until 2024 data is released
  #} else 
 if (year >= 2007 & year <= 2022) {
    acs_year = (year + 2)
  } else {acs_year = year}
    message(paste("   GETTING DATA for", year, " using acs year ", acs_year))
    ERR3 <- FALSE
    tryCatch(
      expr = {        
        # ACS variables
        v <- c(
          totalpop = "B02001_001",
          white = "B02001_002",
          black = "B02001_003",
          aian = "B02001_004",
          asian = "B02001_005",
          nhpi = "B02001_006"
        )
        # this downloads a single county
        Y <- get_acs(
          geography = "tract",
          variables = v,
          state = state_fips,
          county = county_fips,
          year = as.numeric(acs_year),
          output = "wide",
          geometry = FALSE,
          show_call = TRUE
        ) %>%
          dplyr::select(-ends_with("M"))
        
        all_acs <- get_acs(
          geography = "tract",
          variables = v,
          state = state_fips,
          county = county_fips,
          year = as.numeric(acs_year),
          output = "wide",
          geometry = FALSE,
          show_call = TRUE
        )
        
      },
      error = function(e) {
        ERR3 <<- TRUE
        message("    FAILED to get data!")
      },
      warning = function(w) {
        message("warning")
      }
    )
  
  # if we failed, ERR2 = TRUE
  if (ERR2) {
    message("    no data seem to be there")
    return(invisible())
  }
  
  if (ERR3) {
    message("    no data seem to be there or elsewhere")
    return(invisible())
  }    
  
  # columns
  colnames(Y) %<>%
    str_replace_all("E$", "")
  colnames(Y) %<>%
    str_replace(pattern = "NAM", replacement = "name")    
  # add the year column
  Y %<>% mutate(year = year)
  # lowercase column names
  colnames(Y) %<>% str_to_lower()
  # integerize
  Y %<>% mutate_if(.predicate = is.double, .funs = as.integer)
  # state and county
  Y %<>% mutate(
    statefp = str_sub(string = geoid, start = 1, end = 2),
    countyfp = str_sub(string = geoid, start = 3, end = 5),
    tractce = str_sub(string = geoid, start = 6)
  )
  # reorder
  Y %<>% dplyr::select(year, statefp, countyfp, tractce, geoid, everything())
  # "other"
  Y %<>% mutate(
    other = totalpop - white - black - aian - asian - nhpi
  )
  Y
}
# create schemas
# update: separate commands into separate statements
f_pop_create_schemas <- function() {
  sq1 <- "create schema if not exists tract_variables"
  O <- dbExecute(conn = U, statement = sq1)
  sq2 <- "create schema if not exists tract_variables_state"
  O <- dbExecute(conn = U, statement = sq2)
  sq3 <- "create schema if not exists tract_variables_county"
  O <- dbExecute(conn = U, statement = sq3)

    # O <- dbExecute(conn = U, statement = "
  #       create schema if not exists tract_variables;
  #       create schema if not exists tract_variables_state;
  #       create schema if not exists tract_variables_county;")
}
# template
f_pop_template <- function() {
  # x <- f_tract(year = 2000)
  x <- f_tract_download_variables(year = 2016, state_fips = '53', county_fips = '033', verbose = TRUE)
  O <- dbGetQuery(conn = U, statement = "drop table if exists tmp.tract_variables;")
 # O <- dbWriteTable(conn = U, name = c("tmp", "tract_variables"), value = x, row.names = FALSE)
  dbWriteTable(conn = U, name = Id(schema = 'tmp', table = 'tract_variables'), value = x, row.names = FALSE)

  # constraints
  O <- dbExecute(conn = U, statement = "ALTER TABLE tmp.tract_variables ADD CONSTRAINT cnst_year_geoid_tractvar unique(year, statefp, countyfp, tractce);")
  
  O <- dbExecute(conn = U, statement = "drop table if exists tract_variables.tract_variables;")
  sql <- "create table if not exists tract_variables.tract_variables (like tmp.tract_variables including all) partition by list(year);"
  O <- dbExecute(conn = U, statement = sql)
}
# tracts
f_pop_partitions_year <- function(years) {
  for (i in years) {
    # each year's table, partitioned by state
    sql2 <- "create table if not exists tract_variables.tract_variables_xYRx partition of tract_variables.tract_variables for values in (xYRx) partition by list(statefp);" %>%
      str_replace_all(pattern = "xYRx", replacement = i %>% as.character())
    message(sql2)
    O <- dbGetQuery(conn = U, statement = sql2)
  }
}
# tracts-years-states
f_pop_partitions_year_state <- function(state_abbr_list, year_list) {
  # state-year list data frame
  state_year <- fips_codes %>%
    dplyr::filter(state %in% toupper(state_abbr_list)) %>%
    crossing(years = year_list) %>%
    dplyr::select(years, state_code) %>%
    distinct() %>%
    arrange(years, state_code) %>%
    mutate(tablename = str_c("tract_variables_state.tract_variables", years, state_code, sep = "_"))
  # create each year-state table
  for (i in 1:nrow(state_year)) {
    YR <- state_year$years[i]
    tablename <- state_year$tablename[i]
    state_code <- state_year$state_code[i]
    sql3 <- "create table if not exists xTNx partition of tract_variables.tract_variables_xYRx for values in ('xSTx') partition by list(countyfp);" %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xYRx", replacement = YR %>% as.character()) %>%
      str_replace_all(pattern = "xSTx", state_code)
    message(sql3)
    O <- dbGetQuery(conn = U, statement = sql3)
  }
}
f_pop_partitions_year_state_county <- function(counties_df) {
  # create each year-state table
  for (i in 1:nrow(counties_df)) {
    # for (i in 1) {
    YR <- counties_df$years[i]
    tablename <- counties_df$tablename[i]
    state_code <- counties_df$state_code[i]
    county_code <- counties_df$county_code[i]

    sql4 <- "create table if not exists xTNx partition of tract_variables_state.tract_variables_xYRx_xSTx for values in ('xCTx');" %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xYRx", replacement = YR %>% as.character()) %>%
      str_replace_all(pattern = "xSTx", state_code) %>%
      str_replace_all(pattern = "xCTx", county_code)
    message(sql4)
    O <- dbGetQuery(conn = U, statement = sql4)
  }
}
# wrapper
f_pop_create_partitions <- function(state_abbr_list = state_abbr_list, 
                                    years = years) {
  
  counties_df <- f_create_tract_var_table(state_abbr_list = state_abbr_list, 
                                          years = years)
  f_pop_partitions_year(years)
  f_pop_partitions_year_state(year_list = years, state_abbr_list)
  f_pop_partitions_year_state_county(counties_df)
}

# get tract data for one and push to db
f_pop_tractvar_0one <- function(myYear = 2000, myState_fips = "01", myCounty_fips = "001", vrb = TRUE) {
  # get the data
  #    rm(XX)
  XX <- f_tract_download_variables(year = myYear, state_fips = myState_fips, county_fips = myCounty_fips, verbose = vrb)
  assign("XXY", XX, envir = .GlobalEnv)
  
  # if we did not download, then XX is null, so stop
  if(is.null(XX)) {
    message("    failure, ERROR!")
    return(NULL)
  }
  # upsert
  if (vrb) {
    message("    drop....")
    O <- dbGetQuery(conn = U, statement = "drop table if exists tmp.tractvars;")
  }
  if (vrb) {
    message("    write...")
    #O <- dbWriteTable(conn = U, name = c("tmp", "tractvars"), value = XX, row.names = FALSE)
    O < dbWriteTable(conn = U, name = Id(schema = 'tmp', table = 'tractvars'), value = XX, row.names = FALSE)
    }
  if (vrb) {
    message("    upsert....")
    O <- dbExecute(conn = U, statement = "insert into tract_variables.tract_variables 
                   (year, statefp, countyfp, tractce, geoid, name, totalpop, white, black, aian, asian, nhpi, other) 
                    select 
                    year::integer,
  statefp, countyfp, tractce, geoid, name, totalpop, white, black, aian, asian, nhpi, other
from tmp.tractvars
                   on conflict do nothing;")
    return(XX)
  }
}
# download and push to db tract population variables for all
f_pop_tractvar_1all <- function(startnum = 1, endnum, 
                            years, 
                            state_abbr, 
                            verbose = TRUE) {
  # timing and iteration
  t0 <- Sys.time()
  idx <- 0
  
  # tracts-years-states
  counties_df <- fips_codes %>% dplyr::filter(tolower(state) %in% state_abbr) %>%
    crossing(years) %>%
    dplyr::select(years, state_code, county_code) %>%
    distinct() %>%
    mutate(tablename = str_c("tract_variables_county.tract_variables", years, state_code, county_code, sep = "_")) %>%
  # drop alaska
    filter(state_code != "02") %>%
    arrange(state_code, years, county_code) %>%
    mutate(rn = 1:nrow(.)) %>%
    dplyr::select(rn, everything())
  
  # end num?
  if (missing("endnum")) {
    endnum <- nrow(counties_df)
  }
  # run over all entries in county data frame
  
  for (i in startnum:endnum) {
    idx <- idx + 1
    if (verbose) {
      message(paste(i, "of", endnum))
    }
    # year
    year <- counties_df$years[i]
    state_fips <- counties_df$state_code[i]
    county_fips <- counties_df$county_code[i]
    if (verbose) {
      message(paste0("year: ", year, "; state_fips: ", state_fips, "; county_fips: ", county_fips))
    }
    # run it
    ZXY <- f_pop_tractvar_0one(myYear = year, myState_fips = state_fips, myCounty_fips = county_fips, vrb = verbose)
    if (is.null(ZXY)) {
      next()
    }
    # elapsed
    elapsed <- difftime(Sys.time(), t0, units = "secs") %>%
      as.numeric() %>%
      round(2)
    # time per run
    t.run.s <- (elapsed / idx) %>% round(2)
    # estimated time to completion
    total.hours <- (t.run.s * (nrow(counties_df) - i) / 3600) %>% round(2)
    if (verbose) {
      message(paste("    Run:", idx, "; elapsed s:", elapsed, "; s per run:", t.run.s, "; completion:", total.hours, "h"))
    }
  }
}

