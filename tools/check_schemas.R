### If createing a database from scratch -- start here
library(RPostgres)
library(DBI)
library(leaflet)
library('glue')
library(sf)
library(plyr)
library(tidyr)
library(tibble)

## setup and connect to database ####
### connect to the database
setwd('H:/projects/beh_nwi_smartmap')

source("dbconnect.R")
U <- us_census <- connectdb()

# set dsn
dsn = "PG:host=doyenne.csde.washington.edu dbname=nwi_smartmap user=aybloom password=csde4322 port=5432" 

check_for_schemas <- function(){
 schemas <- dbGetQuery(U, 'SELECT schema_name FROM information_schema.schemata;')
 tables <- dbGetQuery(U, "SELECT c.relname AS parent_table, 
                      n.nspname AS schema_name
                      FROM pg_class c
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE c.relkind = 'p'
                      ORDER BY schema_name, parent_table;")
 
 if(!('tmp' %in% schemas$schema_name)){
   O <- dbExecute(U, "CREATE SCHEMA IF NOT EXISTS tmp;") 
 }
 
 if('tract' %in% schemas$schema_name & 
    'tract_county' %in% schemas$schema_name & 
    'tract_state' %in% schemas$schema_name){
   message('Tract schemas already exists')
 } else {
   message('Creating tract schemas')
   f_tract_create_schema()
 }
 
 if('area_water' %in% schemas$schema_name & 
    'area_water_county' %in% schemas$schema_name & 
    'area_water_state' %in% schemas$schema_name){
   message('Area water schemas already exists')
 } else {
   message('Creating area water schemas')
   f__create_schema()
 }
 
 if('tract_nowater' %in% schemas$schema_name & 
    'tract_nowater_county' %in% schemas$schema_name & 
    'tract_nowater_state' %in% schemas$schema_name){
   message('Tract no water schemas already exists')
 } else {
   message('Creating tract no water schemas')
   f_nowater_create_schema()
 }
 
 if('tract_variables' %in% schemas$schema_name & 
    'tract_variables_county' %in% schemas$schema_name & 
    'tract_variables_state' %in% schemas$schema_name){
   message('Tract variables schemas already exist')
 } else {
   message('Creating tract variables (population) schemas')
   f_pop_create_schemas()
 }
 
 if('edges' %in% schemas$schema_name & 
    'edges_county' %in% schemas$schema_name & 
    'edges_state' %in% schemas$schema_name){
   message('Edges schemas already exist')
 } else {
   message('Creating edges (intersections) schemas')
   f_edges_create_schemas()
 }
 
 
}
get_db_list_states_years <- function(conn = U){
  
  tract_states_years_df <- dbGetQuery(conn, 
                                      'SELECT DISTINCT year,statefp FROM tract.tract;') %>%
    mutate(table = 'tract')
  water_states_years_df <- dbGetQuery(conn, 
                                      'SELECT DISTINCT year, statefp FROM area_water.area_water;') %>%
    mutate(table = 'area_water')
  
  tract_nowater_states_year <-  dbGetQuery(conn, 
                                           'SELECT DISTINCT year, statefp FROM tract_nowater.tract_nowater;') %>% 
    mutate(table = 'tract_nowater')
  
  tract_variables_states_year <-  dbGetQuery(conn, 
                                           'SELECT DISTINCT year, statefp FROM tract_variables.tract_variables;') %>% 
    mutate(table = 'tract_variables')
  
  edges_states_year <-  dbGetQuery(conn, 
                                    'SELECT DISTINCT year, statefp FROM edges.edge;') %>% 
    mutate(table = 'edges')
  
  tbl_list <- plyr::rbind.fill(tract_states_years_df, water_states_years_df, tract_nowater_states_year, tract_variables_states_year, edges_states_year)
  
  tbl_list_w <- tbl_list %>% 
    mutate(n = 1) %>% 
    pivot_wider(
    names_from = table, 
    values_from = n
  )
  
  print(tbl_list_w)
  
}
check_raster_files <- function(){
  
  raster_files_tbl <- data.frame(raster_file = NA, 
                                 year = NA, 
                                 state = NA)
  
  # land area ####
  land_area_filepath <- 'H:/projects/beh_nwi_smartmap/data/state_raster/land_area'
  years <- list.files(land_area_filepath)
  
  for(YR in years){
    
    states_fp <- list.files(glue('{land_area_filepath}/{YR}/ha'))
   
    state_fips <- sapply(states_fp, function(x)
      sub(".*_(\\d+)\\.tif$", "\\1", x)
    )
    
    for(ST in state_fips){
    raster_files_tbl <- raster_files_tbl %>% 
      add_row(raster_file = 'land area', 
              year = YR, 
              state = ST)
    }
  }
    
  # population density ####
    pop_density_filepath <- 'H:/projects/beh_nwi_smartmap/data/state_raster/population_density'
    years <- list.files(pop_density_filepath)
    
    for(YR in years){
      
      states_fp <- list.files(glue('{pop_density_filepath}/{YR}/ha'))
      
      state_fips <- sapply(states_fp, function(x)
        sub(".*_(\\d+)\\.tif$", "\\1", x)
      )
      
      for(ST in state_fips){
        raster_files_tbl <- raster_files_tbl %>% 
          add_row(raster_file = 'population density', 
                  year = YR, 
                  state = ST)
      }
    }
    
  # intersection density ####
    intersection_density_filepath <- 'H:/projects/beh_nwi_smartmap/data/state_raster/intersections'
    years <- list.files(intersection_density_filepath)
    
    for(YR in years){
      
      states_fp <- list.files(glue('{intersection_density_filepath}/{YR}/ha'))
      
      state_fips <- sapply(states_fp, function(x)
        sub(".*_(\\d+)\\.tif$", "\\1", x)
      )
      
      for(ST in state_fips){
        raster_files_tbl <- raster_files_tbl %>% 
          add_row(raster_file = 'intersection density', 
                  year = YR, 
                  state = ST)
      }
    }
  
  # business density ####
    business_density_filepath <- 'H:/projects/beh_nwi_smartmap/data/state_raster/business'
    years <- list.files(business_density_filepath)
    
    for(YR in years){
      
      if(nchar(YR)>4){next}
      
      states_ha <- list.files(glue('{business_density_filepath}/{YR}/ha'))
      states_sum <- list.files(glue('{business_density_filepath}/{YR}/sum'))
      
      state_fips_ha <- sapply(states_ha, function(x)
        sub(".*_(\\d+)\\.tif$", "\\1", x)
      )
      
      state_fips_sum <- sapply(states_sum, function(x)
        sub(".*_(\\d+)\\.tif$", "\\1", x)
      )
      
      for(ST in state_fips_ha){
        raster_files_tbl <- raster_files_tbl %>% 
          add_row(raster_file = 'business density', 
                  year = YR, 
                  state = ST)
      }
      
      # for(ST in state_fips_sum){
      #   raster_files_tbl <- raster_files_tbl %>% 
      #     add_row(raster_file = 'business density sum', 
      #             year = YR, 
      #             state = ST)
      # }
    }
    
    ## get list of raster sum files for businesses
    raster_files_tbl <- raster_files_tbl  %>% dplyr::filter(!is.na(year)) %>%
      arrange(state, year)
    
    return(raster_files_tbl)
    
}
