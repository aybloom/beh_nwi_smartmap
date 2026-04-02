### connect to the database
wd <- wd
getwd()
source(glue("{wd}/dbconnect.R"))
# setup functions
source(glue('{wd}/tools/walkdb_setup.R'))
con_str = "PG:host=doyenne.csde.washington.edu dbname=nwi_smartmap user=aybloom password=csde4322 port=5432" 

## set list of years & states 
 year_list <- c(2007, 2016, 2024)
 state_abbr_list = c('wa')
# ** Notes: this assumes that schema are already present. 
# ** If building database from scratch, see setup_download.R file
f_setup_database <- function(conn = U, year_list = year_list, 
                             state_abbr_list = state_abbr_list){
  
  # get state fips
  states_fips <- unique(fips_codes$state_code[tolower(fips_codes$state) %in% state_abbr_list])
  counties_fips <- fips_codes %>% dplyr::filter(state_code %in% states_fips)
  
  #years = year_list
  
  f_tract_create_partitions(years = year_list, 
                            states_fips = states_fips, 
                            counties_fips = counties_fips)
  
  f_water_create_partitions(years = year_list, 
                             states_fips = states_fips, 
                             counties_fips = counties_fips)
  
  f_nowater_create_partitions(years = year_list, 
                              state_abbr_list = state_abbr_list)
  
  f_pop_create_partitions(state_abbr_list = state_abbr_list,
                          years = year_list 
                          )
  f_update_nowater_columns()

  f_edges_create_partitions(years = year_list, 
                            state_abbr_list = state_abbr_list
                            )
  
  f_create_validation_schema()
}

f_add_data_smartwalk_db <- function(conn = U, years = year_list, 
                                    state_abbr_list = state_abbr_list){
  message('...downloading tract data...')
  ## download tracts, water, erase water
  f_tract_getdata_1all(startrun = 1, 
                       years = years,
                       state_abbr = state_abbr_list, 
                       vrb = TRUE
            )
  message('...downloading water data...')
  f_water_getdata_1all(years = years, 
                       state_abbr = state_abbr_list)
  
  message('.... erasing water from tract data...')
  f_nowater_erase_1all(years = years, 
                       state_abbr = state_abbr_list
                       )
  # change download timeout for census downloads
  options(timeout = 400)
  
  message('... downloading tract variables (population)...')
  f_pop_tractvar_1all(years = years, 
                  state_abbr = state_abbr_list)
  
  message('... merge population with no water tract data...')
  f_merge_population_tract_nowater_1all(startnum = 142,
                                        replace = TRUE,
                                        resolution_m = 100, 
                                        years = years,
                                        state_abbr_list = state_abbr_list)
  # edges / intersections
  message('... download edges data...')
  f_download_edges(state_abbr_list = state_abbr_list, years = years, govt_shutdown = FALSE)
  
 
  
  for(year in year_list){
    for(state_abbr in state_abbr_list){
      message('... upsert edges data... ')
      f_upsert_shapefiles(years = year, conn = U, state_abbr_list = state_abbr)
      
      message(glue('... building endpoints and intersections for {state_abbr} year {year}'))
      build_endpoints(conn = U, year = year, state_abbr = state_abbr)
      build_degrees(conn = U, year = year, state_abbr = state_abbr)
      build_intersections(conn = U, state_abbr = state_abbr, year = year)
      
    }  
  }
  }

f_create_raster_files <- function(conn = U, 
                                  years = year_list, 
                                  state_abbr_list = state_abbr_list, 
                                  con_str = con_str){
  #message(glue('... rasterzing population data for states: {state_abbr_list} and years: {years}'))
  # rasterize population variable data
  f_tract_var_rasterize_1all(state_abbr_list=state_abbr_list, years = years, 
                             resolution_m = 100)
  # combine rasters across each state
  message('... combining state-level rasters...')
  state_fips <- fips_codes %>% filter(state %in% toupper(state_abbr_list))
  # get pop density
  f_pop_focus_processing(years = years,
                         state_abbr_list = state_abbr_list)
  
  for(state_fips_code in unique(state_fips$state_code)){
    for(yr in years){
      message(glue('... combining tract-level raster files into state-level raster files for state: {state_fips_code} for year: {yr}'))
      f_land_focus_processing(year = yr,
                              state_code = state_fips_code)
      
      
      message(glue('... rasterizing intersection data for {state_fips_code} for year {yr}'))
      
      # rasterize intersections data
      f_rasterize_intersections_0one(con_str = con_str, 
                                    conn = U, 
                                    state_fips = state_fips_code, year = yr, 
                                    resolution_m = 100, verbose = TRUE, 
                                    state_filepath = 'data/state_raster/intersections')
        
      f_get_intersection_density(con_str = con_str, 
                                 conn = U, 
                                 state_fips = state_fips_code, year = yr, 
                                 verbose = TRUE)
      # business_sum_filepath <- glue('H:/projects/beh_nwi_smartmap/data/state_raster/business/{yr}/sum/state_raster_business_density_{yr}_{state_fips_code}.tif')
      # if(file.exists(business_sum_filepath)){
      # f_get_business_density(state_fips = state_fips_code,
      #                        year = yr,
      #                        verbose = TRUE)
      # } else if(!file.exists(business_sum_filepath)){
      #  message(glue('.... need to extract business data for {year}, for {state} from the Data Enclave'))
      # }
     
      
    }
  }
}

f_create_walk_composite_raster_files <- function(conn = U, 
                                                 years = year_list, 
                                                 state_abbr_list = state_abbr_list, 
                                                 con_str = con_str){
  for(yr in years){
    for(s in state_abbr_list){
      state_fips <-unique(fips_codes$state_code[fips_codes$state ==toupper(s)])
      
      message(glue('.... creating composite measure for {yr} in {s}'))
      f_create_composite_measure_raster(yr = yr, 
                                        state_fips = state_fips)
      
    }
  }
                                                   
                                                   
                                                 }

## check tracts, water, and nowater tracts
f_check_tracts_nowater_db <- function(conn = U, 
                                      years = year_list, 
                                      state_abbr_list = state_abbr_list){
  # get state fips
  states_fips <- unique(fips_codes$state_code[tolower(fips_codes$state) %in% state_abbr_list])
  
  for(YR in years){
  
    for(st_fp in states_fips){
  nowater_tbl <- st_read(dsn, query = glue('SELECT * FROM tract_nowater_state.tract_nowater_{YR}_{st_fp}'))
  
    }
  }
  
}

## Pull Smartmap measures for list of points.
f_Smartmap_validation <- function(con_str = dsn, conn = U, 
validation_points_filepath = 'H:/projects/beh_nwi_smartmap/validation_points.csv', 
year = 2016, 
state_fips = '53'){
  f_create_validation_points_df(con_str = con_str, 
                                conn = conn, 
                                validation_points_filepath = validation_points_filepath)
  
  f_create_validation_buffers(conn = conn)
  f_validation_land_area(conn = conn, 
                         year = 2016, state_fips = '53')
  
  # get walk index values for validation points: 
  smartwalk_results <- f_validate_smartwalk(dsn = con_str, year = year, state_fips = state_fips)
  
  return(smartwalk_results)
}

## Create smartmap validation points table
f_Smartmap_validation <- function(con_str = dsn, conn = U, 
                                  validation_points_filepath = 'H:/projects/beh_nwi_smartmap/validation_points.csv', 
                                  year = 2016, 
                                  state_fips = '53'){
  f_create_validation_points_df(con_str = con_str, 
                                conn = conn, 
                                validation_points_filepath = validation_points_filepath)
  
  f_create_validation_buffers(conn = conn)
  f_validation_land_area(conn = conn, 
                         year = 2016, state_fips = '53')
  
  # get walk index values for validation points: 
  smartwalk_results <- f_validate_smartwalk(dsn = con_str, year = year, state_fips = state_fips)
  
  return(smartwalk_results)
}
