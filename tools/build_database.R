setwd('H:/projects/beh_nwi_smartmap')
source('create_walkability_measures.R')
source('check_schemas.R')

# connect to database & set dsn
U <- us_census <- connectdb()
dsn = "PG:host=doyenne.csde.washington.edu dbname=nwi_smartmap user=aybloom password=csde4322 port=5432" 

### 1. Check existing schemas and database data #####
# check schemas & create missing schemas
check_for_schemas()
# list database years & states
get_db_list_states_years()
# list raster file years & states
check_raster_files()

### 2. Add states / years of data to database ####
year_list <- c(2007, 2016, 2014)
state_abbr_list <- c('wa')

f_setup_database(conn = U, 
                 year_list = year_list, 
                 state_abbr_list)

f_add_data_smartwalk_db(conn = U, 
                        years = year_list, 
                        state_abbr_list = state_abbr_list)

f_create_raster_files(years = year_list, 
                      state_abbr_list = state_abbr_list)

### 3. Upload .csv file of test points to working directory
# save file as: validation_points.csv
# setup validation points, buffers:

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


