## set up functions to build walkability database ####
# download binary versions of R 4.3.2 - capatible packages
#source('tools/package_error_fix.R')

wd <- wd
# functions to check for existing data
source(glue('{wd}/tools/check_schemas.R'))

# tract land area
source(glue('{wd}/tools/tracts_download_postgis_partition.R'))
source(glue('{wd}/tools/area_water_download_postgis_partition.R'))
source(glue('{wd}/tools/tracts_erase_water.R'))

# population density
source(glue('{wd}/tools/tracts_population_download.R')) # download tract-level variables
source(glue('{wd}/tools/tract_population_per_cell.R')) # calculate tract-level variables per cell (resolution_m) in meters
source(glue('{wd}/tools/tracts_rasterize_partition.R')) # rasterize the population data 

# intersection density
GOVT_SHUTDOWN <- FALSE
source(glue('{wd}/tools/download_roads_edges.R'))
source(glue('{wd}/tools/push_edges_to_db.R'))
source(glue('{wd}/tools/create_intersection_buffers.R'))
source(glue('{wd}/tools/rasterize_state_intersections.R'))

source(glue('{wd}/tools/get_business_density.R'))

# focal area functions for land density and population density
source(glue('{wd}/tools/focal_area_functions.R'))

# function to calculate composite measure and create state rasters of composite measure
source(glue('{wd}/tools/create_composite_walk_measure.R'))

# validation functions 
source(glue('{wd}/tools/validate_smartmap_measures.R'))


