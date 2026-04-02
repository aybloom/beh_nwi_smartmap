library(glue)

## create composite walk raster by year / state
# function to create raster z-scores
z_raster <- function(r){

  stats <- global(r, c("mean","sd"), na.rm = TRUE)
  z <- (r - stats[1,"mean"]) / stats[1,"sd"]
  
  return(z)
  
}

f_create_composite_measure_raster <- function(yr = 2007, 
                                              state_fips = '53'){
  
  # import rasters
  busi_raster_yr <- rast(glue('H:/projects/beh_nwi_smartmap/data/state_raster/business/{yr}/ha/state_raster_business_density_{yr}_{state_fips}.tif'))
  pop_raster_yr <- rast(glue('H:/projects/beh_nwi_smartmap/data/state_raster/population_density/{yr}/ha/state_raster_pop_density_ha_{yr}_{state_fips}.tif'))
  network_raster_yr <- rast(glue('H:/projects/beh_nwi_smartmap/data/state_raster/intersections/{yr}/ha/state_raster_intersection_density_{yr}_{state_fips}.tif'))
  
  ## get z-score for raster vaules
  z_busi <- z_raster(r=busi_raster_yr)
  z_pop <- z_raster(pop_raster_yr)
  z_network <- z_raster(network_raster_yr)
  
  ## add z-scores together (no weights): 
  ## extents MUST be aligned for this to work ##
  walk_index <- z_busi + z_pop + z_network
  
  walk_dir <- 'H:/projects/beh_nwi_smartmap/data/state_raster/walk_index'
  if(!dir.exists(walk_dir)){
    dir.create(walk_dir)
  }
  walk_dir_year <- glue('{walk_dir}/{yr}')
  if(!dir.exists(walk_dir_year)){dir.create(walk_dir_year)}
  
  writeRaster(walk_index, glue('{walk_dir_year}/walk_index_{yr}_{state_fips}.tif'))

}
