

## get business density
f_get_business_density <- function(state_fips = '53', year = 2016, 
                                       verbose = TRUE){
  
  raster_sum_infile <- glue('data/state_raster/business/{year}/sum/state_raster_business_density_{year}_{state_fips}.tif')
  land_area_ha <- glue('data/state_raster/land_area/2016/ha/state_raster_land_area_ha_2016_{state_fips}.tif')
  
  outfile_ha <- glue('data/state_raster/business/{year}/ha/state_raster_business_density_{year}_{state_fips}.tif')
  
  r <- rast(raster_sum_infile)
  l_ha <- rast(land_area_ha)
  
  # align extents
  r_resamp <- resample(r, l_ha, method = 'bilinear')
  
  r_ha <- r_resamp / l_ha
  r_ha[l_ha == 0] <- NA
  writeRaster(r_ha, outfile_ha, overwrite=TRUE)
 
}
