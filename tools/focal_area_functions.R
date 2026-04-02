## focal counts of cells in 1 km radius
library(terra)
## 1. combine tract-level raster data across one state 
f_combine_tract_land_raster <- function(year = 2016, 
                                   state_code = '53', 
                                   county_filepath = 'data/tract_raster_county',
                                   state_filepath = 'data/state_raster/land_area'){
  t0 <- Sys.time()
  
  tract_raster_land_files<-list.files(glue('{county_filepath}/{year}/{state_code}'), pattern = 'land_area',
                                     full.names = TRUE)
  
  # create directory for raw state land area data
  
  # year
  if(!dir.exists(glue('{state_filepath}/{year}'))){
    dir.create(glue('{state_filepath}/{year}'))
  }
  # raw data folder
  if(!dir.exists(glue('{state_filepath}/{year}/raw'))){
    dir.create(glue('{state_filepath}/{year}/raw'))
  }
  
  # import all the raster files
  message(glue('.... importing raster files for state {state_code}, year {year}'))
  tract_rasters<-lapply(tract_raster_land_files, rast)
  
  message(glue('.... combining raster files for state {state_code}, year {year}'))
  state_raster <- do.call(mosaic, c(tract_rasters, fun = 'max'))
  
  out_file <- glue('{state_filepath}/{year}/raw/state_raster_raw_{year}_{state_code}.tif')
  writeRaster(state_raster, out_file, overwrite = TRUE)
  
  # elapsed
  elapsed <- difftime(Sys.time(), t0, units = "mins") %>%
    as.numeric() %>%
    round(2)
    message(paste("    elapsed s:", elapsed))
}


# Function to apply pkfilter-like operation + convert to hectares
f_make_land_area <- function(infile, outfile_sum, outfile_ha, radius_cir_crs_units = 1000) {
  # read input raster
  r <- rast(infile)
  
  # Extract CRS units directly
  if(sf::st_crs(r)$units != 'm'){
    message('..... crs of raster file not in meters -- check before continuing')
  }

  # get cell area
  res_xy <- res(r)       # returns c(xres, yres) in CRS units
  cell_area_m2 <- res_xy[1] * res_xy[2]  # area of one cell in m²
  
  # 1. Circular focal filter with 1 km radius window)
  w <- focalMat(r, type = "circle", d = radius_cir_crs_units)
  w[w > 0] <- 1 ## set all values in the radius of the circle to 1.
  r_sum <- focal(r, w, fun = "sum", na.policy = "omit", na.rm = TRUE, fillvalue=NA)
  r_sum[r==0] <- NA

  # write the "sum" raster (cell counts within 1 km)
  writeRaster(r_sum, outfile_sum, overwrite = TRUE)
  
  # 2. Convert counts to area in hectares (divide by 1)
  r_ha <- r_sum * (res(r)[1] * res(r)[1] / 10000)  # 1 ha = 10,000 m²
  r_ha[r_sum == 0]<-NA
  
  # write the hectares raster
  writeRaster(r_ha, outfile_ha, overwrite = TRUE)
  
  return(list(sum = r_sum, ha = r_ha))
}

f_make_pop_density <- function(infile_pop, 
                               infile_land_area_ha, # change to usee 2016 for consistency
                               outfile_sum, outfile_ha, radius_cir_crs_units = 1000){
  # read input raster
  r <- rast(infile_pop)
  
  # Extract CRS units directly
  if(sf::st_crs(r)$units != 'm'){
    message('..... crs of raster file not in meters -- check before continuing')
  }
  
  # get cell area
  res_xy <- res(r)       # returns c(xres, yres) in CRS units
  cell_area_m2 <- res_xy[1] * res_xy[2]  # area of one cell in m²
  
  # 1. Circular focal filter with 1 km radius window)
  w <- focalMat(r, type = "circle", d = radius_cir_crs_units)
  w[w > 0] <- 1 ## set all values in the radius of the circle to 1.
  r_sum <- focal(r, w, fun = "sum", na.policy = "omit", na.rm = TRUE)
  #plot(r_sum)
  
  # write the "sum" raster (cell counts within 1 km)
  writeRaster(r_sum, outfile_sum, overwrite = TRUE)
  
  # 2. Get density by dividing pop by land area
  #r_ha <- r_sum * (res(r)[1] * res(r)[1] / 10000)  # 1 ha = 10,000 m²
  l_ha <- rast(infile_land_area_ha)
  r_ha <- r_sum / l_ha
  r_ha[l_ha == 0] <- NA
  
  # write the hectares raster
  writeRaster(r_ha, outfile_ha, overwrite = TRUE)
  
  return(list(sum = r_sum, ha = r_ha))
  
}

## 2. Run function to sum 1km land-area (focal point stats)
# Run pkfilter-like operation on set year / state raster files
f_land_focus_processing <- function(year = 2016, 
                                    state_code = '53',  ## update to run through a list of states, years (?). 
                                    radius_cir_crs_units = 1000, 
                                    state_filepath = 'data/state_raster/land_area'
){
  #
  if(!dir.exists(glue('{state_filepath}/{year}/sum'))){dir.create(glue('{state_filepath}/{year}/sum'))}
  if(!dir.exists(glue('{state_filepath}/{year}/ha'))){dir.create(glue('{state_filepath}/{year}/ha'))}

  t0<-Sys.time()
  message('... getting infiles and outfiles')
  infile <- glue('{state_filepath}/{year}/raw/state_raster_raw_{year}_{state_code}.tif')
  outfile_sum <- glue('{state_filepath}/{year}/sum/state_raster_land_area_sum_{year}_{state_code}.tif')
  outfile_ha <- glue ('{state_filepath}/{year}/ha/state_raster_land_area_ha_{year}_{state_code}.tif')
  
  message(paste('.... processing land focus statistics for', state_code))
  land_area<-f_make_land_area(infile = infile, outfile_sum = outfile_sum, outfile_ha = outfile_ha)
  
  # elapsed
  elapsed <- difftime(Sys.time(), t0, units = "secs") %>% as.numeric() %>% round(2)
  message(paste("    Total elapsed s:", elapsed))
}
  
## run functions to sum 1km population-area (focal point stats)
f_pop_focus_processing <- function(years = years, 
                               state_abbr_list = state_abbr_list, 
                               radius_cir_crs_units = 1000){
  
  #
  if(!dir.exists(glue('{state_filepath}/{year}/sum'))){dir.create(glue('{state_filepath}/{year}/sum'))}
  if(!dir.exists(glue('{state_filepath}/{year}/ha'))){dir.create(glue('{state_filepath}/{year}/ha'))}
  
  t0<-Sys.time()
  
  counties_df <- f_create_tractrast_var_table(years = years, 
                                              state_abbr_list = state_abbr_list)
  state_fips_list <- unique(fips_codes$state_code[fips_codes$state == toupper(state_abbr_list)])
  t0 <- Sys.time()
  for(yr in years){
    for(f in state_fips_list){
      message(glue('...processing population data for {yr} in {f}'))
      message('... getting infiles and outfiles')
      infile_land_area_ha <- glue('data/state_raster/land_area/2016/ha/state_raster_land_area_ha_2016_{f}.tif') # use only 2016 for consistency in raster extents.
      outfile_sum <- glue('data/state_raster/population_density/{yr}/sum/state_raster_pop_density_sum_{yr}_{f}.tif')
      outfile_ha <- glue ('data/state_raster/population_density/{yr}/ha/state_raster_pop_density_ha_{yr}_{f}.tif')
      
      # read tract data
      ## Combine all tract shapefiles from database    
      message('... combining tract-level shapfiles...')
      tract_tables <- counties_df$tract_tablename[counties_df$year == yr & counties_df$state_code == f]
      tracts_list <- lapply(tract_tables, function(tbl) {
        st_read(conn, query = paste0('SELECT * FROM ', tbl))
      })
      tracts_state <- bind_rows(tracts_list)
      
      # transform
      tracts_state<-st_transform(tracts_state, 26910)
      
      v <- vect(tracts_state)
      
      message('... import 2016 land area for raster template...')
      land_area_template <- rast(infile_land_area_ha)      
      
      r_popdens <- rasterize(v, land_area_template, field = 'population_per_cell', 
                             fun = 'max', background = NA)
      
      # get cell area
      res_xy <- res(r_popdens)       # returns c(xres, yres) in CRS units
      cell_area_m2 <- res_xy[1] * res_xy[2]  # area of one cell in m²
      
      message('...sum the population per cell for 1 km radius window...')
      w <- focalMat(r_popdens, type = "circle", d = radius_cir_crs_units)
      w[w > 0] <- 1 ## set all values in the radius of the circle to 1.
      r_sum <- focal(r_popdens, w, fun = "sum", na.policy = "omit", na.rm = TRUE)
      mean_pop <- round(global(r_sum, 'mean', na.rm=TRUE)[[1]], 2)
      
      message(glue('... Mean population per 1km (total)....{mean_pop} '))
      
      
      # write the "sum" raster (cell counts within 1 km)
      writeRaster(r_sum, outfile_sum, overwrite = TRUE)
      
      # 2. Get density by dividing pop by land area
      #r_ha <- r_sum * (res(r)[1] * res(r)[1] / 10000)  # 1 ha = 10,000 m²
      l_ha <- rast(infile_land_area_ha)
      r_ha <- r_sum / l_ha
      r_ha[l_ha == 0] <- NA
      
      # write the hectares raster
      writeRaster(r_ha, outfile_ha, overwrite = TRUE)
      
    }}
  
  # elapsed
  elapsed <- difftime(Sys.time(), t0, units = "secs") %>% as.numeric() %>% round(2)
  message(paste("    Total elapsed s:", elapsed))
  
}

