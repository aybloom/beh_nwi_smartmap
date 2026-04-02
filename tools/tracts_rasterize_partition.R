## tracts rasterize data -- UPDATED 3/14 ###### 

##rasterize tract nonwater data ##
library(DBI)
library(RPostgres)
library(sf)
library(terra)
library(glue)
library(fs)

source('tools/tract_population_per_cell.R') # create the f_create_tractrast_var_table function for the counties_df table.

## run functions to sum 1km population-area (focal point stats)
f_make_pop_density <- function(year = year, 
                               state_code = state_code, 
                               infile_land_area_ha, # change to usee 2016 for consistency
                               outfile_sum, 
                               outfile_ha, radius_cir_crs_units = 1000){
  
    state_abbr <- unique(fips_codes$state[fips_codes$state_code == state_code])
    counties_df <- f_create_tractrast_var_table(years = year, 
                                                state_abbr_list = state_abbr)
    
        infile_land_area_ha <- glue('data/state_raster/land_area/2016/ha/state_raster_land_area_ha_2016_{state_code}.tif') # use only 2016 for consistency in raster extents.
        outfile_sum <- glue('data/state_raster/population_density/{year}/sum/state_raster_pop_density_sum_{year}_{state_code}.tif')
        outfile_ha <- glue ('data/state_raster/population_density/{year}/ha/state_raster_pop_density_ha_{year}_{state_code}.tif')
  
 # transform
 r_popdens <- rast(glue('data/state_raster/population_density/{year}/raw/state_raster_pop_density_raw_{year}_{state_code}.tif'))
        
  # get cell area
  res_xy <- res(r_popdens)       # returns c(xres, yres) in CRS units
  cell_area_m2 <- res_xy[1] * res_xy[2]  # area of one cell in m²
  
  # 1. Circular focal filter with 1 km radius window)
  w <- focalMat(r_popdens, type = "circle", d = radius_cir_crs_units)
  w[w > 0] <- 1 ## set all values in the radius of the circle to 1.
  r_sum <- focal(r_popdens, w, fun = "sum", na.policy = "omit", na.rm = TRUE)
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
  
 # return(list(sum = r_sum, ha = r_ha))
  
}

f_pop_focus_processing <- function(year = 2016, 
                                   state_code = '53',  ## update to run through a list of states, years (?). 
                                   radius_cir_crs_units = 1000, 
                                   state_filepath = 'data/state_raster/population_density'){
  
  #
  if(!dir.exists(glue('{state_filepath}/{year}/sum'))){dir.create(glue('{state_filepath}/{year}/sum'))}
  if(!dir.exists(glue('{state_filepath}/{year}/ha'))){dir.create(glue('{state_filepath}/{year}/ha'))}
  
  t0<-Sys.time()
  message('... getting infiles and outfiles')
  infile_land_area_ha <- glue('data/state_raster/land_area/2016/ha/state_raster_land_area_ha_2016_{state_code}.tif') # use only 2016 for consistency in raster extents.
  outfile_sum <- glue('{state_filepath}/{year}/sum/state_raster_pop_density_sum_{year}_{state_code}.tif')
  outfile_ha <- glue ('{state_filepath}/{year}/ha/state_raster_pop_density_ha_{year}_{state_code}.tif')
  
  message(paste('.... processing land focus statistics for', state_code))
  pop_density<-f_make_pop_density(#infile_pop = infile_pop, 
                                  year = year,
                                  state_code = state_code,
                                  infile_land_area_ha = infile_land_area_ha, 
                                  outfile_sum = outfile_sum, outfile_ha = outfile_ha)
  
  # elapsed
  elapsed <- difftime(Sys.time(), t0, units = "secs") %>% as.numeric() %>% round(2)
  message(paste("    Total elapsed s:", elapsed))
}


## UPDATED f_tract_rasterize functions ####
f_tract_var_rasterize_1all <- function(conn = U, 
                           replace = TRUE, 
                           resolution_m = 100, 
                           years = c(2007, 2016, 2024), 
                           state_abbr_list = c('wa'),
                           vrb = TRUE){
  
  t0 <- Sys.time()
  counties_df <- f_create_tractrast_var_table(years = years, 
                                              state_abbr_list = state_abbr_list)
   state_fips_list <- unique(fips_codes$state_code[fips_codes$state == toupper(state_abbr_list)])

  for(yr in years){
    for(f in state_fips_list){
  message(glue('... combining tract shapefiles for state {f} and year {yr}...'))
  ## Combine all tract shapefiles from database    
  tract_tables <- counties_df$tract_tablename[counties_df$year == yr & counties_df$state_code == f]
  message('... get list of tract files...')
  tracts_list <- lapply(tract_tables, function(tbl) {
    st_read(conn, query = paste0('SELECT * FROM ', tbl))
  })
  
  message('...bind tract files into state...')
  tracts_state <- bind_rows(tracts_list)
  
  # transform crs
  tracts_state<-st_transform(tracts_state, 26910)
  # convert to vector
  tracts_vect <- vect(tracts_state)
  
  # create raster template from 2016 data
  message('... get state shapefile for 2016...')
  
  state_template <- tigris::tracts(state = f, year = 2016) %>% st_transform(26910)
  r <- rast(state_template)
  
  # get bounding box
  bbox <- st_bbox(state_template)
  
  xmin <- bbox['xmin']
  ymin <- bbox['ymin']
  xmax <- bbox['xmax']
  ymax <- bbox['ymax']
  
  r_template <- rast(
    xmin = xmin,
    xmax = xmax,
    ymin = ymin,
    ymax = ymax,
    resolution = resolution_m, ## check this - make it adjustable
    crs = "EPSG:26910"
  )
  
  
  # rasterize tract land data
  r_out_land <- rasterize(tracts_vect, r_template, field = 1, background = 0)
  r_out_pop <- rasterize(tracts_vect, r_template, field = 'population_per_cell', fun = 'max', backgroun = NA)

  r_out<-c(r_out_land, r_out_pop)
  names(r_out)<-c('land_area', 'population')
  #check with plot:
  #plot(r_out)crs()
  
  # Build output path
  outdir<-'data/state_raster'

  if(!dir.exists('data')){dir.create("~/data")}
  if(!dir.exists(outdir)){dir.create(outdir)}
  
  if(vrb){
    message(paste0("    ...raster data saving"))
  }  
  tif_file_land <- glue("{outdir}/land_area/{yr}/raw/state_raster_raw_{yr}_{f}.tif")
  tif_file_pop <- glue('{outdir}/population_density/{yr}/raw/state_raster_pop_density_raw_{yr}_{f}.tif')
  writeRaster(r_out_land, tif_file_land, overwrite=TRUE)
  writeRaster(r_out_pop, tif_file_pop, overwrite=TRUE)
  
  # timing
  if(vrb){
    message(paste0("    ... ", format(Sys.time() - t0)))
  }
  
    }
  }
   }
