## Tracts Rasterize intersection points ###
library(DBI)
library(RPostgres)
library(sf)
library(terra)
library(glue)
library(fs)
library(tidycensus)

# states and counties FIPS codes
states_fips <- unique(fips_codes$state_code)[1:51]
state_code <- unique(fips_codes$state)[1:51]

## function to rasterize one state / year of intersections
f_rasterize_intersections_0one <- function(con_str = con_str, 
                                           conn = U, 
                                           state_fips = '53', year = 2016, 
                                           resolution_m = 100, verbose = TRUE, 
                                           state_filepath = 'H:/projects/beh_nwi_smartmap/data/state_raster/intersections'){
  
  # create directory
  if(!file.exists(glue('{state_filepath}'))){dir.create(glue('{state_filepath}'))}
  if(!file.exists(glue('{state_filepath}/{year}'))){dir.create(glue('{state_filepath}/{year}'))}
  if(!file.exists(glue('{state_filepath}/{year}/raw'))){dir.create(glue('{state_filepath}/{year}/raw'))}
  
  # get state file for bounding: 
  #state_file <- st_read(conn, query = glue('SELECT * FROM tract_nowater_state.tract_nowater_{year}_{state_fips}'))
  state_file_raster <- rast(glue('H:/projects/beh_nwi_smartmap/data/state_raster/land_area/2016/raw/state_raster_raw_2016_{state_fips}.tif'))
  state_abbr <- tolower(unique(fips_codes$state[fips_codes$state_code == state_fips]))
  
  # update CRS
  state_bbox <- ext(state_file_raster)
 # state_bbox <- st_as_sf(as.polygons(state_bbox), crs = 26910) ## put function in to get the right crs ?
  
  # get bounding box
  bbox <- st_bbox(state_bbox)
  
  xmin <- as.numeric(bbox['xmin'])
  ymin <- as.numeric(bbox['ymin'])
  xmax <- as.numeric(bbox['xmax'])
  ymax <- as.numeric(bbox['ymax'])
  
  out_filename <- glue('H:/projects/beh_nwi_smartmap/data/state_raster/intersections/{year}/raw/state_raster_intersection_buffer_{year}_{state_fips}.tif')
  sql_query <- glue("SELECT geom_snap_buffer_26910 FROM edges.intersections_3plus_{state_fips}_{year}")
  
  geom <- st_read(con_str, query = sql_query)
  
  # define output raster
  r_template <- rast(ext(xmin, xmax, ymin, ymax), 
            res = resolution_m, 
            crs = st_crs(geom)$wkt)
  
  # add constant field
  geom$intersection_density <- 1
  
  # new approach to get all cells touched by polygons
  # hits <- cells(r_template, vect(geom), touches = TRUE) # generates a dataframe: cell + polygon index
  # tabs <- table(hits[, 'cell']) # count how many polygons touch each cell
  # 
  # create output raster
  
  
  r_out <- rasterize(vect(geom), r_template, field = 'intersection_density', 
                     fun = 'sum', background = 0)
  
  r_out <- mask(r_out, state_file_raster)
  r_out[state_file_raster==0]<-NA
  
  if(verbose){
    message(".     ... rasterizing state data")
  }
  
  writeRaster(r_out, out_filename, overwrite=TRUE)
  }


## intersection density 
f_get_intersection_density <- function(con_str = con_str, 
                                       conn = U, 
                                       state_fips = '53', year = 2016, 
                                      verbose = TRUE){
  
  raster_infile <- glue('H:/projects/beh_nwi_smartmap/data/state_raster/intersections/{year}/raw/state_raster_intersection_buffer_{year}_{state_fips}.tif')
  land_area_raw <- glue('H:/projects/beh_nwi_smartmap/data/state_raster/land_area/2016/raw/state_raster_raw_2016_{state_fips}.tif')
  land_area_ha <- glue('H:/projects/beh_nwi_smartmap/data/state_raster/land_area/2016/ha/state_raster_land_area_ha_2016_{state_fips}.tif')
  
  outfile_sum <- glue('H:/projects/beh_nwi_smartmap/data/state_raster/intersections/{year}/sum/state_raster_intersection_density_{year}_{state_fips}.tif')
  if(!file.exists(glue('H:/projects/beh_nwi_smartmap/data/state_raster/intersections/{year}/ha'))){dir.create(glue('data/state_raster/intersections/{year}/ha'))}
  outfile_ha <- glue('H:/projects/beh_nwi_smartmap/data/state_raster/intersections/{year}/ha/state_raster_intersection_density_{year}_{state_fips}.tif')
  
   r <- rast(raster_infile)
   
   # sum intersections per 1km
  
   l_raw<-rast(land_area_raw)
   l_ha <- rast(land_area_ha)
   
   r_ha <- r / l_ha
   r_ha[l_ha == 0] <- NA
  writeRaster(r_ha, outfile_ha, overwrite = TRUE)
  
}



