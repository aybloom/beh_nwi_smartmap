## Get county template shapefile ##
library(tigris)

county_name = 'King County'

get_county_template <-function(county_name = county_name, 
                               year = 2016, 
                               state_abbr = 'wa'){
  
  state_fips <- unique(fips_codes$state_code[fips_codes$state == toupper(state_abbr)])
  county_fips <- unique(fips_codes$county_code[fips_codes$county == county_name & 
                                                fips_codes$state_code == state_fips])
  
  shp_county <- tigris::counties(state = state_fips, year = year) %>%
    dplyr::filter(COUNTYFP == county_fips)
  shp_county_4326 <- st_transform(shp_county, crs = 4326)
  
  #shp_county_4326 %>% leaflet() %>% addTiles() %>% addPolygons()
  
  return(shp_county)
}
 
crop_ha_files <- function(year = 2016, 
                         county_template = shp_county
                         ){
  
  county_rast_list <- list()
  
  ha_filepath_list <- c(population = glue('H:/projects/beh_nwi_smartmap/data/state_raster/population_density/{year}/ha/state_raster_pop_density_ha_{year}_53.tif'), 
                        intersections = glue('H:/projects/beh_nwi_smartmap/data/state_raster/intersections/{year}/ha/state_raster_intersection_density_{year}_53.tif'), 
                        business = glue('H:/projects/beh_nwi_smartmap/data/state_raster/business/{year}/ha/state_raster_business_density_{year}_53.tif'))

  for(ha_filepath in ha_filepath_list){
    
    file_name <- sub(".*state_raster/([^/]+).*", "\\1", ha_filepath)
    
    ha_rast <- rast(ha_filepath)
    shp_county_26910 <- st_transform(shp_county_template, st_crs(ha_rast))
    ha_county <- crop(ha_rast, shp_county_26910)
    ha_county <- mask(ha_county, shp_county_26910)
    
    county_rast_list <- c(county_rast_list, ha_county)

  }
  
  return(county_rast_list)
  
}
