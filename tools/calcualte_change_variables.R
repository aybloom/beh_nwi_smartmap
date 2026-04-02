## calculate change in measures raster file ###
setwd('H:/projects/beh_nwi_smartmap')
source('check_schemas.R')
library(kableExtra) # pretty tables### UPDATE -- add library AY
library(glue)
library(dplyr)
library(leaflet)
library(raster)
library(sf)
library(DBI)
library(tidycensus)
library(terra)
library(tigris)

# set year one and year two to compare
year1 <- 2007
year2 <- 2024

# set year to get raster for
state_abbr <- 'wa'

f_get_raster_difference<-function(state_abbr = state_abbr, 
                        county_crop = '033',
                        year1 = year1, 
                        year2 = year2){
  
  state_fips = unique(fips_codes$state_code[fips_codes$state == toupper(state_abbr)])
  
  if(year2 <= year1){
    message('.... year2 needs to be after year1')
    return(null)
  }
  ## check for raster files ####
  raster_files<-check_raster_files()
  missingyears <- c(year1 = FALSE, year2 = FALSE)
  if(!(year1 %in% raster_files$year[raster_files$state == state_fips])){
    message(glue('.... missing raster data for {year1} for {state_abbr}'))
    missingyears['year1'] <- TRUE
  }
   if(!(year2 %in% raster_files$year[raster_files$state == state_fips])){
    message('.... missing raster data for {year2} for {state_abbr}')
     missingyears['year2'] <- TRUE
   }
  if(any(!unlist(missingyears))){
    return(null)
  } else {
    
    # set template (state or county)
    if(is.na(county_crop)){
      shp_template <- states(year = year2) %>% 
        filter(STATEFP == state_fips)
    } else {
        shp_template <- counties(state = state_fips, year= year2) %>% 
          filter(COUNTYFP == county_crop)
    }
    shp_template <- st_transform(shp_template, crs = 26910)#st_crs(r_intdens_yr1))
    shp_template <- vect(shp_template)
 ## Import raster files for year1 ####
  
    r_intdens_yr1 <- rast(x = glue("data/state_raster/intersections/{year1}/ha/state_raster_intersection_density_{year1}_{state_fips}.tif")) %>%
      crop(., shp_template)%>% mask(., shp_template)
    r_popdens_yr1 <- rast(x = glue('data/state_raster/population_density/{year1}/ha/state_raster_pop_density_ha_{year1}_{state_fips}.tif')) %>%
      crop(., shp_template)
    r_busdens_yr1 <- rast(x = glue('data/state_raster/business/{year1}/ha/state_raster_business_density_{year1}_{state_fips}.tif')) %>%
      crop(., shp_template)
    
## Import raster files for year2 ####
    r_intdens_yr2 <- rast(x = glue("data/state_raster/intersections/{year2}/ha/state_raster_intersection_density_{year2}_{state_fips}.tif")) %>%
      crop(., shp_template) %>% mask(., shp_template) 
    r_popdens_yr2 <- rast(x = glue('data/state_raster/population_density/{year2}/ha/state_raster_pop_density_ha_{year2}_{state_fips}.tif')) %>%
      crop(., shp_template) %>% mask(., shp_template) 
    r_busdens_yr2 <- rast(x = glue('data/state_raster/business/{year2}/ha/state_raster_business_density_{year2}_{state_fips}.tif')) %>%
      crop(., shp_template) %>% mask(., shp_template) 
    
## align rasters
    r_intdens_yr1_align <- resample(r_intdens_yr1, r_intdens_yr2, 'bilinear')
    r_popdens_yr1_align <- resample(r_popdens_yr1, r_popdens_yr2, 'bilinear')
    r_busdens_yr1_align <- resample(r_busdens_yr1, r_busdens_yr2, 'bilinear')
  
 ## subtract rasters to compare change ####
    r_indens_change <- (r_intdens_yr2-r_intdens_yr1_align)/r_intdens_yr2_align
    r_indens_change[r_intdens_yr1_align ==0] <- NA
    r_popdens_change <- (r_popdens_yr2 - r_popdens_yr1_align)/r_popdens_yr2_align
    r_busdens_change <- (r_busdens_yr2 - r_busdens_yr1_align)/r_busdens_yr2_align
    r_busdens_change[r_busdens_yr1_align == 0]<-NA
    
  ## absolute change values
    r_indens_change_abs <- (r_intdens_yr2 - r_intdens_yr1_align)
    r_popdens_change_abs <- (r_popdens_yr2 - r_popdens_yr1_align)
    r_busdens_change_abs <- (r_busdens_yr2 - r_busdens_yr1_align)
    
  ## stack map layers and save
    r_stacked_change <- c(r_indens_change, r_popdens_change, r_busdens_change, 
                          r_indens_change_abs, r_popdens_change_abs, r_busdens_change_abs)
    names(r_stacked_change) <- c(glue('perc_change_intersection_density_{year1}_{year2}'), 
                                glue('perc_change_population_density_{year1}_{year2}'),
                                glue('perc_change_business_density_{year1}_{year2}'), 
                                glue('absolute_change_intersection_density_{year1}_{year2}'), 
                                glue('absolute_change_population_density_{year1}_{year2}'), 
                                glue('absolute_change_business_density_{year1}_{year2}')
                                )
    
    
  }
  
  writeRaster(r_stacked_change, 
              glue('H:/projects/beh_nwi_smartmap/data/change_rasters/stacked_change_raster_{state_fips}{county_crop}_{year1}_{year2}.tif'))
  return(r_stacked_change)
}

