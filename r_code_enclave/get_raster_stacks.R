
setwd('E:/r_code_base')
source('raster_businesses.R')

### run function to pull and rasterize business data###
### WA State Businesses ####
f_pull_business_year_rasterize(con_str = "PG: host=localhost dbname=infousa user=postgres port=5432", 
                               year = 2016, 
                               state_abbr = 'WA', 
                               resolution_m = 100, 
                               out_folder = 'E:/state_raster/business_density')

f_pull_business_year_rasterize(con_str = "PG: host=localhost dbname=infousa user=postgres port=5432", 
                               year = 2007, 
                               state_abbr = 'WA', 
                               resolution_m = 100, 
                               out_folder = 'E:/state_raster/business_density')

f_pull_business_year_rasterize(con_str = "PG: host=localhost dbname=infousa user=postgres port=5432", 
                               year = 2023, 
                               state_abbr = 'WA', 
                               resolution_m = 100, 
                               out_folder = 'E:/state_raster/business_density')

## stack rasters for transfer
wa_stack <- c(wa_2006_business, wa_2016_business, wa_2023_business)
names(wa_stack) <- c('wa_2006', 'wa_2016', 'wa_2023')

## setack rasters 2007, 2024
wa_stack <- c(wa_2007_business, wa_2024_business)
names(wa_stack) <- c('wa_2007', 'wa_2024')

## PA Businesses ####
for(year in c(2007, 2016, 2024)){
    f_pull_business_year_rasterize(con_str = "PG: host=localhost dbname=infousa user=postgres port=5432", 
    year = year, 
    state_abbr = 'PA', 
    resolution_m = 100, 
    out_folder = 'E:/state_raster/business_density')
}

### MN Businesses ####
for(year in c(2007, 2016, 2024)){
  f_pull_business_year_rasterize(con_str = "PG: host=localhost dbname=infousa user=postgres port=5432", 
                                 year = year, 
                                 state_abbr = 'MN', 
                                 resolution_m = 100, 
                                 out_folder = 'E:/state_raster/business_density')
}



### pull in business data & Stack for export ####
pa_2007 <- rast('E:/state_raster/business_density/2007/raw/state_raster_business_raw_2007_PA.tif')
pa_2016 <- rast('E:/state_raster/business_density/2016/raw/state_raster_business_raw_2016_PA.tif')
pa_2024 <- rast('E:/state_raster/business_density/2024/raw/state_raster_business_raw_2024_PA.tif')

mn_2007 <- rast('E:/state_raster/business_density/2007/raw/state_raster_business_raw_2007_MN.tif')
mn_2016 <- rast('E:/state_raster/business_density/2016/raw/state_raster_business_raw_2016_MN.tif')
mn_2024 <- rast('E:/state_raster/business_density/2024/raw/state_raster_business_raw_2024_MN.tif')

names(pa_2007) <- 'pa_2007'
names(pa_2016) <- 'pa_2016'
names(pa_2024) <- 'pa_2024'

names(mn_2007) <- 'mn_2007'
names(mn_2016) <- 'mn_2016'
names(mn_2024) <- 'mn_2024'

#plot(mn_2016)
#plot(mn_2024)
#summary(mn_2024)

pa_full <- c(pa_2007, pa_2016, pa_2024)
mn_full <- c(mn_2007, mn_2016, mn_2024)

writeRaster(pa_full, 'X:/business_density_rasters/pa_rasters_2007_2016_2024.tif', overwrite = TRUE)

writeRaster(mn_full, 'X:/business_density_rasters/mn_rasters_2007_2016_2024.tif')
