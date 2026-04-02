## clean up exported business rasters for WA state
business_raster_filepath <- 'data/state_raster/business/'

## Separate WA Rasters #####
full_business_rasters <- rast(glue('{business_raster_filepath}/wa_rasters_2006_2016_2023.tif'))

# udpate raster names
# names(full_business_rasters) <- c('wa_2006', 'wa_2016', 'wa_2023')

wa_2006 <- full_business_rasters$wa_2006
wa_2016 <- full_business_rasters$wa_2016
wa_2023 <- full_business_rasters$wa_2023

## save individual rasters
writeRaster(wa_2006, glue('{business_raster_filepath}/2006/sum/state_raster_business_density_2006_53.tif'), overwrite = TRUE)
writeRaster(wa_2016, glue('{business_raster_filepath}/2016/sum/state_raster_business_density_2016_53.tif'), overwrite = TRUE)
writeRaster(wa_2023, glue('{business_raster_filepath}/2023/sum/state_raster_business_density_20123_53.tif'), overwrite = TRUE)

## 2007 & 2024
full_business_rasters <- rast(glue('{business_raster_filepath}/wa_rasters_2007_2024.tif'))

wa_2007 <- full_business_rasters$wa_2007
wa_2024 <- full_business_rasters$wa_2024

writeRaster(wa_2007, glue('{business_raster_filepath}/2007/sum/state_raster_business_density_2007_53.tif'), overwrite = TRUE)
writeRaster(wa_2024, glue('{business_raster_filepath}/2024/sum/state_raster_business_density_2024_53.tif'), overwrite = TRUE)

## Separate MN Rasters ####r
full_business_rasters_mn <- rast(glue('{business_raster_filepath}/mn_rasters_2007_2016_2024.tif'))

mn_2007 <- full_business_rasters_mn$mn_2007
mn_2016 <- full_business_rasters_mn$mn_2016
mn_2024 <- full_business_rasters_mn$mn_2024

## save individual rasters
writeRaster(mn_2007, glue('{business_raster_filepath}/2007/sum/state_raster_business_density_2007_27.tif'), overwrite = TRUE)
writeRaster(mn_2016, glue('{business_raster_filepath}/2016/sum/state_raster_business_density_2016_27.tif'), overwrite = TRUE)
writeRaster(mn_2024, glue('{business_raster_filepath}/2024/sum/state_raster_business_density_2024_27.tif'), overwrite = TRUE)


## Separate PA Rasters ####r
full_business_rasters_pa <- rast(glue('{business_raster_filepath}/pa_rasters_2007_2016_2024.tif'))

pa_2007 <- full_business_rasters_pa$pa_2007
pa_2016 <- full_business_rasters_pa$pa_2016
pa_2024 <- full_business_rasters_pa$pa_2024

## save individual rasters
writeRaster(pa_2007, glue('{business_raster_filepath}/2007/sum/state_raster_business_density_2007_42.tif'), overwrite = TRUE)
writeRaster(pa_2016, glue('{business_raster_filepath}/2016/sum/state_raster_business_density_2016_42.tif'), overwrite = TRUE)
writeRaster(pa_2024, glue('{business_raster_filepath}/2024/sum/state_raster_business_density_2024_42.tif'), overwrite = TRUE)

