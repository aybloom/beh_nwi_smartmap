## get state extent for making rasters
library(tidycensus)
library(glue)
library(sf)
library(tidyverse)
library(terra)
con_str = "PG: host=localhost dbname=infousa user=postgres port=5432"


get_state_extent <- function(con_str = con_str, 
                             state_abbr = 'WA'){
# state_table_sql <- glue("SELECT geom FROM business.business 
#                            WHERE state = '{state_abbr}' and archive_version_year = 2019")
# 
# d_state_table <- st_read(con_str, query = state_table_sql)
# state_pts_utm <- st_transform(d_state_table, 26910)
# state_pts_buffered <- state_pts_utm %>% mutate(geom = st_buffer(geom, 1000))
# d_state_union <- st_union(state_pts_buffered)
# d_state_polygon <- st_convex_hull(d_state_union)

## get state raster template #####
state_2016 <-rast('X:/state_raster_raw_2016_53.tif')  

bbox <- st_bbox(d_state_polygon)

expand <- 1000 * 5 ## keep this? 

extent_obj <- ext(bbox["xmin"] - expand, 
                  bbox["xmax"] + expand, 
                  bbox["ymin"] - expand, 
                  bbox["ymax"] + expand
)

st_write(d_state_polygon, 
         glue('E:/state_raster/business_density/extent_{state_abbr}.shp'))

}
