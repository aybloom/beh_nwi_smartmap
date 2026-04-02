### Review Intersections ####
setwd('H:/projects/beh_nwi_smartmap')
source('dbconnect.R')
source('tools/get_map_template_functions.R')
library(sf)
library(leaflet)
library(terra)
library(glue)
library('DBI')
# connect to database & set dsn
U <- us_census <- connectdb()
con_str = "PG:host=doyenne.csde.washington.edu dbname=nwi_smartmap user=aybloom password=csde4322 port=5432" 

King_template <- get_county_template(county_name = 'King County', year = 2016, state_abbr = 'wa') 
King_template_26910 <- st_transform(King_template, 26910)

### EDGES ####
edges_tbl_2007 <- st_read(con_str, query = 'SELECT * from edges_state.edge_2007_53')
head(edges_tbl_2007)

### ENDPOINTS ####
endpoints_2007 <- st_read(con_str, query = 'SELECT * from edges.endpoints_53_2007')
head(endpoints_2007)

### NODE DEGREES ###
node_degrees_2007 <- st_read(con_str, query = 'SELECT * from edges.node_degrees_53_2007')
head(node_degrees_2007)

### endpoints with degrees ###
endpoint_degrees_2007 <- st_read(con_str, query = 'SELECT * from edges.endpoints_with_degree_53_2007')
head(endpoint_degrees_2007)

### edges with degrees ###
segment_degrees_2007 <- st_read(con_str, query = 'SELECT * from edges.segment_degrees_53_2007')
head(segment_degrees_2007)
edges_full_degrees_2007 <- st_read(con_str, query = 'SELECT * from edges.edges_degrees_53_2007')
head(edges_full_degrees_2007)
### 

#### ENDPOINTS ####
endpoints_tbl_2024 <- st_read(con_str, query = 'SELECT * from edges.endpoints_53_2024')
endpoints_tbl_2016 <- st_read(con_str, query = 'SELECT * from edges.endpoints_53_2016')
endpoints_tbl_2007 <- st_read(con_str, query = 'SELECT * from edges.endpoints_53_2007')

endpoints_king <- st_intersects(endpoints_tbl_2024, King_template_26910 , sparse = FALSE)
endpoints_filter <- st_filter(endpoints_tbl_2024, King_template_26910)

endpoints_4326 <- st_transform(endpoints_filter, 4326)

leaflet(endpoints_4326) %>%
  addTiles() %>%
  addCircleMarkers(
    radius = 2, 
    fillColor = 'red', 
    fillOpacity = 0.7, 
    stroke = FALSE, 
    popup = ~paste0("tlid; mtfcc: ", tlid, "; ", mtfcc),
    clusterOptions = markerClusterOptions())

table(endpoints_4326$mtfcc)


### endpoints with degrees ####
endpoints_with_degree_tbl_2007 <- st_read(con_str, query = 'SELECT * from edges.endpoints_with_degree_53_2007')
head(endpoints_with_degree_tbl_2007)

# segments ####
segments_with_degrees_tbl_2007 <- st_read(con_str, query = 'SELECT * from edges.segment_degrees_53_2007')
head(segments_with_degrees_tbl_2007)

# edges with degrees
edges_with_degrees_tbl_2007 <- st_read(con_str, query = 'SELECT * from edges.edges_with_degrees_53_2007')
head(edges_with_degrees_tbl_2007)

## INTERSECTIONS ####
intersections_tbl_2024 <- st_read(con_str, query = 'SELECT * from edges.intersections_3plus_53_2024')
intersections_tbl_2007 <- st_read(con_str, query = 'SELECT * from edges.intersections_3plus_53_2007')
intersections_tbl_2016 <- st_read(con_str, query = 'SELECT * from edges.intersections_3plus_53_2016')

intersections_filter_2007 <- st_filter(intersections_tbl_2007, King_template_26910)
intersections_filter_2016 <- st_filter(intersections_tbl_2016, King_template_26910)
intersections_filter_2024 <- st_filter(intersections_tbl_2024, King_template_26910)

intersections_4326_2007 <- st_transform(intersections_filter_2007, 4326)
intersections_4326_2016 <- st_transform(intersections_filter_2016, 4326)
intersections_4326_2024 <- st_transform(intersections_filter_2024, 4326)


leaflet() %>%
  addTiles() %>%
  addCircleMarkers(
    data = intersections_4326_2007,
    radius = 5, 
    fillColor = 'yellow', 
    fillOpacity = 1, 
    stroke = FALSE, 
    popup = ~paste0("tlid: ", tlid),
    clusterOptions = markerClusterOptions()) %>%
  #### 2016 ####
addCircleMarkers(
  data = intersections_4326_2016,
  radius = 5, 
  fillColor = 'red', 
  fillOpacity = 0.6, 
  stroke = FALSE, 
  popup = ~paste0("tlid: ", tlid),
  clusterOptions = markerClusterOptions()) %>%
  ### 2024 #####
addCircleMarkers(
  data = intersections_4326_2007,
  radius = 5, 
  fillColor = 'blue', 
  fillOpacity = 0.3, 
  stroke = FALSE, 
  popup = ~paste0("tlid: ", tlid),
  clusterOptions = markerClusterOptions())


### Check land area raster files for WA: ####
wa_land_2007 <- rast('data/state_raster/land_area/2007/raw/state_raster_raw_2007_53.tif')
wa_land_2016 <- rast('data/state_raster/land_area/2016/raw/state_raster_raw_2016_53.tif')
wa_land_2024 <- rast('data/state_raster/land_area/2024/raw/state_raster_raw_2024_53.tif')
plot(wa_land_2007)  
plot(wa_land_2016)  
plot(wa_land_2024)  
res(wa_land_2007)
res(wa_land_2016)
res(wa_land_2024)
ext(wa_land_2007) ### prior to 2011, extent is slightly different than other yearss
ext(wa_land_2016)
ext(wa_land_2024)


## check intersection buffers for the state
sql_query <- glue("SELECT geom_snap_buffer_26910 FROM edges.intersections_3plus_53_2016")
geom <- st_read(con_str, query = sql_query)

