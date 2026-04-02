# connect to db
source("dbconnect.R")
U <- us_census <- connectdb()

# function to get state UTM
get_state_utm <- function(state_fips = '53', 
                          year = 2016, 
                          conn = U){
  
  ## get state shp file
  state_file <- st_read(conn, query = glue('SELECT * FROM tract_nowater_state.tract_nowater_{year}_{state}'))
  
  ## use file to get UTM
  centroid <- st_centroid(st_union(state_file))
  lon <- st_coordinates(centroid)[1]
  
  utm_zone <- floor((lon + 180)/6) + 1
  
  # EPSG code for UTM zone
  epsg_code <- 26900 + utm_zone
  message("Using EPSG:", epsg_code, " for UTM zone ", utm_zone)
  
  # return epsg_code
  return(epsg_code)
}
