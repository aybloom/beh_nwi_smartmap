library(sf)
library(rvest)

download_older_tiger <- function(year = 2006, 
                                 state_abbr = 'wa'){
  
  cap_state_abbr <- toupper(state_abbr)
  
  if(year == 2006){

    # URL for state shapefiles
    base_url <- glue("https://www2.census.gov/geo/tiger/tiger{year}se/{cap_state_abbr}/")
    
    # Local folder to save files
    out_dir <- "gisdata/us_census/tract/tiger2006_WA"
    dir.create(out_dir, showWarnings = FALSE)
    
    # Scrape all .zip filenames from the index
    zip_files <- read_html(base_url) %>%
      html_nodes("a") %>%
      html_attr("href") %>%
      str_subset("\\.ZIP$")
    
    # Display what we will download
    zip_files
    length(zip_files)
    
    # Download each file
    walk(zip_files, function(z) {
      message("Downloading ", z)
      download.file(url = paste0(base_url, z),
                    destfile = file.path(out_dir, z),
                    mode = "wb")
    })
  
  }
}




uzip_tiger_county <- function(state_fips = '53', 
                              county_fips = '033', 
                              year = 2006){
  state_abbr_upper <_ 
  
  unzip_dir <- 'gisdata/us_census/tract/tiger{year'
  unzip(zipfile = glue('{unzip_dir}/tiger{year}_WA/TGR{state_fips}{county_fips}.ZIP'), 
        exdir = unzip_dir)
  
  county_rt <- glue('{unzip_dir}/TGR{state_fips}{county_fips}.RT1')
  county_data <- st_read(county_rt, driver = 'TIGER')
  
}
