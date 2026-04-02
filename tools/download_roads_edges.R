## Download all edges
library(curl)
library(sf)
library(readxl)
library(tigris)
library(tidyverse)
library(magrittr)

f_download_edges <- function(state_abbr_list = c('wa'), years = c(2016), govt_shutdown = FALSE, replace = TRUE){
  
  # setup 
  # note that edges URLs change in 2011 -- downloads differ. 

  if(govt_shutdown == TRUE){
    ## add archival prefix to urls
    urlbase <- glue('https://web.archive.org/web/20250613140421/')
 
  } else{
    urlbase <- "https://www2.census.gov/geo/tiger/"
  }
  
  # where to download & store
  mydestdir <- file.path("H:/projects/beh_nwi_smartmap/gisdata/us_census/edges")
  if(!dir.exists(mydestdir)){
    dir.create(mydestdir)
  }
  
  #get state_fips codes for download, along with list of counties
  state_fips = fips_codes %>% dplyr::filter(state %in% toupper(state_abbr_list)) %>% 
    dplyr::select(state_code, state, state_name, county_code, county) %>% unique()
  
  for(YR in years){
   
   if(YR==2007){
     urlbase_year = str_c(urlbase, 'TIGER2007FE')
   } else {
     urlbase_year = str_c(urlbase, 'TIGER', YR)
   }
  for(i in unique(state_fips$state_code)){
    state_name <- unique(state_fips$state_name[which(state_fips$state_code ==i)])
    
    # county-level downloads
    if(YR >= 2016){
      state_year_url <- glue(urlbase_year, '/EDGES/tl_{YR}_{i}')
      
      for(c in state_fips$county_code[which(state_fips$state_code == i)]){
        county_year_url <- glue("{state_year_url}{c}_edges.zip")
        
        zip_edges_file <- str_c(mydestdir, '/tl_',YR, '_', i,c, '_edges.zip')
        shp_edges_file <- str_replace(zip_edges_file, "\\.zip$", ".shp")
        
        print(shp_edges_file)
        
        if(file.exists(shp_edges_file) & replace == FALSE){
          message('... file already downloaded, skipping')
        } else {
        download.file(url = county_year_url, 
                      destfile = zip_edges_file)
        
        unzip(zipfile = zip_edges_file, 
              exdir = mydestdir)
        }
        
      
      }
    } else if(YR < 2010){
      state_year_url <- str_c(urlbase_year, '/', i, '_', toupper(state_name))
      
      for(c in state_fips$county_code[which(state_fips$state_code == i)]){
        county_name <- state_fips$county[which(state_fips$state_code == i & state_fips$county_code == c)] %>% 
        str_remove(" County$") %>% # remove " County"
          str_replace_all("\\.", "") %>%    # remove periods (St. -> St)
          str_replace_all("\\s+", "_")     # replace ALL spaces with "_"
        
        if(YR == 2007){
       county_year_url <- str_c(state_year_url, '/', i, c, '_', county_name, '/fe_2007_', i,c, '_edges.zip')
        }else if(YR < 2010){
          county_year_url <- str_c(state_year_url, '/', i, c, '_', county_name, '/tl_', YR, '_', i,c, '_edges.zip')
        }
        zip_edges_file <- str_c(mydestdir, '/tl_',YR, '_', i,c, '_edges.zip')
        
        download.file(url = county_year_url, 
                      destfile = zip_edges_file)
        
        unzip(zipfile = zip_edges_file, 
              exdir = mydestdir)
        
    }
  }
  }
  }
}

