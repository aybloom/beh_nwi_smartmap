

f_create_tractrast_var_table <- function(years, 
                                         state_abbr_list){
  
  # year-state-county
  counties_fips <- fips_codes %>%
    dplyr::filter(state %in% toupper(state_abbr_list)) %>%
    crossing(years)
  
  # year-state-county
  counties_df <- counties_fips %>% 
    dplyr::select(year = years, state_code, county_code) %>% 
    distinct() %>% 
    mutate(land_area_focal_processing_filename = paste0(str_c('tract_raster_', year, '_', state_code)),
           raster_filename = paste0(str_c("tract_raster", year, state_code, county_code, sep="_"),  '.tif'),
           tract_tablename = str_c("tract_nowater_county", str_c("tract_nowater", year, state_code, county_code, sep="_"), sep = "."), 
           raster_resolution = 30) %>%
    arrange(year, state_code, county_code)
  
  return(counties_df)
}


# ---- Add total pop column to DB if it doesn't exist ----
f_update_nowater_columns <-function(){
col_check <- dbGetQuery(U, glue::glue("
    SELECT column_name 
    FROM information_schema.columns
    WHERE table_schema = 'tract_nowater'
      AND table_name = 'tract_nowater'
      AND column_name = 'population_per_cell';
  "))

if (nrow(col_check) == 0) {
  message("Adding column population_per_cell to database parent table...")
  O <- dbExecute(U, glue::glue("
      ALTER TABLE tract_nowater.tract_nowater
      ADD COLUMN population_per_cell numeric;
    "))
}
if (nrow(col_check) > 0) {
  message('population_per_cell column already exists')
}
}
f_merge_population_tract_nowater_0one<-function(i, resolution_m = 100,
                                                verbose = TRUE, 
                                                years = years, state_abbr_list = state_abbr_list, 
                                                counties_df = counties_df
){
  
  # set values from counties_df
  year = counties_df$year[i]
  state_code = counties_df$state_code[i] 
  county_code = counties_df$county_code[i]
  cell_area = resolution_m^2
  
  # pull in tract_no_water shapefile
  tract_nowater<-st_read(dsn, query = glue('SELECT * from tract_nowater_county.tract_nowater_{year}_{state_code}_{county_code}'))
  tract<-st_read(dsn, query = glue('SELECT * from tract_county.tract_{year}_{state_code}_{county_code}'))
  tract_population<-tract_tbl<-dbGetQuery(U, glue("SELECT geoid, totalpop FROM tract_variables_county.tract_variables_{year}_{state_code}_{county_code}")) 
  
  ## check for missing geoids in tract_population:
  tract_add_pop <- NULL
  for(tract_geoid in tract_nowater$geoid){
    
    if(tract_geoid %in% tract_population$geoid | !is.null(tract_add_pop)){next}
    else{
      message(glue('... getting decennial data for {state_code}, county: {county_code}'))
      tract_dec_pop <- get_decennial(geography = 'tract', 
                                     variables = 'P001001', 
                                     state = state_code, county = county_code, year = (year - year %% 10))
      tract_add_pop <- tract_dec_pop %>% 
        dplyr::select(geoid = GEOID, totalpop = value)
    }
    
}
  if(!is.null(tract_add_pop)){
    tract_population <- rbind(tract_population, tract_add_pop)
  }
  
  
  # merge by geoid
  tract_nowater_pop<-merge(tract_nowater, tract_population, by = 'geoid', all.x=TRUE)
  
  if(TRUE %in% is.null(tract_nowater_pop$totalpop)){
    message(glue('..... WARNING: missing population for some tracts in county: {county_code}, state: {state_code}'))
  }
  
  # calculate population per cell totalpop/st_area(geom_26910)
  tract_nowater_pop_26910 <- st_transform(tract_nowater_pop, 26910) %>% 
    dplyr::rename(geom_26910 = geom_4326)
  tract_nowater_pop_26910 %<>% 
    rowwise() %>%
    mutate(population_per_cell = ifelse(totalpop == 0, 0,
                                            totalpop / (st_area(geom_26910) / cell_area))
    )

  # ---- Update values in DB table ----
  values_sql <- paste0(
    "('", tract_nowater_pop_26910$geoid, "', ", tract_nowater_pop_26910$population_per_cell, ")",
    collapse = ", "
  )
  
  message("Updating population_per_cell_10m values in database...")

  update_sql <- glue::glue("
  UPDATE tract_nowater_county.tract_nowater_{year}_{state_code}_{county_code} AS t
  SET population_per_cell = c.population_per_cell
  FROM (VALUES
    {values_sql}
  ) AS c(geoid, population_per_cell)
  WHERE t.geoid = c.geoid;
")
  
  o <- dbExecute(U, update_sql)
  
  message("Done updating table.")
  
}

f_merge_population_tract_nowater_1all <- function(startnum = 1, 
                                                  replace = FALSE, 
                                                  resolution_m = 100,
                                                  years = c(2016), 
                                                  state_abbr_list = c('wa'),
                                                  verbose = TRUE){
  t0<- Sys.time()
  idx <- 0
  
  counties_df <- f_create_tractrast_var_table(years = years, state_abbr_list = state_abbr_list)
  
  # run over all the entries inthe county data frame
  for(i in startnum:nrow(counties_df)){
    idx <- idx + 1
    if(verbose){
      message(paste(i, "of", nrow(counties_df)))
    }
    # run the rasterize function
    f_merge_population_tract_nowater_0one(i = i, resolution_m = resolution_m, verbose = vrb, 
                                          counties_df = counties_df)
    # elapsed 
    elapsed <- difftime(Sys.time(), t0, units = "secs") %>% as.numeric() %>% round(2)
    # time per run
    t.run.s <- (elapsed / idx) %>% round(2)
    # estimated time to completion
    total.hours <- (t.run.s * (nrow(counties_df) - i) / 3600) %>%  round(2)
    if(verbose){
      message(paste("    Run:", idx, "; elapsed s:", elapsed, "; s per run:", t.run.s, "; completion:", total.hours, "h"))
    }        
  }
  
  
}
