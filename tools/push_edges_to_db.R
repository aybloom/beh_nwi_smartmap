# Phil Hurvitz 20211122
# push edges to db

library(sf)
library(tidyverse)
library(magrittr)
library(tools)
library(tigris)

# create the edges schema
f_edges_create_schemas <- function(overwrite = FALSE) {
  # drop the schema if overwriting
  if (overwrite) {
    O <- dbExecute(
      conn = U,
      statement = "drop schema if exists edges cascade"    )
    O <- dbExecute(conn = U, statement = 'drop schema if exists edges_state cascade')    
    O <- dbExecute(conn = U, statement = "drop schema if exists edges_county cascade")

  }
  schemas <- c("edges", "edges_state", "edges_county")
  
  for (s in schemas) {
    dbExecute(U, paste0("CREATE SCHEMA IF NOT EXISTS ", s, ";"))
  }
}

# a wrapper to run the functions
wrapper_write_edges <- function(){
  f_structure_level_1()
  f_structure_level_2()
  
  # make the structure and upsert
  for(y in years){
    f_structure_level_3(y)
    f_upsert_shapefiles(y)
  }
  # f_structure_level_3(2010)
  # f_structure_level_3(2015)
  # f_structure_level_3(2020)
  # f_upsert_shapefiles(2015)
  # f_upsert_shapefiles(2020)
  
  # f_snapper()
  f_index()
}

f_import_reformat_edges <- function(year = 2016, state = '53', county = '033'){
  # get data (must be downloaded first -- see download_roads_edges.R)
  
  #mydestfile <- list.files(path = "~/gisdata/us_census/edges", pattern = "*.shp", full.names = TRUE)
  mydestfile <- glue('data/gisdata/edges/tl_{year}_{state}{county}_edges.shp')
  if(!file.exists(mydestfile)){
    message(glue('..... edges for state: {state}, county: {county} and year: {year}do not exists, go back and run download_edges function'))
  }
  # read the shape file
  edge <- st_read(dsn = mydestfile, quiet = TRUE)
  # lowercase column names
  colnames(edge) %<>% str_to_lower(.)
  
  # add file name and year
  edge$fname <- mydestfile %>% basename() %>% str_remove("_edges.zip")
  edge$year <- str_split(string = basename(mydestfile), pattern = "_", simplify = TRUE)[2] %>% as.integer()
  
   # read the shapefile
    edge <- st_read(dsn = i, quiet = TRUE)
    # lowercase
    colnames(edge) %<>% str_to_lower(.)
    # year, etc
    edge$fname <- basename(i) %>% file_path_sans_ext()
    edge$year <- myyear %>% as.integer()
    edge$state_abbr <- mystatename
    # seem to be some non-UTF8 characters
    edge$fullname <- iconv(edge$fullname)
  
  return(edge)
}

# Get a single file for table formatting
E <- function(conn = U){
  
  edge <- f_import_reformat_edges()
  
  # one record for writing to db for structure
  edgex <- edge[1,]
  
  # write to a template table, just for structure
  O <- dbExecute(conn = U, statement = "drop table if exists tmp.edge_template cascade;")
  # state
  edgex$state_abbr <- "xx"
  st_write(obj = edgex, dsn = dsn,
           layer = "edge_template",
           layer_options = c("SCHEMA=tmp", "GEOMETRY_NAME=geom"),
           quiet = TRUE)
  
  # create the structure for main and year edges tables in edges schema
  O <- dbExecute(conn = U, statement = "
        drop table if exists edges.edge cascade")
  O <- dbExecute(conn = U, statement = 
        "--like the template
        create table edges.edge (like tmp.edge_template) partition by list (year)")
}

# year tables. Run once for each year.
f_edges_partitions_year <- function(years){
  
  for (YR in years) {
    # each year's table, partitioned by state
    sql2 <- "create table if not exists edges.edge_xYRx partition of edges.edge for values in (xYRx) partition by list(statefp);" %>%
      str_replace_all(pattern = "xYRx", replacement = YR %>% as.character())
    message(sql2)
    O <- dbExecute(conn = U, statement = sql2)
  }
}

# state tables. run once for each year
f_edges_partitions_year_state <- function(years, 
                                          state_abbr_list){
  
  fips_filtered <- fips_codes %>%
    filter(state %in% toupper(state_abbr_list)) %>% 
    dplyr::select(state_code) %>% unique()
  states_fips <- fips_filtered$state_code
  
  for (YR in years) {
    # create each year-state table
    for (ST in states_fips) {
      sql3 <- "create table if not exists edges_state.edge_xYRx_xSTx partition of edges.edge_xYRx for values in ('xSTx') partition by list(countyfp);" %>%
        str_replace_all(pattern = "xYRx", replacement = YR %>% as.character()) %>%
        str_replace_all(pattern = "xSTx", ST)
      message(sql3)
      O <- dbExecute(conn = U, statement = sql3)
    }
  }
}

f_edges_partitions_year_state_county <- function(years, state_abbr_list){
  
  counties_fips <- fips_codes %>% filter(state %in% toupper(state_abbr_list))
  for (YR in years) {
    # for each yer-state-county
    for (CTY in 1:nrow(counties_fips)) {
      #        for(CTY in 1){
      state_fips <- counties_fips$state_code[CTY]
      county_fips <- counties_fips$county_code[CTY]
      # message(paste(YR, ST, state_fips, county_fips))
      sql4 <- "create table if not exists edges_county.edge_xYRx_xSTx_xCTx partition of edges_state.edge_xYRx_xSTx for values in ('xCTx');" %>%
        str_replace_all(pattern = "xYRx", replacement = YR %>% as.character()) %>%
        str_replace_all(pattern = "xSTx", state_fips) %>%
        str_replace_all(pattern = "xCTx", county_fips)
      message(sql4)
      O <- dbExecute(conn = U, statement = sql4)
    }
  }
}

f_edges_create_partitions <- function(years, state_abbr_list){
  f_edges_partitions_year(years)
  f_edges_partitions_year_state(years, 
                                state_abbr_list)
  f_edges_partitions_year_state_county(years, 
                                       state_abbr_list)
}

# lowest level tables (year x county), create and insert records
f_upsert_shapefiles <- function(years = years, conn = U, state_abbr_list = state_abbr_list ){
  # fips states
  state_fips <- fips_codes %>%  filter(state %in% toupper(state_abbr_list)) %>% dplyr::select(state, state_code) %>% terra::unique()
  state_code <- unique(state_fips$state_code)
  # a list of all the shape files for this year
  
  fnames_shp <- list.files("H:/projects/beh_nwi_smartmap/gisdata/us_census/edges", pattern = "*.shp$", recursive = TRUE, full.names = TRUE)

  # Update your filtering
  fnames_shp <- fnames_shp[
    str_detect(fnames_shp, paste0("_", state_code, "\\d{3}_edges\\.shp$"))
  ]
  
  pattern_years <- paste0(years, collapse = "|")
  fnames_year <-str_subset(fnames_shp, pattern_years)

  # create a lowest level detail (county) table for each shape file
  #for (j in 1){
  for (j in 1:length(fnames_year)){
    # get the shape file name and print a message
    i <- fnames_year[j]
    message(paste("processing", j, "of", length(fnames_year), i))
    # start time
    t0 <- Sys.time()
    # basename of the shape file with and w/o file extension
    bn <- basename(i)
    bn_noext <- file_path_sans_ext(bn)
    # split basename into pieces by underscore and then get pieces of name
    fname_parts <- bn %>% str_split(pattern = "[[:punct:]]", simplify = TRUE)
    # year
    myyear <- fname_parts[2]
    # state and county
    mystatefips <- fname_parts[3] %>% str_sub(start = 1, end = 2)
    mycountyfips <- fname_parts[3] %>% str_sub(start = 3)
    mystatename <- state_fips %>% dplyr::filter(state_code == mystatefips) %>% pull(state) %>% str_to_lower()
    # construct a table name for the county
    mytablename <- str_c("edges_county.edge", myyear, mystatefips, mycountyfips, sep = "_")
     # query to create table
    sql1 <- "create table if not exists xTNx partition of edges.edge_xYx_xSNx for values in ('xBNx')" %>%
      str_replace_all(pattern = "xTNx", mytablename) %>%
      str_replace_all(pattern = "xBNx", bn_noext) %>%
      str_replace_all(pattern = "xYx", myyear) %>%
      str_replace_all(pattern = "xSNx", mystatefips)
    sql2 <- "create index if not exists idx_xBNx on xTNx using gist(geom)" %>%
      str_replace_all(pattern = "xTNx", mytablename) %>%
      str_replace_all(pattern = "xBNx", bn_noext) %>%
      str_replace_all(pattern = "xYx", myyear) %>%
      str_replace_all(pattern = "xSNx", mystatefips)
    message(paste("    ", sql1, sql2))
    # create the table
    O <- dbExecute(conn = conn, statement = sql1)
    O <- dbExecute(conn = conn, statement = sql2)
    
    # unique id constraint
    cnst_sql <- "select count(*) = 1 as cns from pg_constraint WHERE conname = 'cnst_xCNSTx';" %>% str_replace_all("xCNSTx", bn_noext)
    cnst <- dbGetQuery(conn = conn, statement = cnst_sql)$cns
    if(!cnst){
      message("\tconstraint")
      sqlid <- "alter table xTNx add constraint cnst_xBNx unique(tlid);" %>%
        str_replace_all(pattern = "xTNx", replacement = mytablename) %>%
        str_replace_all(pattern = "xBNx", bn_noext)
      O <- dbExecute(conn = conn, statement = sqlid)
    }
    # read the shapefile
    edge <- st_read(dsn = i, quiet = TRUE)
    # lowercase
    colnames(edge) %<>% str_to_lower(.)
    # year, etc
    edge$fname <- basename(i) %>% file_path_sans_ext()
    edge$year <- myyear %>% as.integer()
    edge$state_abbr <- mystatename
    # seem to be some non-UTF8 characters
    edge$fullname <- iconv(edge$fullname)
    
    # fix colnames for 2007
    if(FALSE %in% (c('persist', 'gcseflg', 'offsetl', 'offsetr', 'tnidf', 'tnidt', 'divroad') %in% colnames(edge))){
      edge <- edge %>%
        mutate(persist = NA, 
               gcseflg = NA, 
               offsetl = NA,
               offsetr = NA, 
               tnidf = as.numeric(NA), 
               tnidt = as.numeric(NA), 
               divroad = NA) %>%
      dplyr::select(any_of(c('statefp', 'countyfp', 'tlid', 'tfidl', 'tfidr', 'mtfcc',    
                    'fullname','smid','lfromadd', 'ltoadd', 'rfromadd', 'rtoadd',    
                    'zipl', 'zipr',  'featcat', 'hydroflg', 'railflg', 'roadflg',   
                   'olfflg', 'passflg', 'divroad', 'exttyp', 'ttyp', 'deckedroad',
                    'artpath', 'persist', 'gcseflg', 'offsetl', 'offsetr', 'tnidf',     
                    'tnidt','geometry','fname','year', 'state_abbr')))
    }
    
         
    # write
    message("\twrite")
    # write to tmp
    #dsn = "PG:host=db dbname=walk_db user=aybloom password=census port=5432"
    st_write(obj = edge, dsn = dsn, layer = "tmp.edge", delete_layer = TRUE, layer_options = "GEOMETRY_NAME=geom")
    # upsert
    message("\tupsert")
    sql2 <- "insert into xTNx 
            SELECT *  from tmp.edge on conflict do nothing;" %>%
      str_replace_all(pattern = "xTNx", replacement = mytablename)
    O <- dbExecute(conn = conn, statement = sql2)
  
    
    ## error inserting tnidt column....
    tmp_edge <- dbGetQuery(conn, statement = 'SELECT * from tmp.edge;')
    cnty_edge <- dbGetQuery(conn, statement = 'SELECT * from edges_county.edge_2024_53_001;')
    
    t1 <- Sys.time()
    message(paste("   ", difftime(t1, t0, units = "secs") %>% round(1), "seconds"))
  }
}

# snap to grid, 0.000001 deg =~ 0.11 m at the equator
f_snapper <- function(conn = U){
  # does the column exist?
  if (!dbGetQuery(conn = conn, statement = "select count(*) from information_schema.columns where table_schema || table_name || column_name =
                    'edgesedgegeom_snap';")) {
    O <- dbExecute(conn = conn, statement = "alter table edges.edge add column geom_snap geometry(linestring, 4269);")
  }
}

# lowest level geom and other index
f_index <- function(geomfield = 'geom', conn = U){
  fnames <- dbGetQuery(conn = conn, statement = "select table_schema, table_name from information_schema.tables where table_schema = 'edges' order by table_schema, table_name;")

    for(i in 1:nrow(fnames)){
    schema <- fnames$table_schema[i]
    tablename <- fnames$table_name[i]
    sql1 <- "
        create index if not exists idx_xTNx_xGEOMx on xSCx.xTNx using gist(xGEOMx);"  %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xSCx", schema) %>%
      str_replace_all(pattern = "xGEOMx", geomfield)
    sql2 <- "
        create index if not exists idx_xTNx_xGEOMx_snap on xSCx.xTNx using gist(xGEOMx_snap);"  %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xSCx", schema) %>%
      str_replace_all(pattern = "xGEOMx", geomfield)
    sql3 <- "
        create index if not exists idx_xTNx_year on xSCx.xTNx using btree(year);"  %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xSCx", schema) %>%
      str_replace_all(pattern = "xGEOMx", geomfield)
    sql4 <- "
        create index if not exists idx_xTNx_mtfcc on xSCx.xTNx using btree(mtfcc);"  %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xSCx", schema) %>%
      str_replace_all(pattern = "xGEOMx", geomfield)
    sql5 <- "
        create index if not exists idx_xTNx_tlid on xSCx.xTNx using btree(tlid);" %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xSCx", schema) %>%
      str_replace_all(pattern = "xGEOMx", geomfield)
    #message(paste0(sql1, sql2, sql3, sql4, sql5))
    O <- dbExecute(conn = conn, statement = sql1)
    O <- dbExecute(conn = conn, statement = sql2)
    O <- dbExecute(conn = conn, statement = sql3)
    O <- dbExecute(conn = conn, statement = sql4)
    O <- dbExecute(conn = conn, statement = sql5)
    
    
  }
}

# a generic function to create btree schemas
f_index_btree <- function(fieldname, conn = U){
  fnames <- dbGetQuery(conn = conn, statement = "select table_schema, table_name from information_schema.tables where table_schema = 'edges' order by table_schema, table_name;")
  for(i in 1:nrow(fnames)){
    schema <- fnames$table_schema[i]
    tablename <- fnames$table_name[i]
    sql <- "create index if not exists idx_xTNx_xFLDx on xSCx.xTNx using btree(xFLDx);
        " %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xSCx", schema) %>%
      str_replace_all(pattern = "xFLDx", fieldname)
    message(sql)
    O <- dbGetQuery(conn = conn, statement = sql)
  }
}

# a generic function to create gist schemas
f_index_gist <- function(fieldname, conn = watr){
  fnames <- dbGetQuery(conn = conn, statement = "select table_schema, table_name from information_schema.tables where table_schema = 'edge_detail' order by table_schema, table_name;")
  for(i in 1:nrow(fnames)){
    schema <- fnames$table_schema[i]
    tablename <- fnames$table_name[i]
    sql <- "create index if not exists idx_xTNx_xFLDx on xSCx.xTNx using gist(xFLDx);
        " %>%
      str_replace_all(pattern = "xTNx", replacement = tablename) %>%
      str_replace_all(pattern = "xSCx", schema) %>%
      str_replace_all(pattern = "xFLDx", fieldname)
    message(sql)
    O <- dbGetQuery(conn = conn, statement = sql)
  }
}
