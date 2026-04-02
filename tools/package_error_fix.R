#### FIX this error: Error: package or namespace load failed for ‘RPostgres’ in inDL(x, as.logical(local), as.logical(now), ...):
#unable to load shared object 'C:/Program Files/R/R-4.5.1/library/rlang/libs/x64/rlang.dll':
#  LoadLibrary failure: The specified procedure could not be found.
######

## need to check and update libpaths: 
.libPaths()
.libPaths("C:/Program Files/R/R-4.3.2/library")

# rm(list = ls())
# options(install.packages.compile.from.source = 'never')
# install.packages('rlang', type = 'binary')

### next restart R ###
# library(rlang)

install.packages(c('RPostgres','DBI', 'leaflet', 'glue', 'tidyverse', 'tidycensus'), 
                 dependencies = TRUE, type = 'binary')
#install.packages('tidycensus', dependencies = TRUE, type = 'binary')
