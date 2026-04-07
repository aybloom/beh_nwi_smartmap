library(DBI)

connectdb <- function(dbname = 'infousa', 
                      host = 'localhost', 
                      port = 5432, 
                      user = 'postgres') {
  dbConnect(dbDriver("PostgreSQL"), 
            dbname = dbname, 
            host = host, 
            port = port, 
            user = user)
}

