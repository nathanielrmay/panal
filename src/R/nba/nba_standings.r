library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA  <- "nba"
TARGET_TABLE   <- "nba_leaguestandings"
DB_NAME        <- "panal"
DB_USER        <- "than"
DB_PASS        <- "fishy"
DB_HOST        <- "localhost"
SEASON         <- "2025-26" 

# ==========================================
# 1. FETCH DATA
# ==========================================
print(paste("Fetching NBA League Standings for season:", SEASON))

df <- hoopR::nba_leaguestandings(season = SEASON)

# Convert to standard data frame
df <- as.data.frame(df)

print(paste("Fetched", nrow(df), "standings entries."))

# ==========================================
# 2. PREPARE FOR DB (Flattening)
# ==========================================
df <- df %>%
  mutate(across(where(is.list), ~ sapply(.x, function(y) {
    if (is.null(y) || length(y) == 0) return(NA)
    jsonlite::toJSON(y, auto_unbox = TRUE)
  })))

# ==========================================
# 3. DATABASE OPERATIONS
# ==========================================
con <- dbConnect(RPostgres::Postgres(),
                 dbname = DB_NAME, host = DB_HOST,
                 user = DB_USER, password = DB_PASS)

target_id <- Id(schema = TARGET_SCHEMA, table = TARGET_TABLE)

print(paste("Writing to table:", TARGET_TABLE))

tryCatch({
  # Check if table exists to prevent dropping it (which fails due to MV dependency)
  if (dbExistsTable(con, target_id)) {
    print("Table exists. Truncating and appending to preserve dependencies...")
    dbExecute(con, paste0("TRUNCATE TABLE ", TARGET_SCHEMA, ".", TARGET_TABLE))
    dbWriteTable(con, target_id, df, append = TRUE, row.names = FALSE)
  } else {
    print("Table does not exist. Creating...")
    dbWriteTable(con, target_id, df, overwrite = TRUE, row.names = FALSE)
  }

  print("Success: Data written to database.")
  
  # Refresh Materialized View
  print("Refreshing Materialized View: nba.standings...")
  dbExecute(con, "REFRESH MATERIALIZED VIEW nba.standings")
  print("Success: Materialized view refreshed.")
  
}, error = function(e) {
  print(paste("Database operation error:", e$message))
})

dbDisconnect(con)
print("Script Complete.")
