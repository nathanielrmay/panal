library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA  <- "nba"
TARGET_TABLE   <- "nba_game_log"
DB_NAME        <- "panal"
DB_USER        <- "than"
DB_PASS        <- "fishy"
DB_HOST        <- "localhost"
SEASON         <- "2025-26"

# ==========================================
# 1. FETCH DATA
# ==========================================
print(paste("Fetching NBA League Game Log for season:", SEASON))

df <- hoopR::nba_leaguegamelog(season = SEASON)

# Convert to standard data frame
df <- as.data.frame(df)

print(paste("Fetched", nrow(df), "game log entries."))

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
  dbWriteTable(con, target_id, df, overwrite = TRUE, row.names = FALSE)
  print("Success: Data written to database.")
}, error = function(e) {
  print(paste("Database write error:", e$message))
})

dbDisconnect(con)
print("Script Complete.")
