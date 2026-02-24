library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA  <- "ncaamb"
TARGET_TABLE   <- "espn_scoreboard"
DB_NAME        <- "panal"
DB_USER        <- "than"
DB_PASS        <- "fishy"
DB_HOST        <- "localhost"

# Date format YYYYMMDD
SEARCH_DATE    <- "20260123" 

# ==========================================
# 1. FETCH DATA
# ==========================================
print(paste("Fetching ESPN MBB Scoreboard for date:", SEARCH_DATE))

# Correct argument is 'season', which accepts YYYYMMDD strings
df <- hoopR::espn_mbb_scoreboard(season = SEARCH_DATE)

# Convert to standard data frame
df <- as.data.frame(df)

if (nrow(df) == 0) {
  print("No games found for this date.")
} else {
  print(paste("Fetched", nrow(df), "games."))
}

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
  print("Success: Scoreboard data written to database.")
}, error = function(e) {
  print(paste("Database write error:", e$message))
})

dbDisconnect(con)
print("Script Complete.")
