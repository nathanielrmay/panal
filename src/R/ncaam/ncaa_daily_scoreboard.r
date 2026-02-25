library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA  <- "ncaamb"
TARGET_TABLE   <- "ncaa_mbb_daily_scoreboard"
source("../db_config.R")

# NCAA source usually expects YYYY-MM-DD or specific format handled by hoopR
SEARCH_DATE    <- "2026-01-23" 

# ==========================================
# 1. FETCH DATA
# ==========================================
print(paste("Fetching NCAA Official Daily Scoreboard for:", SEARCH_DATE))

df <- hoopR::ncaa_mbb_daily_scoreboard(date = SEARCH_DATE, division = 1)

# Convert to standard data frame
df <- as.data.frame(df)

if (nrow(df) == 0) {
  print("No games found for this date on the NCAA scoreboard.")
} else {
  print(paste("Fetched", nrow(df), "games from NCAA source."))
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
  print("Success: NCAA Daily Scoreboard written to database.")
}, error = function(e) {
  print(paste("Database write error:", e$message))
})

dbDisconnect(con)
print("Script Complete.")

