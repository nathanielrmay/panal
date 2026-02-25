library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA  <- "ncaamb"
TARGET_TABLE   <- "hoopsr_player_box_scores"
CURRENT_SEASON <- 2026
source("../db_config.R")

# ==========================================
# 1. FETCH CURRENT SEASON DATA
# ==========================================
print(paste("Fetching fresh data for season:", CURRENT_SEASON))
df <- hoopR::load_mbb_player_box(seasons = CURRENT_SEASON)

# CRITICAL FIX: Convert to standard data frame so DB driver understands it
df <- as.data.frame(df)

# JSON Flattening (Required for PBP data)
df <- df %>%
  mutate(across(where(is.list), ~ sapply(.x, function(y) {
    if (is.null(y) || length(y) == 0) return(NA)
    jsonlite::toJSON(y, auto_unbox = TRUE)
  })))

# ==========================================
# 2. DATABASE OPERATIONS
# ==========================================
con <- dbConnect(RPostgres::Postgres(),
                 dbname = DB_NAME, host = DB_HOST,
                 user = DB_USER, password = DB_PASS)

target_id <- Id(schema = TARGET_SCHEMA, table = TARGET_TABLE)

# Check if table exists. If not, we do a full load including 2025.
if (!dbExistsTable(con, target_id)) {
  print("Table doesn't exist. Performing initial load of 2025 and 2026...")
  df_hist <- hoopR::load_mbb_player_box(seasons = 2025:2026)

  # CRITICAL FIX: Convert to standard data frame
  df_hist <- as.data.frame(df_hist)

  # (Apply the same JSON flattening to df_hist here)
  df_hist <- df_hist %>% mutate(across(where(is.list), ~ sapply(.x, function(y) {
    if (is.null(y) || length(y) == 0) return(NA)
    jsonlite::toJSON(y, auto_unbox = TRUE)
  })))

  dbWriteTable(con, target_id, df_hist, overwrite = TRUE, row.names = FALSE)

} else {
  # TABLE EXISTS: Targeted Update
  print(paste("Cleaning out old", CURRENT_SEASON, "data..."))

  # Use your SQL skills here:
  query <- paste0("DELETE FROM ", TARGET_SCHEMA, ".", TARGET_TABLE,
                  " WHERE season = ", CURRENT_SEASON)
  dbExecute(con, query)

  print("Appending fresh season data...")
  dbWriteTable(con, target_id, df, append = TRUE, row.names = FALSE)
}

dbDisconnect(con)
print("Update Complete!")
