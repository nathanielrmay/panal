library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA    <- "nba"
TARGET_TABLE     <- "espn_nba_scoreboard"
source("../db_config.R")
SEASON_START_STR <- "2025-10-22" # NBA 2025-26 Season Start Date

# ==========================================
# 1. DATABASE CONNECTION & DATE LOGIC
# ==========================================
con <- dbConnect(RPostgres::Postgres(),
                 dbname = DB_NAME, host = DB_HOST,
                 user = DB_USER, password = DB_PASS)

target_id <- Id(schema = TARGET_SCHEMA, table = TARGET_TABLE)

# Determine Start Date
start_date <- as.Date(SEASON_START_STR)

if (dbExistsTable(con, target_id)) {
  # Check for the latest date in the database
  tryCatch({
    query <- paste0("SELECT MAX(game_date) as max_date FROM ", TARGET_SCHEMA, ".", TARGET_TABLE)
    res <- dbGetQuery(con, query)
    
    if (!is.na(res$max_date)) {
      # Start from the last recorded date to ensure updates (morning vs evening pulls)
      start_date <- as.Date(res$max_date)
      print(paste("Found existing data. Resuming from:", start_date))
    } else {
      print("Table exists but is empty. Starting from season start.")
    }
  }, error = function(e) {
    print(paste("Error querying max date:", e$message))
    print("Defaulting to season start.")
  })
} else {
  print("Table does not exist. Starting from season start.")
}

end_date <- Sys.Date()
print(paste("Processing range:", start_date, "to", end_date))

# ==========================================
# 2. PROCESSING LOOP
# ==========================================
curr_date <- start_date

while (curr_date <= end_date) {
  search_str <- format(curr_date, "%Y%m%d")
  delete_date_str <- format(curr_date, "%Y-%m-%d")
  
  print(paste("--------------------------------------------------"))
  print(paste("Processing Date:", search_str))
  
  # Fetch Data
  tryCatch({
    df <- hoopR::espn_nba_scoreboard(season = search_str)
    df <- as.data.frame(df)
    
    if (nrow(df) > 0) {
      print(paste("  Fetched", nrow(df), "games."))
      
      # JSON Flattening
      df <- df %>%
        mutate(across(where(is.list), ~ sapply(.x, function(y) {
          if (is.null(y) || length(y) == 0) return(NA)
          jsonlite::toJSON(y, auto_unbox = TRUE)
        })))
        
      # Clean up old data for this specific date (handling updates)
      if (dbExistsTable(con, target_id)) {
        delete_query <- paste0("DELETE FROM ", TARGET_SCHEMA, ".", TARGET_TABLE, 
                        " WHERE game_date::date = '", delete_date_str, "'::date")
        
        # Execute Delete
        rows_deleted <- dbExecute(con, delete_query)
        print(paste("  Cleared", rows_deleted, "old records for", delete_date_str))
      }
      
      # Write to DB (Append)
      dbWriteTable(con, target_id, df, append = TRUE, row.names = FALSE)
      print("  Success: Data appended to database.")
      
    } else {
      print("  No games found for this date.")
    }
    
  }, error = function(e) {
    print(paste("  ERROR fetching/writing data for", search_str, ":", e$message))
  })
  
  # Increment Date
  curr_date <- curr_date + 1
  
  # Optional: Sleep to be nice to the API
  Sys.sleep(1) 
}

dbDisconnect(con)
print("==========================================")
print("Script Complete.")

