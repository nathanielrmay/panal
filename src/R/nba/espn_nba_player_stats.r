library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA    <- "nba"
TARGET_TABLE     <- "espn_nba_player_stats"
SOURCE_TABLE     <- "espn_nba_player_box" # Source for athlete_ids
DB_NAME          <- "panal"
DB_USER          <- "than"
DB_PASS          <- "fishy"
DB_HOST          <- "localhost"
CURRENT_SEASON   <- 2026

# ==========================================
# 1. DATABASE CONNECTION & ATHLETE LIST
# ==========================================
con <- dbConnect(RPostgres::Postgres(),
                 dbname = DB_NAME, host = DB_HOST,
                 user = DB_USER, password = DB_PASS)

print("Fetching unique athlete IDs from player box scores...")

# Get distinct athlete IDs to process
# We cast to integer to ensure compatibility with the function
query_athletes <- paste0("SELECT DISTINCT athlete_id FROM ", TARGET_SCHEMA, ".", SOURCE_TABLE, 
                        " WHERE athlete_id IS NOT NULL")

athletes_df <- dbGetQuery(con, query_athletes)

if (nrow(athletes_df) == 0) {
  print("No athletes found in player box scores.")
  dbDisconnect(con)
  quit(save = "no")
}

print(paste("Found", nrow(athletes_df), "athletes to process."))

target_id <- Id(schema = TARGET_SCHEMA, table = TARGET_TABLE)

# ==========================================
# 2. PROCESSING LOOP
# ==========================================
# Optional: Shuffle the list to avoid hitting the same API endpoint pattern if that matters
# athletes_df <- athletes_df[sample(nrow(athletes_df)), , drop = FALSE]

for (i in 1:nrow(athletes_df)) {
  aid <- athletes_df$athlete_id[i]
  
  print(paste("--------------------------------------------------"))
  print(paste("Processing Athlete ID:", aid, "(", i, "of", nrow(athletes_df), ")"))
  
  tryCatch({
    # Fetch Data
    df <- hoopR::espn_nba_player_stats(athlete_id = aid, year = CURRENT_SEASON)
    df <- as.data.frame(df)
    
    if (nrow(df) > 0) {
      print(paste("  Fetched stats record."))
      
      # JSON Flattening
      df <- df %>%
        mutate(across(where(is.list), ~ sapply(.x, function(y) {
          if (is.null(y) || length(y) == 0) return(NA)
          jsonlite::toJSON(y, auto_unbox = TRUE)
        })))
        
      # Clean up old data for this specific athlete
      if (dbExistsTable(con, target_id)) {
        delete_query <- paste0("DELETE FROM ", TARGET_SCHEMA, ".", TARGET_TABLE, 
                        " WHERE athlete_id = '", aid, "'")
        dbExecute(con, delete_query)
      }
      
      # Write to DB
      dbWriteTable(con, target_id, df, append = TRUE, row.names = FALSE)
      print("  Success: Data appended.")
      
    } else {
      print("  No stats found for this athlete.")
    }
    
  }, error = function(e) {
    print(paste("  ERROR for Athlete ID", aid, ":", e$message))
  })
  
  Sys.sleep(0.5) # Be gentle to the API
}

dbDisconnect(con)
print("==========================================")
print("Script Complete.")
