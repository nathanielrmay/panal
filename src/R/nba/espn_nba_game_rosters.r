library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA    <- "nba"
TARGET_TABLE     <- "espn_nba_game_rosters"
SOURCE_TABLE     <- "espn_nba_scoreboard"
DB_NAME          <- "panal"
DB_USER          <- "than"
DB_PASS          <- "fishy"
DB_HOST          <- "localhost"
SEASON_START_STR <- "2025-10-22"

# ==========================================
# 1. DATABASE CONNECTION & LOGIC
# ==========================================
con <- dbConnect(RPostgres::Postgres(),
                 dbname = DB_NAME, host = DB_HOST,
                 user = DB_USER, password = DB_PASS)

target_id <- Id(schema = TARGET_SCHEMA, table = TARGET_TABLE)
source_id <- Id(schema = TARGET_SCHEMA, table = SOURCE_TABLE)

# Determine Start Date
start_date <- as.Date(SEASON_START_STR)

if (dbExistsTable(con, target_id)) {
  tryCatch({
    query <- paste0("SELECT MAX(s.game_date) as max_date ",
                    "FROM ", TARGET_SCHEMA, ".", SOURCE_TABLE, " s ",
                    "JOIN ", TARGET_SCHEMA, ".", TARGET_TABLE, " r ON s.game_id = r.game_id")
    res <- dbGetQuery(con, query)
    
    if (!is.na(res$max_date)) {
      start_date <- as.Date(res$max_date)
      print(paste("Found existing roster data. Resuming from games on:", start_date))
    }
  }, error = function(e) {
    print(paste("Error determining start date:", e$message))
  })
}

# 2. GET GAME IDS TO PROCESS
query_games <- paste0("SELECT DISTINCT game_id, game_date FROM ", TARGET_SCHEMA, ".", SOURCE_TABLE, 
                     " WHERE game_date::date >= '", start_date, "'::date ",
                     " ORDER BY game_date ASC")

games_to_process <- dbGetQuery(con, query_games)

if (nrow(games_to_process) == 0) {
  print("No games found in scoreboard to process.")
  dbDisconnect(con)
  quit(save = "no")
}

print(paste("Processing", nrow(games_to_process), "games..."))

# ==========================================
# 3. PROCESSING LOOP
# ==========================================
for (i in 1:nrow(games_to_process)) {
  gid <- games_to_process$game_id[i]
  gdate <- games_to_process$game_date[i]
  
  print(paste("--------------------------------------------------"))
  print(paste("Game ID:", gid, "(", gdate, ")"))
  
  tryCatch({
    # Fetch Data
    df <- hoopR::espn_nba_game_rosters(game_id = gid)
    df <- as.data.frame(df)
    
    if (nrow(df) > 0) {
      print(paste("  Fetched", nrow(df), "roster records."))
      
      # JSON Flattening
      df <- df %>%
        mutate(across(where(is.list), ~ sapply(.x, function(y) {
          if (is.null(y) || length(y) == 0) return(NA)
          jsonlite::toJSON(y, auto_unbox = TRUE)
        })))
        
      # Clean up old data
      if (dbExistsTable(con, target_id)) {
        delete_query <- paste0("DELETE FROM ", TARGET_SCHEMA, ".", TARGET_TABLE, 
                        " WHERE game_id = '", gid, "'")
        dbExecute(con, delete_query)
      }
      
      # Write to DB
      dbWriteTable(con, target_id, df, append = TRUE, row.names = FALSE)
      print("  Success: Data appended.")
      
    } else {
      print("  No roster data found for this game.")
    }
    
  }, error = function(e) {
    if (grepl("athlete_roster_df", e$message)) {
      print(paste("  No roster data available for Game ID", gid, "(handled)"))
    } else {
      print(paste("  ERROR for Game ID", gid, ":", e$message))
    }
  })
  
  Sys.sleep(0.5) 
}

dbDisconnect(con)
print("==========================================")
print("Script Complete.")
