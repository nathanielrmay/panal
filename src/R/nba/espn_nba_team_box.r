library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA    <- "nba"
TARGET_TABLE     <- "espn_nba_team_box"
SOURCE_TABLE     <- "espn_nba_scoreboard"
source("../db_config.R")
SEASON_START_STR <- "2025-10-22"

# ==========================================
# 1. DATABASE CONNECTION & LOGIC
# ==========================================
print("Connecting to DB...")
con <- dbConnect(RPostgres::Postgres(),
                 dbname = DB_NAME, host = DB_HOST,
                 user = DB_USER, password = DB_PASS)

target_id <- Id(schema = TARGET_SCHEMA, table = TARGET_TABLE)
source_id <- Id(schema = TARGET_SCHEMA, table = SOURCE_TABLE)

start_date <- as.Date(SEASON_START_STR)

if (dbExistsTable(con, target_id)) {
  tryCatch({
    query <- paste0("SELECT MAX(s.game_date) as max_date ",
                    "FROM ", TARGET_SCHEMA, ".", SOURCE_TABLE, " s ",
                    "JOIN ", TARGET_SCHEMA, ".", TARGET_TABLE, " t ON s.game_id = t.game_id")
    res <- dbGetQuery(con, query)
    
    if (!is.na(res$max_date)) {
      start_date <- as.Date(res$max_date)
      print(paste("Found existing team box data. Resuming from games on:", start_date))
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
games_to_process <- as.data.frame(games_to_process)

# CRITICAL FIX: Convert game_id to character to avoid integer64 issues
games_to_process$game_id <- as.character(games_to_process$game_id)

if (nrow(games_to_process) == 0) {
  print("No games found in scoreboard to process.")
  dbDisconnect(con)
  quit(save = "no")
}

print(paste("Processing", nrow(games_to_process), "games..."))
flush.console()

# ==========================================
# 4. PROCESSING LOOP
# ==========================================
print("Starting main loop...")
flush.console()

i <- 1
max_rows <- nrow(games_to_process)

while (i <= max_rows) {
  flush.console()
  
  gid <- games_to_process$game_id[i]
  print(paste("--------------------------------------------------"))
  print(paste("Iteration:", i, "of", max_rows, " - GameID:", gid))
  flush.console()
  
  tryCatch({
    print(paste("  Fetching GameID:", gid))
    flush.console()
    
    df <- hoopR::espn_nba_team_box(game_id = gid)
    df <- as.data.frame(df)
    
    if (nrow(df) > 0) {
      print(paste("  Fetched", nrow(df), "team records. Processing..."))
      flush.console()
      
      df <- df %>%
        mutate(across(where(is.list), ~ sapply(.x, function(y) {
          if (is.null(y) || length(y) == 0) return(NA)
          jsonlite::toJSON(y, auto_unbox = TRUE)
        })))
        
      if (dbExistsTable(con, target_id)) {
        delete_query <- paste0("DELETE FROM ", TARGET_SCHEMA, ".", TARGET_TABLE, 
                        " WHERE game_id = '", gid, "'")
        dbExecute(con, delete_query)
      }
      
      dbWriteTable(con, target_id, df, append = TRUE, row.names = FALSE)
      print("  Success: Data appended.")
      flush.console()
    } else {
      print("  No team data found.")
      flush.console()
    }
  }, error = function(e) {
    print(paste("  ERROR for Game ID", gid, ":", e$message))
    flush.console()
  })
  
  i <- i + 1
  Sys.sleep(0.5)
}

dbDisconnect(con)
print("Script Complete.")

