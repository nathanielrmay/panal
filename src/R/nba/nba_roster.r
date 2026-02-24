library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)
library(purrr)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA  <- "nba"
TARGET_TABLE   <- "nba_team_roster"
DB_NAME        <- "panal"
DB_USER        <- "than"
DB_PASS        <- "fishy"
DB_HOST        <- "localhost"
# SEASON       <- "2025-26" # Commented out to let hoopR use default

# ==========================================
# 1. FETCH DATA
# ==========================================
print("Connecting to DB to fetch Team IDs...")
con <- dbConnect(RPostgres::Postgres(),
                 dbname = DB_NAME, host = DB_HOST,
                 user = DB_USER, password = DB_PASS)

# Fetch team IDs from our local table instead of the API
# This is more robust if the teams API endpoint is flaky
tryCatch({
  # Check if table exists first
  if (dbExistsTable(con, Id(schema = TARGET_SCHEMA, table = "nba_teams"))) {
    teams_df <- dbReadTable(con, Id(schema = TARGET_SCHEMA, table = "nba_teams"))
    team_ids <- teams_df$team_id
    print(paste("Loaded", length(team_ids), "Team IDs from database."))
  } else {
    stop("Table 'nba.nba_teams' not found! Run nba_teams.r first.")
  }
}, error = function(e) {
  dbDisconnect(con) # Ensure disconnect on error
  stop("Failed to read teams from DB: ", e$message)
})

print(paste("Fetching rosters for", length(team_ids), "teams..."))

# Function to fetch with retry and delay
fetch_roster_safe <- function(tid, index, total) {
  # Progress log
  message(sprintf("[%d/%d] Fetching team ID: %s", index, total, tid))
  
  # Rate limit pause (very important for NBA API)
  Sys.sleep(1.5) 
  
  tryCatch({
    # Explicitly pass season to avoid internal hoopR errors
    roster_data <- hoopR::nba_commonteamroster(team_id = tid, season = "2025-26")
    
    if (!is.null(roster_data$CommonTeamRoster)) {
      df <- as.data.frame(roster_data$CommonTeamRoster)
      return(df)
    } else {
      message(paste("  -> No 'CommonTeamRoster' data for team", tid))
      return(NULL)
    }
  }, error = function(e) {
    message(paste("  -> Error fetching team", tid, ":", e$message))
    return(NULL)
  })
}

# Iterate manually to have better control (vs map_df which might fail eagerly)
results_list <- list()
for (i in seq_along(team_ids)) {
  res <- fetch_roster_safe(team_ids[i], i, length(team_ids))
  if (!is.null(res)) {
    results_list[[i]] <- res
  }
}

all_rosters <- bind_rows(results_list)

if (nrow(all_rosters) == 0) {
  dbDisconnect(con)
  stop("No roster data fetched successfully.")
}

print(paste("Fetched", nrow(all_rosters), "total player records."))

# ==========================================
# 2. PREPARE FOR DB (Flattening)
# ==========================================
# Flatten any list columns to JSON strings for Postgres compatibility
all_rosters <- all_rosters %>%
  mutate(across(where(is.list), ~ sapply(.x, function(y) {
    if (is.null(y) || length(y) == 0) return(NA)
    jsonlite::toJSON(y, auto_unbox = TRUE)
  })))

# ==========================================
# 3. DATABASE OPERATIONS
# ==========================================
# Connection is already open from step 1

# Ensure schema exists
dbExecute(con, paste0("CREATE SCHEMA IF NOT EXISTS ", TARGET_SCHEMA, ";"))

target_id <- Id(schema = TARGET_SCHEMA, table = TARGET_TABLE)

print(paste("Writing to table:", TARGET_TABLE))

tryCatch({
  dbWriteTable(con, target_id, all_rosters, overwrite = TRUE, row.names = FALSE)
  print("Success: Data written to database.")
}, error = function(e) {
  print(paste("Database write error:", e$message))
})

dbDisconnect(con)
print("Script Complete.")
