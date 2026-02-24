library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(stringr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA  <- "ncaamb"
TEAMS_TABLE    <- "ncaa_teams"
CONF_TABLE     <- "ncaa_conferences"
DB_NAME        <- "panal"
DB_USER        <- "than"
DB_PASS        <- "fishy"
DB_HOST        <- "localhost"
SEASON         <- 2025

# ==========================================
# 1. FETCH DATA
# ==========================================
print(paste("Fetching NCAA MBB Teams for season:", SEASON))

df <- hoopR::ncaa_mbb_teams(year = SEASON)

# CRITICAL FIX for "hoopR_data" signature error
# Convert the custom hoopR class/tibble into a standard R data.frame immediately
df <- as.data.frame(df)

print(paste("Fetched", nrow(df), "rows."))

# ==========================================
# 2. FIX TEAM IDS
# ==========================================
print("Extracting Team IDs from URLs...")

# Extract digits following "/teams/"
# Example: "/teams/590530" -> "590530"
df <- df %>%
  mutate(
    extracted_id = str_extract(team_url, "(?<=/teams/)\\d+"),
    team_id = ifelse(is.na(team_id) | team_id == "NA" | team_id == "", extracted_id, team_id)
  ) %>%
  select(-extracted_id)

missing_count <- sum(is.na(df$team_id))
print(paste("Missing IDs after fix:", missing_count))

# ==========================================
# 3. EXTRACT CONFERENCES
# ==========================================
print("Extracting unique conferences...")

conferences <- df %>%
  select(conference_id, conference_name = conference, division) %>%
  distinct() %>%
  filter(!is.na(conference_id))

# Convert conferences to standard data.frame too
conferences <- as.data.frame(conferences)

print(paste("Found", nrow(conferences), "unique conferences."))

# ==========================================
# 4. PREPARE FOR DB (Flattening)
# ==========================================
# Helper to flatten list columns
flatten_df <- function(d) {
  # Ensure it's a data frame first (redundant but safe)
  d <- as.data.frame(d)

  d %>%
    mutate(across(where(is.list), ~sapply(.x, function(y) {
      if (is.null(y) || length(y) == 0) return(NA)
      jsonlite::toJSON(y, auto_unbox = TRUE)
    })))
}

df_teams <- flatten_df(df)
df_conf  <- flatten_df(conferences)

# ==========================================
# 5. WRITE TO DATABASE
# ==========================================
con <- dbConnect(RPostgres::Postgres(),
                 dbname = DB_NAME, host = DB_HOST,
                 user = DB_USER, password = DB_PASS)

# Write Teams
print(paste("Writing teams to", TEAMS_TABLE))
tryCatch({
  dbWriteTable(con, Id(schema = TARGET_SCHEMA, table = TEAMS_TABLE), df_teams, overwrite = TRUE, row.names = FALSE)
  print("Teams table written successfully.")
}, error = function(e) {
  print(paste("Error writing teams table:", e$message))
})

# Write Conferences
print(paste("Writing conferences to", CONF_TABLE))
tryCatch({
  dbWriteTable(con, Id(schema = TARGET_SCHEMA, table = CONF_TABLE), df_conf, overwrite = TRUE, row.names = FALSE)
  print("Conferences table written successfully.")
}, error = function(e) {
  print(paste("Error writing conferences table:", e$message))
})


dbDisconnect(con)
print("SUCCESS: NCAA Teams and Conferences have been populated.")
