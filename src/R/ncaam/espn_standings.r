library(hoopR)
library(DBI)
library(RPostgres)
library(dplyr)
library(jsonlite)

# ==========================================
# CONFIGURATION
# ==========================================
TARGET_SCHEMA  <- "ncaamb"
TARGET_TABLE   <- "espn_standings"
source("../db_config.R")
SEASON         <- 2025 

# ==========================================
# 1. FETCH DATA
# ==========================================
print(paste("Fetching ESPN MBB Standings for season:", SEASON))

df <- hoopR::espn_mbb_standings(year = SEASON)

# Convert to standard data frame
df <- as.data.frame(df)

print(paste("Fetched", nrow(df), "standings entries."))

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
  print("Success: ESPN Standings data written to database.")
}, error = function(e) {
  print(paste("Database write error:", e$message))
})

dbDisconnect(con)
print("Script Complete.")

