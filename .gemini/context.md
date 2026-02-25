# Development Protocol
> **CRITICAL INSTRUCTION FOR AI AGENTS:** Review this document (`docs/project.md`) with every change to the codebase. After completing a significant task or reaching a stable milestone, you MUST:
> 1. **Commit**: Stage and commit your changes with a concise, descriptive message.
> 2. **Push**: Push the changes to the remote repository (`origin master`) immediately.
> 3. **Sync Parent**: After pushing here, also perform a commit in the parent 'everything' project to sync the project's metadata.

# Project Overview

## Intent
The goal of this project ("panal") is to build a robust pipeline for retrieving, storing, and analyzing sports data. The data will be used for statistical analysis and potentially for identifying value in betting markets.

## Future Development (Python)
I intend to expand the data collection capabilities using Python to interface with the following external APIs/sources:
- **KenPom**: Advanced co
- **Dunks and Threes**: Advanced basketball analytics.
- **The Odds API** (or similar): Real-time betting odds and lines.

## Existing Infrastructure (R Scripts)
The project currently uses a suite of R scripts located in `R_scripts/` to fetch data using the `hoopR` library and store it in a local PostgreSQL database.

### Common Pattern
Most scripts follow a consistent ETL pattern:
1.  **Configuration**: Define target schema (e.g., `nba`, `ncaamb`), table name, and database credentials.
2.  **Fetch**: Retrieve data for the current season (and history if initializing) using `hoopR`.
3.  **Process**: Convert results to a standard DataFrame and flatten nested JSON columns to ensure compatibility with PostgreSQL.
4.  **Storage**:
    - If the table does not exist: Perform a full historical load (e.g., 2025-2026).
    - If the table exists: Delete records for the current season and append the fresh data.

### Use the already created scripts as templates.
- NBA Scripts (`R_scripts/nba/`)
- NCAAM Scripts (`R_scripts/ncaam/`)

# Operational Notes
- **Deployment**: Everything is run on a **remote Netcup Ubuntu server**.
- **Database Tools**: 
  - **`dbhub-postgres-panal`**: This module is responsible for gathering sports data (via R and Python scripts) and writing it to this database.
- **Testing**: Since I am an AI and cannot log into the remote server via SSH or FTP myself, all testing of server-side connectivity or system-level changes must be performed in coordination with the user.
- **FTP**: The server uses VSFTPD with virtual users. Configuration is managed in `/var/www/nmay.dev/ftp/configs/`.
