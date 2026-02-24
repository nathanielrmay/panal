# Plan: NBA Injury Report PDF Scraping

## Goal
Automate the retrieval of official NBA injury data from PDF reports hosted on [official.nba.com](https://official.nba.com/nba-injury-report-2025-26-season/) and store it in the PostgreSQL database for analysis.

## Tech Stack
*   **Language:** Python (preferred over R for PDF/scraping ecosystem).
*   **Libraries:**
    *   `requests`: For downloading the PDF files.
    *   `beautifulsoup4`: For parsing the HTML of the main page to find the latest PDF link.
    *   `pdfplumber`: Validated as a robust tool for extracting tables from PDFs, handling grid layouts better than `PyPDF2`.
    *   `pandas`: For data cleaning and DataFrame manipulation.
    *   `sqlalchemy` / `psycopg2`: For database connection and upsert operations.

## Implementation Plan

### 1. Scrape the Link
*   Fetch the main landing page: `https://official.nba.com/nba-injury-report-2025-26-season/`.
*   Parse the HTML to find the anchor tag `<a>` containing text like "Injury Report" associated with the current date.
*   Extract the `href` attribute to get the direct PDF URL.

### 2. Process the PDF
*   Download the PDF into memory (using `io.BytesIO`) to avoid unnecessary disk I/O.
*   Initialize `pdfplumber` on the memory stream.
*   Iterate through pages and extract the main table.
*   **Target Columns:** `Game Date`, `Team`, `Player`, `Current Status` (Out, Questionable, Doubtful, Available), `Reason`.

### 3. Data Cleaning
*   **Standardization:** Map team names from the PDF (e.g., "L.A. Lakers") to match the official `nba_teams` table in your database.
*   **Formatting:** Convert date strings to PostgreSQL-friendly format (`YYYY-MM-DD`).
*   **Sanitization:** Clean whitespace and handle special characters in player names.

### 4. Database Storage
*   **Target Table:** `nba.injury_reports`
*   **Schema Design:**
    ```sql
    CREATE TABLE nba.injury_reports (
        report_date DATE,
        team_name VARCHAR(50),
        player_name VARCHAR(100),
        status VARCHAR(50),
        reason TEXT,
        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (report_date, player_name)
    );
    ```
*   **Strategy:** Use an "Upsert" (Insert, ON CONFLICT DO UPDATE) strategy. This ensures that if a player's status changes during the day (e.g., from "Questionable" to "Out"), the record is updated rather than duplicated.
