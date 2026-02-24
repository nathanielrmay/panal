# Plan: News API & Analysis Strategy

## Goal
Complement RSS feeds with broad API-based searches and, crucially, apply natural language processing (NLP) to all aggregated content (RSS + API) to make it queryable by Team and Player.

## Part 1: Public APIs (Structured Data)
Use these for broader keyword monitoring or when RSS feeds are insufficient.

### Option A: NewsAPI (Development / Free Tier)
*   **Pros:** Easy JSON API. Filters by simple keywords (e.g., `q="NBA" OR "Basketball"`).
*   **Cons:** Free tier is limited to recent articles and non-commercial use.
*   **Use Case:** Good for catching headlines from general news sites that don't have specific NBA RSS feeds.

### Option B: Bing News Search API (Azure)
*   **Pros:** High relevance, comprehensive coverage, commercial-grade SLAs.
*   **Cons:** Paid service (with a free tier).
*   **Use Case:** High-quality production deployments.

### Option C: The Odds API
*   **Note:** Primarily for betting lines, but metadata often contains game-state info (postponements, etc.) that acts as "news".

---

## Part 2: Analysis & Enrichment Strategy
Raw links are not enough for analysis. We must "enrich" the data to answer questions like *"Show me all news about the Boston Celtics."*

### 1. Entity Extraction (NER)
Use NLP to identify specific entities in the text (Titles + Summaries).
*   **Tool:** `spaCy` (Python) with a pre-trained model (e.g., `en_core_web_sm`).
*   **Process:**
    1.  Load the text of the headline/summary.
    2.  Run NER to find `ORG` (Organizations/Teams) and `PERSON` (Players).
    3.  **Entity Resolution:** Match extracted names against your database:
        *   "Celtics" -> matches `nba_teams.name` -> ID: 2
        *   "LeBron" -> matches `nba_players.name` -> ID: 23

### 2. Sentiment Analysis
Determine if the news is positive or negative.
*   **Tool:** `TextBlob` or `VADER` (via `nltk`).
*   **Metric:** Returns a polarity score (-1.0 to +1.0).
*   **Use Case:** Spotting injury crises (negative sentiment clusters) or winning streaks (positive).

---

## Part 3: Storage Schema
This schema unifies data from both RSS and APIs.

```sql
CREATE TABLE nba.news_feed (
    id SERIAL PRIMARY KEY,
    source_type VARCHAR(20), -- 'RSS', 'API'
    source_name VARCHAR(50), -- 'ESPN', 'Reddit', 'NewsAPI'
    title TEXT NOT NULL,
    url TEXT UNIQUE NOT NULL,
    published_at TIMESTAMP,
    summary TEXT,
    
    -- Analysis Fields
    sentiment_score DECIMAL(3,2), -- -1.00 to 1.00
    
    created_at TIMESTAMP DEFAULT NOW()
);

-- Join table for Many-to-Many relationships (One article can mention multiple teams)
CREATE TABLE nba.news_tags (
    news_id INT REFERENCES nba.news_feed(id),
    team_id INT REFERENCES nba.nba_teams(team_id), -- Assuming team_id exists
    player_id INT, -- If you have a players table
    PRIMARY KEY (news_id, team_id, player_id)
);
```

## Part 4: Unified Workflow
1.  **Ingest:** Scripts fetch data from RSS feeds and APIs.
2.  **Normalize:** Convert all incoming data to a standard object format.
3.  **Analyze:** Pass the object through the `spaCy` pipeline to extract Team/Player IDs and calculate Sentiment.
4.  **Persist:** Save the article to `news_feed` and the associations to `news_tags`.
