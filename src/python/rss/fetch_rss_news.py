import feedparser
import psycopg2
from psycopg2 import sql
from dateutil import parser as date_parser
from datetime import datetime
import time

# ==========================================
# CONFIGURATION
# ==========================================
DB_NAME = "panal"
DB_USER = "than"
DB_PASS = "fishy"
DB_HOST = "localhost"
DB_PORT = "5432"

RSS_FEEDS = [
    {"source": "ESPN", "url": "https://www.espn.com/espn/rss/nba/news"},
    {"source": "Bleacher Report", "url": "https://bleacherreport.com/articles/feed?tag_id=16"},
    {"source": "RealGM", "url": "https://basketball.realgm.com/rss/wiretap/0/0.xml"},
    # {"source": "Reddit", "url": "https://www.reddit.com/r/nba/new/.rss"}, # Commented out due to volume
]

# ==========================================
# DATABASE SETUP
# ==========================================
def get_db_connection():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

def create_table_if_not_exists(conn):
    create_table_query = """
    CREATE SCHEMA IF NOT EXISTS nba;
    
    CREATE TABLE IF NOT EXISTS nba.aggregate_rss_news (
        id SERIAL PRIMARY KEY,
        source VARCHAR(50),
        title TEXT,
        url TEXT UNIQUE,
        published_at TIMESTAMP,
        summary TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_nba_news_published_at ON nba.aggregate_rss_news (published_at DESC);
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
    conn.commit()
    print("Ensured table nba.aggregate_rss_news exists.")

# ==========================================
# MAIN LOGIC
# ==========================================
def fetch_and_store_news():
    conn = get_db_connection()
    create_table_if_not_exists(conn)
    
    new_articles_count = 0
    
    for feed_info in RSS_FEEDS:
        source_name = feed_info["source"]
        url = feed_info["url"]
        
        print(f"Fetching {source_name}...")
        try:
            feed = feedparser.parse(url)
            
            for entry in feed.entries:
                title = entry.get("title", "")
                link = entry.get("link", "")
                summary = entry.get("summary", entry.get("description", ""))
                
                # Handle dates
                if "published" in entry:
                    published_str = entry.published
                elif "updated" in entry:
                    published_str = entry.updated
                else:
                    published_str = str(datetime.now())
                
                try:
                    published_at = date_parser.parse(published_str)
                except:
                    published_at = datetime.now()

                # Insert into DB (Ignore duplicates based on URL)
                insert_query = """
                INSERT INTO nba.aggregate_rss_news (source, title, url, published_at, summary)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                RETURNING id;
                """
                
                with conn.cursor() as cur:
                    cur.execute(insert_query, (source_name, title, link, published_at, summary))
                    if cur.fetchone():
                        new_articles_count += 1
                        print(f"  [NEW] {title}")
                        
        except Exception as e:
            print(f"  Error fetching {source_name}: {e}")

    conn.commit()
    conn.close()
    print(f"\nDone. Added {new_articles_count} new articles.")

if __name__ == "__main__":
    fetch_and_store_news()
