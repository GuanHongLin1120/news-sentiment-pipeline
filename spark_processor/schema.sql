-- Table to store AI-scored news articles
CREATE TABLE IF NOT EXISTS scored_news (
    id              SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    link            TEXT,
    summary         TEXT,
    published       TEXT,
    source          VARCHAR(50),
    category        VARCHAR(50),
    sentiment       VARCHAR(20),
    sentiment_score REAL,
    tickers         TEXT,
    ai_summary      TEXT,
    fetched_at      TEXT,
    inserted_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index on tickers for faster queries like "all TSM news"
CREATE INDEX IF NOT EXISTS idx_tickers ON scored_news (tickers);
-- Index on sentiment_score for analytical queries
CREATE INDEX IF NOT EXISTS idx_sentiment ON scored_news (sentiment_score);