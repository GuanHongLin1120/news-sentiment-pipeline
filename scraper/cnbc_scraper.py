import feedparser
from datetime import datetime

# CNBC RSS feeds for different categories
CNBC_FEEDS = {
    "top_news": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "technology": "https://www.cnbc.com/id/19854910/device/rss/rss.html",
    "finance": "https://www.cnbc.com/id/10000664/device/rss/rss.html",
}

def fetch_cnbc_news(category="top_news"):
    feed_url = CNBC_FEEDS[category]
    feed = feedparser.parse(feed_url)

    articles = []
    for entry in feed.entries:
        articles.append({
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
            "summary": entry.get("summary", ""),
            "category": category,
            "source": "cnbc",
            "fetched_at": datetime.now().isoformat()
        })

    return articles

if __name__ == "__main__":
    news = fetch_cnbc_news("technology")
    print(f"Fetched {len(news)} articles\n")

    # Print the first 3 articles to verify
    for article in news[:3]:
        print("Title:", article["title"])
        print("Link:", article["link"])
        print("Published:", article["published"])
        print("-" * 50)