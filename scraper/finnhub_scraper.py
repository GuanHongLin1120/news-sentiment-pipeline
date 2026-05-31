import os
import finnhub
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read the API key securely from .env
API_KEY = os.getenv("FINNHUB_API_KEY")

# Initialize the Finnhub client
finnhub_client = finnhub.Client(api_key=API_KEY)

def fetch_company_news(ticker="TSM", days_back=7):
    """Fetch recent news for a given stock ticker."""
    from datetime import timedelta

    today = datetime.now()
    start_date = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")

    # Finnhub company_news requires: symbol, from date, to date
    news = finnhub_client.company_news(ticker, _from=start_date, to=end_date)

    articles = []
    for item in news:
        articles.append({
            "title": item.get("headline", ""),
            "link": item.get("url", ""),
            "summary": item.get("summary", ""),
            "published": datetime.fromtimestamp(item.get("datetime", 0)).isoformat(),
            "ticker": ticker,
            "source": "finnhub",
            "fetched_at": datetime.now().isoformat()
        })

    return articles

if __name__ == "__main__":
    # Quick check: is the API key loaded?
    if not API_KEY:
        print("ERROR: API key not found. Check your .env file.")
    else:
        print(f"API key loaded (ends with ...{API_KEY[-4:]})\n")

        news = fetch_company_news("TSM")
        print(f"Fetched {len(news)} articles for TSM\n")

        # Print the first 3 to verify
        for article in news[:3]:
            print("Title:", article["title"])
            print("Published:", article["published"])
            print("-" * 50)