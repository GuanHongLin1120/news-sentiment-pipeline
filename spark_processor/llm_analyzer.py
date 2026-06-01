import os
import json
import time
from google import genai
from dotenv import load_dotenv

load_dotenv()

# Initialize the Gemini client with your API key from .env
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# The model to use — fast and cheap, good for high-volume scoring
MODEL = "gemini-2.5-flash"

def analyze_article(title, summary="", max_retries=3):
    """Send one article to Gemini with retry logic for transient failures."""

    prompt = f"""You are a financial news analyst. Analyze the following news article
and respond with ONLY a JSON object (no markdown, no extra text) in this exact format:

{{
  "sentiment": "bullish" | "bearish" | "neutral",
  "sentiment_score": <float between -1.0 and 1.0>,
  "tickers": [<list of stock ticker symbols mentioned, e.g. "TSM", "NVDA">],
  "summary": "<one-sentence summary>"
}}

Article title: {title}
Article summary: {summary}
"""

    for attempt in range(1, max_retries + 1):
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=prompt,
            )
            raw = response.text.strip()
            raw = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(raw)

        except Exception as e:
            # If it's the last attempt, give up and return a safe default
            if attempt == max_retries:
                print(f"    (failed after {max_retries} attempts: {e})")
                return {
                    "sentiment": "neutral",
                    "sentiment_score": 0.0,
                    "tickers": [],
                    "summary": "",
                    "error": str(e)
                }
            # Otherwise wait and retry (exponential backoff: 2s, 4s, 8s)
            wait = 2 ** attempt
            print(f"    (attempt {attempt} failed, retrying in {wait}s...)")
            time.sleep(wait)

if __name__ == "__main__":
    # Test with one real-looking headline
    test_title = "TSMC raises full-year revenue guidance on strong AI chip demand"
    test_summary = "The chipmaker cited robust orders from AI customers as the main driver."

    print("Analyzing test article...\n")
    result = analyze_article(test_title, test_summary)

    print("Structured result from Gemini:")
    print(json.dumps(result, indent=2))