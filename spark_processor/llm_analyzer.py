import os
import json
import time
from dotenv import load_dotenv

load_dotenv()

# Select the LLM provider via env var; defaults to groq.
# This is the single switch that makes the scoring layer provider-agnostic.
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "groq").lower()

# ---- Provider-specific clients & models (lazy: only init what we use) ----
if LLM_PROVIDER == "groq":
    from groq import Groq
    _client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    _MODEL = "llama-3.3-70b-versatile"
elif LLM_PROVIDER == "gemini":
    from google import genai
    _client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    _MODEL = "gemini-2.5-flash"
else:
    raise ValueError(f"Unsupported LLM_PROVIDER: {LLM_PROVIDER}")


def _call_groq(prompt):
    """Provider-specific call: Groq (OpenAI-compatible chat interface)."""
    response = _client.chat.completions.create(
        model=_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
    )
    return response.choices[0].message.content.strip()


def _call_gemini(prompt):
    """Provider-specific call: Gemini (single-string generate_content)."""
    response = _client.models.generate_content(model=_MODEL, contents=prompt)
    raw = response.text.strip()
    # Gemini may wrap output in markdown fences; strip them
    return raw.replace("```json", "").replace("```", "").strip()


def _call_llm(prompt):
    """Dispatch to the active provider. The rest of the code is provider-agnostic."""
    if LLM_PROVIDER == "groq":
        return _call_groq(prompt)
    elif LLM_PROVIDER == "gemini":
        return _call_gemini(prompt)


def analyze_article(title, summary="", max_retries=3):
    """Score one article. Provider-agnostic: prompt, retry, and fallback
    logic stay identical regardless of which LLM backend is selected."""

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
            raw = _call_llm(prompt)
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
    test_title = "TSMC raises full-year revenue guidance on strong AI chip demand"
    test_summary = "The chipmaker cited robust orders from AI customers as the main driver."

    print(f"Analyzing test article (provider={LLM_PROVIDER})...\n")
    result = analyze_article(test_title, test_summary)

    print("Structured result:")
    print(json.dumps(result, indent=2))