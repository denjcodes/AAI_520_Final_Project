"""
News API adapter with sentiment classification using a 5-step prompt chain.
"""

import logging
import os
import re
import time
from typing import Any, Dict, List
import pandas as pd
import requests

from .base import APIDataAdapter


class NewsAdapter(APIDataAdapter):
    """Fetches news from NewsAPI and performs keyword-based sentiment analysis."""

    POS = ('beat', 'raise', 'growth', 'record', 'surge', 'expand')
    NEG = ('miss', 'cut', 'decline', 'lawsuit', 'drop', 'recall', 'probe')

    def __init__(self, health_check: bool = False, fail_on_error: bool = False):
        """Init adapter and optionally run health check."""
        self.key = os.getenv('NEWSAPI_KEY')
        self.session = requests.Session() if self.key else None
        super().__init__('NEWSAPI_KEY', health_check, fail_on_error)
        self.key = self.api_key

    def _test_api_connection(self) -> bool:
        """Check API key, connection, and response validity."""
        print(f"\n{'='*60}\n{self.adapter_name} Health Check\n{'='*60}")

        print("[2/3] Testing API connection...", end=" ")
        try:
            r = self.session.get(
                'https://newsapi.org/v2/everything',
                headers={'X-Api-Key': self.api_key},
                params={'q': 'test', 'language': 'en', 'pageSize': 1},
                timeout=10
            )
            r.raise_for_status()
            print("Success")
        except Exception as e:
            print(f"Failed ({type(e).__name__})")
            self._health_check_passed = False
            self._health_check_message = f"API connection failed: {type(e).__name__}"
            print(f"\n{'='*60}\nHealth Check Failed\n{'='*60}\n")
            return False

        print("[3/3] Verifying response data...", end=" ")
        try:
            if isinstance(r.json().get('articles', []), list):
                print("Success")
                self._health_check_passed = True
                self._health_check_message = "News API operational"
            else:
                print("Warning")
                self._health_check_passed = False
                self._health_check_message = "Unexpected response format"
        except Exception as e:
            print(f"Failed ({type(e).__name__})")
            self._health_check_passed = False
            self._health_check_message = f"Response parsing failed: {type(e).__name__}"

        status = "Passed" if self._health_check_passed else "Failed"
        print(f"\n{'='*60}\nHealth Check {status}\n{'='*60}\n")
        return self._health_check_passed

    def _get_stub_data(self, ticker: str, *args, **kwargs) -> List[Dict[str, Any]]:
        """Return stub articles when API is unavailable."""
        now = int(time.time())
        return [
            {
                'title': f'[Stub] Growth story for {ticker}',
                'description': f'{ticker} gains as services growth improves',
                'content': '...',
                'publishedAt': pd.Timestamp.utcfromtimestamp(now).isoformat()
            },
            {
                'title': f'[Stub] Miss story for {ticker}',
                'description': f'{ticker} faces supply chain miss and margin pressure',
                'content': '...',
                'publishedAt': pd.Timestamp.utcfromtimestamp(now - 3600).isoformat()
            }
        ]

    def _ingest(self, ticker: str, window_days: int = 7) -> List[Dict[str, Any]]:
        """Fetch news articles or fallback to stub data."""
        if not self.session:
            logging.warning(f'Using stub news data for {ticker}')
            return self._get_stub_data(ticker)

        try:
            r = self.session.get(
                'https://newsapi.org/v2/everything',
                headers={'X-Api-Key': self.key},
                params={'q': ticker, 'language': 'en', 'pageSize': 50, 'sortBy': 'publishedAt'},
                timeout=20
            )
            r.raise_for_status()
            articles = r.json().get('articles', [])
            logging.info(f'Fetched {len(articles)} articles for {ticker}')
            return articles
        except Exception as e:
            logging.error(f'News API error: {e}')
            return self._get_stub_data(ticker)

    def _preprocess(self, items: List[Dict]) -> List[str]:
        """Clean and normalize article text."""
        return [
            re.sub(r'\s+', ' ', ' '.join([
                str(a.get('title', '')),
                str(a.get('description', '')),
                str(a.get('content', ''))
            ])).strip().lower()
            for a in items
        ]

    def _classify(self, texts: List[str]) -> List[str]:
        """Classify sentiment by keyword presence."""
        def classify_text(t):
            has_pos = any(k in t for k in self.POS)
            has_neg = any(k in t for k in self.NEG)
            if has_pos and has_neg:
                return 'mixed'
            elif has_pos:
                return 'positive'
            elif has_neg:
                return 'negative'
            else:
                return 'neutral'
        return [classify_text(t) for t in texts]

    def _extract(self, texts: List[str]) -> List[Dict]:
        """Extract monetary entities from text."""
        return [{'money': list(set(re.findall(r'\$\s?\d+(?:\.\d+)?', t)))} for t in texts]

    def _summarize(self, labels: List[str], texts: List[str], k: int = 2) -> Dict:
        """Summarize sentiment counts and sample snippets."""
        return {
            'summary_text': '...',
            'counts': {s: labels.count(s) for s in ['mixed', 'positive', 'negative', 'neutral']},
            'snippets': [f'- {labels[i]}: {texts[i][:100]}...' for i in range(min(k, len(texts)))]
        }

    def run_chain(self, ticker: str, window_days: int = 7) -> Dict:
        """Run full pipeline: Ingest → Preprocess → Classify → Extract → Summarize."""
        items = self._ingest(ticker, window_days)
        texts = self._preprocess(items)
        labels = self._classify(texts)
        extracts = self._extract(texts)
        summary = self._summarize(labels, texts)

        return {
            'type': 'news',
            'counts': summary['counts'],
            'samples': [
                {'text': texts[i], 'sentiment': labels[i], 'entities': extracts[i]}
                for i in range(min(3, len(texts)))
            ],
            'summary': summary,
            'is_stub': self.session is None or len(items) == 2
        }
