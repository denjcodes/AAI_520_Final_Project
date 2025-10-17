"""
Data adapters for investment research system.

This module provides adapters for various data sources:
- YahooFinanceAdapter: Stock prices and company information
- NewsAdapter: News articles with sentiment analysis
- SECAdapter: SEC regulatory filings
"""

from .base import baseDataAdapter, APIDataAdapter
from .yahoo import YahooFinanceAdapter
from .news import NewsAdapter
from .sec import SECAdapter

__all__ = [
    'baseDataAdapter',
    'APIDataAdapter',
    'YahooFinanceAdapter',
    'NewsAdapter',
    'SECAdapter',
]

__version__ = '1.0.0'
