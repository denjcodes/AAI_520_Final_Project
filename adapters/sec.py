"""
SEC filings adapter for retrieving company filings from the SEC API.
"""

import logging
import os
from typing import Any, Dict, List, Tuple
import requests

from .base import APIDataAdapter


class SECAdapter(APIDataAdapter):
    """Fetches SEC filings (10-K, 10-Q, etc.) for fundamental analysis."""

    def __init__(self, health_check: bool = False, fail_on_error: bool = False):
        """Init adapter and optionally run health check."""
        self.base_url = 'https://api.sec-api.io'
        super().__init__('SEC_API_KEY', health_check, fail_on_error)

    def _test_api_connection(self) -> bool:
        """Run SEC API connection and response validation."""
        print("[2/3] Testing API connection...", end=" ")
        try:
            response = requests.get(
                f'{self.base_url}/',
                params={
                    'query': 'ticker:AAPL AND formType:"10-K"',
                    'from': '0',
                    'size': '1',
                    'sort': [{'filedAt': {'order': 'desc'}}]
                },
                headers={'Authorization': self.api_key},
                timeout=10
            )
            response.raise_for_status()
            print("Success")
        except Exception as e:
            print(f"Failed ({type(e).__name__})")
            self._health_check_passed = False
            self._health_check_message = f"API connection failed: {type(e).__name__}"
            print(f"\n{'='*60}\nHealth Check Failed\n{'='*60}\n")
            return False

        print("[3/3] Verifying response data...", end=" ")
        try:
            if isinstance(response.json().get('filings', []), list):
                print("Success")
                self._health_check_passed = True
                self._health_check_message = "SEC API operational"
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

    def _get_stub_data(self, ticker: str, limit: int = 3, *args, **kwargs) -> List[Dict[str, Any]]:
        """Return synthetic SEC filing data as fallback."""
        return [{
            'form': '10-Q',
            'filingDate': '2025-08-01',
            'text': f'{ticker} reported EPS beat; management raised guidance; risk factors discussed.',
            'url': '(stub)',
            'is_stub': True
        } for _ in range(limit)]

    def latest_filings(
        self,
        ticker: str,
        form_types: Tuple[str, ...] = ('10-K', '10-Q'),
        limit: int = 3
    ) -> List[Dict[str, Any]]:
        """Fetch recent SEC filings or stub data if unavailable."""
        if not self.api_key:
            logging.warning(f'Using stub SEC data for {ticker}')
            return self._get_stub_data(ticker, limit)

        try:
            form_query = ' OR '.join([f'formType:"{ft}"' for ft in form_types])
            query = f'ticker:{ticker} AND ({form_query})'

            response = requests.get(
                f'{self.base_url}/',
                params={
                    'query': query,
                    'from': '0',
                    'size': str(limit),
                    'sort': [{'filedAt': {'order': 'desc'}}]
                },
                headers={'Authorization': self.api_key},
                timeout=20
            )
            response.raise_for_status()

            filings = [{
                'form': f.get('formType', ''),
                'filingDate': f.get('filedAt', '')[:10],
                'text': f"{f.get('companyName', ticker)} filed {f.get('formType', '')}.",
                'url': f.get('linkToFilingDetails', ''),
                'is_stub': False
            } for f in response.json().get('filings', [])]

            logging.info(f'Fetched {len(filings)} SEC filings for {ticker}')
            return filings

        except Exception as e:
            logging.error(f'SEC API error: {e}')
            return self._get_stub_data(ticker, limit)
