"""
Yahoo Finance adapter for fetching stock prices and company info via yfinance.
"""

import logging
from typing import Any, Dict
import numpy as np
import pandas as pd
import yfinance as yf

from .base import baseDataAdapter


class YahooFinanceAdapter(baseDataAdapter):
    """Retrieves stock prices and company info using the yfinance library."""

    def __init__(self, health_check: bool = False, fail_on_error: bool = False, test_ticker: str = 'AAPL'):
        """Init adapter and optionally run health check."""
        self.test_ticker = test_ticker
        super().__init__(health_check=health_check, fail_on_error=fail_on_error)

    def _run_health_check(self) -> bool:
        """Verify Yahoo Finance connectivity and data integrity."""
        print(f"\n{'='*60}\n{self.adapter_name} Health Check\n{'='*60}")
        print(f"Testing with ticker: {self.test_ticker}")
        print(f"Test initiated at: {self._init_time}")

        test_ticker = self._sanitize_ticker(self.test_ticker)

        try:
            print("\n[1/4] Creating ticker object...", end=" ")
            t = yf.Ticker(test_ticker)
            print("Success")

            print("[2/4] Fetching ticker info...", end=" ")
            try:
                info = t.info
                if info and len(info) > 5:
                    print(f"Success ({info.get('longName', info.get('shortName', 'N/A'))})")
                else:
                    print("Warning (minimal info)")
            except Exception as e:
                print(f"Warning ({type(e).__name__})")

            print("[3/4] Fetching price history (5 days)...", end=" ")
            hist = t.history(period='5d', auto_adjust=False, actions=False)
            if hist is not None and not hist.empty:
                latest_date = hist.index[-1].date()
                latest_price = hist['Close'].iloc[-1]
                print(f"Success ({len(hist)} days, latest ${latest_price:.2f} on {latest_date})")

                print("[4/4] Verifying data quality...", end=" ")
                if 'Close' in hist.columns:
                    print("Success")
                    self._health_check_passed = True
                    self._health_check_message = "Yahoo Finance operational"
                else:
                    print("Warning (unexpected structure)")
                    self._health_check_passed = False
            else:
                print("Failed (empty data)")
                self._health_check_passed = False
                self._health_check_message = "Empty data returned"

        except Exception as e:
            print(f"Failed ({type(e).__name__}: {str(e)[:60]})")
            self._health_check_passed = False
            self._health_check_message = f"Health check failed: {type(e).__name__}: {str(e)[:100]}"

        status = "Passed" if self._health_check_passed else "Failed"
        print(f"\n{'='*60}\nHealth Check {status}")
        if not self._health_check_passed:
            print(f"Reason: {self._health_check_message}")
        print(f"{'='*60}\n")

        return self._health_check_passed

    def _sanitize_ticker(self, ticker: str) -> str:
        """Clean ticker symbols and ensure uppercase."""
        ticker = str(ticker).strip()
        return ticker[1:].upper() if ticker.startswith('$') else ticker.upper()

    def fetch_prices(self, ticker: str, period: str = '6mo', interval: str = '1d') -> pd.DataFrame:
        """Fetch historical price data or fallback to stub."""
        ticker = self._sanitize_ticker(ticker)
        logging.info(f'Fetching price data for {ticker} (period={period}, interval={interval})')

        if self._stub:
            logging.warning('Using stub price data')
            return self._get_stub_data(ticker)

        try:
            t = yf.Ticker(ticker)
            df = t.history(period=period, interval=interval, auto_adjust=False, actions=False, raise_errors=False)
        except Exception as e:
            logging.error(f'Fetch error: {type(e).__name__}: {e}')
            return self._get_stub_data(ticker, error=str(e))

        if df is None or df.empty:
            logging.warning(f'Empty data for {ticker}, retrying shorter period')
            try:
                df = t.history(period='1mo', interval='1d', auto_adjust=False, actions=False)
                if df.empty:
                    return self._get_stub_data(ticker, error='Empty after retry')
            except:
                return self._get_stub_data(ticker, error='Retry failed')

        df = self._normalize_dataframe(df, ticker)
        df['source'] = 'yfinance'
        df.attrs['is_stub'] = False
        logging.info(f'Fetched {len(df)} price points for {ticker}')
        return df

    def _normalize_dataframe(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Normalize column names and ensure consistent structure."""
        df = df.reset_index()

        rename_map = {}
        if 'Date' not in df.columns:
            rename_map.update({c: 'Date' for c in ['Datetime', 'date', 'datetime', 'index'] if c in df.columns})
        if 'Close' not in df.columns:
            rename_map.update({c: 'Close' for c in ['Adj Close', 'adj_close', 'close'] if c in df.columns})
        if rename_map:
            df = df.rename(columns=rename_map)

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join([str(p) for p in t if p]).strip() for t in df.columns]

        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df = df.dropna(subset=['Date']).sort_values('Date')
        if 'Close' in df.columns:
            df = df.dropna(subset=['Close'])

        return df

    def _get_stub_data(self, ticker: str, error: str = None, *args, **kwargs) -> pd.DataFrame:
        """Generate synthetic random-walk price data for fallback."""
        idx = pd.date_range(end=pd.Timestamp.today(), periods=30, freq='D')
        np.random.seed(hash(ticker) % (2**32))

        base_price = 100 + (hash(ticker) % 200)
        prices = base_price * np.exp(np.cumsum(np.random.normal(0.001, 0.02, len(idx))))

        df = pd.DataFrame({
            'Date': idx,
            'Close': prices,
            'Open': prices * 0.99,
            'High': prices * 1.01,
            'Low': prices * 0.98,
            'Volume': np.random.randint(1e6, 1e8, len(idx))
        })

        df['source'] = 'stub_fallback'
        df.attrs['is_stub'] = True
        if error:
            df.attrs['stub_reason'] = error
            logging.warning(f'[Stub] {ticker}: {error}')
        else:
            logging.warning(f'[Stub] Generated data for {ticker}')
        return df

    def fetch_info(self, ticker: str) -> Dict[str, Any]:
        """Fetch company info and fundamentals or stub on failure."""
        ticker = self._sanitize_ticker(ticker)
        if self._stub:
            return {'stub': True, 'ticker': ticker}

        try:
            t = yf.Ticker(ticker)
            # Always use full info (not fast_info) to get complete data including P/E, 52-week ranges, etc.
            info = t.info or {}
            info['source'] = 'info'
            logging.info(f'Fetched info for {ticker} with {len(info)} fields')
            return info
        except Exception as e:
            logging.warning(f'Yahoo info error for {ticker}: {e}')
            return {'error': str(e), 'ticker': ticker}

    def fetch_financials(self, ticker: str) -> pd.DataFrame:
        """Fetch income statement (financials) or return empty DataFrame on failure."""
        ticker = self._sanitize_ticker(ticker)
        logging.info(f'Fetching financials for {ticker}')

        if self._stub:
            logging.warning('Using stub financials data')
            return pd.DataFrame()

        try:
            t = yf.Ticker(ticker)
            financials = t.financials
            if financials is not None and not financials.empty:
                logging.info(f'Fetched financials for {ticker}: {financials.shape}')
                return financials
            else:
                logging.warning(f'Empty financials for {ticker}')
                return pd.DataFrame()
        except Exception as e:
            logging.error(f'Error fetching financials for {ticker}: {e}')
            return pd.DataFrame()

    def fetch_balance_sheet(self, ticker: str) -> pd.DataFrame:
        """Fetch balance sheet or return empty DataFrame on failure."""
        ticker = self._sanitize_ticker(ticker)
        logging.info(f'Fetching balance sheet for {ticker}')

        if self._stub:
            logging.warning('Using stub balance sheet data')
            return pd.DataFrame()

        try:
            t = yf.Ticker(ticker)
            balance_sheet = t.balance_sheet
            if balance_sheet is not None and not balance_sheet.empty:
                logging.info(f'Fetched balance sheet for {ticker}: {balance_sheet.shape}')
                return balance_sheet
            else:
                logging.warning(f'Empty balance sheet for {ticker}')
                return pd.DataFrame()
        except Exception as e:
            logging.error(f'Error fetching balance sheet for {ticker}: {e}')
            return pd.DataFrame()

    def fetch_cashflow(self, ticker: str) -> pd.DataFrame:
        """Fetch cash flow statement or return empty DataFrame on failure."""
        ticker = self._sanitize_ticker(ticker)
        logging.info(f'Fetching cash flow for {ticker}')

        if self._stub:
            logging.warning('Using stub cash flow data')
            return pd.DataFrame()

        try:
            t = yf.Ticker(ticker)
            cashflow = t.cashflow
            if cashflow is not None and not cashflow.empty:
                logging.info(f'Fetched cash flow for {ticker}: {cashflow.shape}')
                return cashflow
            else:
                logging.warning(f'Empty cash flow for {ticker}')
                return pd.DataFrame()
        except Exception as e:
            logging.error(f'Error fetching cash flow for {ticker}: {e}')
            return pd.DataFrame()
