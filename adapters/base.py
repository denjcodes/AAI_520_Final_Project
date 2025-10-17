"""
Base adapter classes for data retrieval with health checks and API fallback.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict
import os
import pandas as pd


class baseDataAdapter(ABC):
    """Abstract base for data adapters with health checks and stub fallback."""

    def __init__(self, health_check: bool = False, fail_on_error: bool = False):
        """Init adapter; optionally run health check and fail if requested."""
        self.adapter_name = self.__class__.__name__
        self._stub = False
        self._init_time = pd.Timestamp.now()
        self._health_check_passed = False
        self._health_check_message = "Not Tested"

        if health_check:
            if not self._run_health_check() and fail_on_error:
                raise RuntimeError(f"Health Check failed: {self._health_check_message}")

    @abstractmethod
    def _run_health_check(self) -> bool:
        """Run adapter-specific health check; must set _health_check_* attributes."""
        pass

    @abstractmethod
    def _get_stub_data(self, *args, **kwargs):
        """Return fallback data if real source is unavailable."""
        pass

    def get_health_status(self) -> Dict[str, Any]:
        """Return adapter’s health status as a dictionary."""
        return {
            'healthy': self._health_check_passed,
            'message': self._health_check_message,
            'init_time': str(self._init_time),
            'stub_mode': self._stub
        }

    def is_healthy(self) -> bool:
        """Return True if last health check passed."""
        return self._health_check_passed


class APIDataAdapter(baseDataAdapter):
    """Base class for API adapters that use environment-based API keys."""

    def __init__(self, api_key_env_var: str, health_check: bool = False, fail_on_error: bool = False):
        """Load API key from environment and optionally run health check."""
        self.api_key = os.getenv(api_key_env_var)
        self.api_key_env_var = api_key_env_var
        super().__init__(health_check=health_check, fail_on_error=fail_on_error)

    def _run_health_check(self) -> bool:
        """Run generic 3-step health check: key, connection, response."""
        print(f"\n{'='*60}\n{self.adapter_name} Health Check\n{'='*60}")

        print("\n[1/3] Checking API key...", end=" ")
        if not self.api_key:
            print("Failed (no API key found)")
            self._health_check_passed = False
            self._health_check_message = f"No {self.api_key_env_var} found in environment"
            print(f"\n{'='*60}\nHealth Check Failed - No API key configured\n{'='*60}\n")
            return False
        print("Success")
        return self._test_api_connection()

    @abstractmethod
    def _test_api_connection(self) -> bool:
        """API-specific connection and response validation."""
        pass
