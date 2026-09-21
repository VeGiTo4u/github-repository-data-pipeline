import time
from typing import Callable
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from ingestion.logger import get_logger

class GitHubClientError(Exception):
    """Raised when the GitHub API returns a non-retryable error."""

class GitHubClient:
    """Centralized GitHub HTTP client.
    We encapsulate all requests here to guarantee consistent rate-limit backpressure,
    preventing a single massive repo from exhausting the global API budget."""

    BASE_URL = "https://api.github.com"

    def __init__(self, token: str, run_id: str | None = None, throttle_threshold: int = 100):
        self._session = requests.Session()
        
        retries = Retry(
            total=10,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        self._session.mount("https://", HTTPAdapter(max_retries=retries))
        
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        })
        self._log = get_logger(__name__, run_id=run_id)
        self._throttle_threshold = throttle_threshold

    @property
    def last_status_code(self) -> int:
        if hasattr(self, "_last_response") and self._last_response is not None:
            return self._last_response.status_code
        return 200

    @property
    def last_rate_limit_remaining(self) -> int | None:
        if hasattr(self, "_last_response") and self._last_response is not None:
            val = self._last_response.headers.get("X-RateLimit-Remaining")
            if val is not None:
                try:
                    return int(val)
                except ValueError:
                    return None
        return None

    def get(
        self,
        endpoint: str,
        params: dict | None = None,
        stop_predicate: Callable | None = None,
    ):
        """Yields records lazily.
        By streaming records as a generator instead of returning a massive list, we prevent 
        OOM kills in the Airflow worker when extracting repos with hundreds of thousands of issues.
        """
        # ponytail: generator instead of list accumulation — fixes OOM for large repos
        url = f"{self.BASE_URL}{endpoint}"
        request_params = dict(params or {})
        request_params.setdefault("per_page", 100)
        page_count = 0
        record_count = 0

        while url:
            response = self._request_with_retry(url, request_params)
            data = response.json()

            if isinstance(data, list):
                for item in data:
                    if stop_predicate and stop_predicate(item):
                        return
                    yield item
                    record_count += 1
            else:
                yield data
                record_count += 1

            page_count += 1
            if page_count > 1 and page_count % 5 == 0:
                self._log.info(
                    f"Still fetching {endpoint}... fetched {page_count} pages ({record_count} records) so far"
                )

            # Follow Link header pagination
            url = response.links.get("next", {}).get("url")
            # After the first request, params are encoded in the next URL
            request_params = None

    def post_graphql(self, query: str, variables: dict | None = None) -> requests.Response:
        """Sends a GraphQL query via POST and returns the raw Response.
        
        The caller is responsible for handling GraphQL-specific pagination 
        and unpacking the 'data' payload.
        """
        url = "https://api.github.com/graphql"
        json_payload = {"query": query}
        if variables:
            json_payload["variables"] = variables
            
        return self._request_with_retry(url, params=None, json_payload=json_payload)

    def _request_with_retry(
        self, url: str, params: dict | None, json_payload: dict | None = None
    ) -> requests.Response:
        """Handles rate limits (including 403 rate limit errors) by sleeping until reset.
        Network and 5xx errors are handled automatically by the urllib3 HTTPAdapter."""

        while True:
            self._wait_for_rate_limit()
            
            try:
                if json_payload is not None:
                    response = self._session.post(url, json=json_payload, timeout=30)
                else:
                    response = self._session.get(url, params=params, timeout=30)
            except requests.exceptions.RequestException as exc:
                raise GitHubClientError(f"Network error for {url}: {exc}") from exc

            self._last_response = response

            if response.status_code < 400:
                if not response.text.strip() and response.status_code != 204:
                    self._log.warning(f"Empty {response.status_code} OK from {url}, treating as timeout")
                else:
                    return response

            # Rate limit 403 — sleep until reset and retry
            remaining = response.headers.get("X-RateLimit-Remaining")
            if response.status_code == 403 and (
                remaining == "0" or "rate limit" in response.text.lower()
            ):
                reset_ts = int(response.headers.get("X-RateLimit-Reset", 0))
                wait = max(reset_ts - int(time.time()), 1) + 2
                self._log.warning(
                    f"Rate limit exceeded (HTTP 403) for {url}, sleeping {wait}s until reset"
                )
                time.sleep(wait)
                continue

            # 4xx — fail fast, don't retry (unless it was a rate limit handled above)
            if 400 <= response.status_code < 500:
                raise GitHubClientError(
                    f"GitHub API {response.status_code} for {url}: {response.text}"
                )

    def _wait_for_rate_limit(self):
        """Inspects X-RateLimit-Remaining from the session's last response.

        Two tiers of backpressure:
        1. Below throttle_threshold but > 5: sleep 1s (cooperative slowdown,
           leaves API budget for sibling mapped tasks).
        2. At 5 or below: sleep until reset (hard stop, existing behavior).
        """
        if not hasattr(self, "_last_response") or self._last_response is None:
            return

        resp = self._last_response
        remaining = resp.headers.get("X-RateLimit-Remaining")
        if remaining is None:
            return

        remaining_int = int(remaining)

        if remaining_int <= 5:
            reset_ts = int(resp.headers.get("X-RateLimit-Reset", 0))
            wait = max(reset_ts - int(time.time()), 1) + 2
            self._log.warning(f"Rate limit near zero ({remaining} remaining), sleeping {wait}s until reset")
            time.sleep(wait)
        elif remaining_int < self._throttle_threshold:
            self._log.info(
                f"Rate limit below threshold ({remaining}/{self._throttle_threshold}), "
                f"cooperative 1s slowdown"
            )
            time.sleep(1)
