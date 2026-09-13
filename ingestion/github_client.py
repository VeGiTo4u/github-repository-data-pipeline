import time
from typing import Callable
import requests
from ingestion.logger import get_logger

class GitHubClientError(Exception):
    """Raised when the GitHub API returns a non-retryable error."""

class GitHubClient:
    """Handles GitHub API requests with auth, pagination, rate-limit handling,
    and exponential backoff on 5xx errors (Phase-1.md Section 8.1)."""

    BASE_URL = "https://api.github.com"

    def __init__(self, token: str, run_id: str | None = None, throttle_threshold: int = 100):
        self._session = requests.Session()
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
    ) -> list[dict]:
        """Fetches a GitHub API endpoint, handling pagination.

        Returns the full list of records across all pages.
        For single-object endpoints (e.g. /repos/owner/repo), returns a
        one-element list.
        If stop_predicate is provided, stops pagination when stop_predicate(record) is True.
        """
        url = f"{self.BASE_URL}{endpoint}"
        all_records = []
        request_params = dict(params or {})
        request_params.setdefault("per_page", 100)
        page_count = 0

        while url:
            response = self._request_with_retry(url, request_params)
            data = response.json()

            if isinstance(data, list):
                if stop_predicate:
                    for item in data:
                        if stop_predicate(item):
                            return all_records
                        all_records.append(item)
                else:
                    all_records.extend(data)
            else:
                all_records.append(data)

            page_count += 1
            if page_count > 1 and page_count % 5 == 0:
                self._log.info(
                    f"Still fetching {endpoint}... fetched {page_count} pages ({len(all_records)} records) so far"
                )

            # Follow Link header pagination
            url = response.links.get("next", {}).get("url")
            # After the first request, params are encoded in the next URL
            request_params = None

        return all_records

    def get_raw(self, endpoint: str, params: dict | None = None) -> requests.Response:
        """Fetches a single page and returns the raw Response object.

        Used by extractor/normalizer to capture HTTP metadata (status,
        rate-limit headers) for the lineage envelope.
        """
        url = f"{self.BASE_URL}{endpoint}"
        return self._request_with_retry(url, params)

    def _request_with_retry(
        self, url: str, params: dict | None, max_retries: int = 3
    ) -> requests.Response:
        """Retries on 5xx / network errors with exponential backoff.
        Handles rate limits (including 403 rate limit errors) by sleeping until reset.
        Fails fast on other 4xx (real errors)."""

        for attempt in range(max_retries + 1):
            try:
                self._wait_for_rate_limit()
                response = self._session.get(url, params=params, timeout=30)
                self._last_response = response

                if response.status_code < 400:
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

                # 4xx — fail fast, don't retry
                if 400 <= response.status_code < 500:
                    raise GitHubClientError(
                        f"GitHub API {response.status_code} for {url}: {response.text}"
                    )

                # 5xx — retry
                self._log.warning(
                    f"GitHub API 5xx ({response.status_code}) for {url}, "
                    f"attempt {attempt + 1}/{max_retries + 1}",
                )

            except requests.exceptions.RequestException as exc:
                if attempt == max_retries:
                    raise GitHubClientError(
                        f"Network error after {max_retries + 1} attempts for {url}"
                    ) from exc
                self._log.warning(
                    f"Network error for {url}, attempt {attempt + 1}/{max_retries + 1}: {exc}",
                )

            backoff = 2 ** attempt
            self._log.info(f"Retrying in {backoff}s...")
            time.sleep(backoff)

        raise GitHubClientError(f"Failed after {max_retries + 1} attempts for {url}")

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
