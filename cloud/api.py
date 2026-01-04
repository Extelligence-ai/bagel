"""API client for Matcha Cloud."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from .config import get_api_key, get_api_url


class MatchaAPIError(Exception):
    """Exception raised for API errors."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        """Initialize the exception."""
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class MatchaClient:
    """Client for Matcha Cloud API."""

    def __init__(
        self, api_key: Optional[str] = None, api_url: Optional[str] = None
    ) -> None:
        """Initialize the client."""
        self.api_key = api_key or get_api_key()
        self.api_url = (api_url or get_api_url()).rstrip("/")

        if not self.api_key:
            raise MatchaAPIError(
                "No API key configured. Run 'bagel cloud login' or set MATCHA_API_KEY."
            )

    def _headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Any:
        """Make an API request."""
        url = f"{self.api_url}{endpoint}"

        try:
            response = requests.request(
                method, url, headers=self._headers(), timeout=120, **kwargs
            )

            if response.status_code == 401:
                raise MatchaAPIError(
                    "Invalid API key. Run 'bagel cloud login' to reconfigure.", 401
                )
            elif response.status_code == 402:
                raise MatchaAPIError(
                    "Plan limit reached. Upgrade at https://matcha.extelligence.ai/settings",
                    402,
                )
            elif response.status_code == 403:
                raise MatchaAPIError("Access denied. Check your permissions.", 403)
            elif response.status_code >= 400:
                try:
                    error = response.json().get("detail", response.text)
                except Exception:
                    error = response.text
                raise MatchaAPIError(f"API error: {error}", response.status_code)

            return response.json()

        except requests.exceptions.ConnectionError:
            raise MatchaAPIError(
                f"Could not connect to {self.api_url}. Check your internet connection."
            )
        except requests.exceptions.Timeout:
            raise MatchaAPIError("Request timed out. Try again.")

    # =========================================================================
    # Files
    # =========================================================================

    def list_files(
        self, prefix: str = "", file_type: str = "all"
    ) -> List[Dict[str, Any]]:
        """List files in the knowledge base."""
        response = self._request("POST", "/kb/list", json={"prefix": prefix})
        files = response.get("files", [])

        if file_type != "all":
            files = [f for f in files if f.get("file_type") == file_type]

        return files

    def get_file_details(self, file_key: str) -> Dict[str, Any]:
        """Get details for a specific file."""
        return self._request("GET", f"/kb/file/{file_key}/details")

    def upload_file(
        self, file_path: Path, tags: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Upload a file to the knowledge base."""
        if not file_path.exists():
            raise MatchaAPIError(f"File not found: {file_path}")

        # Get presigned URL
        presigned = self._request(
            "POST",
            "/kb/upload/presigned",
            json={
                "filename": file_path.name,
                "content_type": self._guess_content_type(file_path),
            },
        )

        # Upload to S3
        with open(file_path, "rb") as f:
            upload_response = requests.put(
                presigned["upload_url"],
                data=f,
                headers={
                    "Content-Type": presigned.get("content_type", "application/octet-stream")
                },
                timeout=300,
            )

            if upload_response.status_code != 200:
                raise MatchaAPIError(f"Upload failed: {upload_response.text}")

        result = {
            "key": presigned["key"],
            "filename": file_path.name,
            "status": "uploaded",
        }

        # Add tags if provided
        if tags:
            self._request(
                "PATCH", f"/kb/file/{presigned['key']}/metadata", json={"tags": tags}
            )
            result["tags"] = tags

        return result

    def _guess_content_type(self, file_path: Path) -> str:
        """Guess content type from file extension."""
        ext = file_path.suffix.lower()
        types = {
            ".mcap": "application/octet-stream",
            ".bag": "application/octet-stream",
            ".db3": "application/octet-stream",
            ".ulg": "application/octet-stream",
            ".pdf": "application/pdf",
            ".txt": "text/plain",
            ".md": "text/markdown",
            ".json": "application/json",
            ".yaml": "text/yaml",
            ".yml": "text/yaml",
        }
        return types.get(ext, "application/octet-stream")

    # =========================================================================
    # Query
    # =========================================================================

    def query(self, prompt: str, file_key: Optional[str] = None) -> Dict[str, Any]:
        """Run a query against your data."""
        payload: Dict[str, Any] = {"prompt": prompt}

        if file_key:
            payload["file_context"] = {
                "source": "cli",
                "file_keys": [file_key],
            }

        return self._request("POST", "/run_tool", json=payload)

    # =========================================================================
    # Describe / Topics
    # =========================================================================

    def describe_file(self, file_key: str) -> Dict[str, Any]:
        """Get metadata description of a file."""
        return self._request(
            "GET", f"/kb/file/{file_key}/details?skip_introspection=false"
        )

    def get_channels(self, file_key: str) -> List[Dict[str, Any]]:
        """Get channels/topics from a bag file."""
        response = self._request("GET", f"/kb/file/{file_key}/channels")
        return response.get("channels", [])

    # =========================================================================
    # Tags
    # =========================================================================

    def add_tags(self, file_key: str, tags: List[str]) -> Dict[str, Any]:
        """Add tags to a file."""
        return self._request(
            "PATCH", f"/kb/file/{file_key}/metadata", json={"tags": tags}
        )

    def set_metadata(
        self, file_key: str, metadata: Dict[str, str]
    ) -> Dict[str, Any]:
        """Set custom metadata on a file."""
        return self._request(
            "PATCH", f"/kb/file/{file_key}/metadata", json={"custom_metadata": metadata}
        )

    # =========================================================================
    # Pipelines
    # =========================================================================

    def run_pipeline(
        self,
        pipeline_yaml: str,
        file_keys: Optional[List[str]] = None,
        mode: str = "single",
    ) -> Dict[str, Any]:
        """Run a pipeline on files."""
        return self._request(
            "POST",
            "/kb/pipeline/run",
            json={
                "pipeline_yaml": pipeline_yaml,
                "file_keys": file_keys or [],
                "mode": mode,
            },
        )

    # =========================================================================
    # Stats
    # =========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get knowledge base statistics."""
        return self._request("GET", "/kb/stats")

