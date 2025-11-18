#!/usr/bin/env python3
"""
LetterXpress API Client
Handles communication with the LetterXpress API v3 for automated letter printing and sending.
"""

import base64
import hashlib
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class LetterXpressClient:
    """Client for interacting with the LetterXpress API."""

    def __init__(
        self,
        username: Optional[str] = None,
        api_key: Optional[str] = None,
        mode: str = "test"
    ):
        """
        Initialize the LetterXpress client.

        Args:
            username: LetterXpress username (defaults to env LETTERXPRESS_USERNAME)
            api_key: LetterXpress API key (defaults to env LETTERXPRESS_APIKEY)
            mode: API mode - "test" or "live" (defaults to env LETTERXPRESS_MODE or "test")
        """
        self.username = username or os.getenv("LETTERXPRESS_USERNAME")
        self.api_key = api_key or os.getenv("LETTERXPRESS_APIKEY")
        self.mode = mode if mode in ["test", "live"] else os.getenv("LETTERXPRESS_MODE", "test")

        if not self.username or not self.api_key:
            raise ValueError("LetterXpress username and API key are required")

        self.base_url = "https://api.letterxpress.de/v3"
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json"
        })

    def _get_auth_payload(self) -> Dict:
        """Get authentication payload for API requests."""
        return {
            "auth": {
                "username": self.username,
                "apikey": self.api_key,
                "mode": self.mode
            }
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None
    ) -> Dict:
        """
        Make a request to the LetterXpress API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (without base URL)
            data: Request payload

        Returns:
            Response JSON data

        Raises:
            requests.HTTPError: If the request fails
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # Add auth to data
        if data is None:
            data = self._get_auth_payload()
        else:
            data.update(self._get_auth_payload())

        logger.debug(f"{method} {url}")
        logger.debug(f"Request data: {data}")

        response = self.session.request(method, url, json=data)

        # Log response details before raising error
        logger.debug(f"Response status: {response.status_code}")
        logger.debug(f"Response body: {response.text}")

        response.raise_for_status()

        return response.json()

    def check_balance(self) -> Tuple[float, str]:
        """
        Check account balance.

        Returns:
            Tuple of (balance, currency)
        """
        # Use GET request with JSON body (as per API documentation)
        response = self._make_request("GET", "/balance")

        if response.get("status") == 200:
            data = response.get("data", {})
            return data.get("balance", 0.0), data.get("currency", "EUR")

        raise Exception(f"Failed to check balance: {response.get('message')}")

    def get_price(
        self,
        pages: int,
        color: str = "1",
        mode: str = "simplex",
        shipping: str = "national",
        registered: Optional[str] = None
    ) -> float:
        """
        Get price quote for a letter.

        Args:
            pages: Number of pages
            color: "1" for black/white, "4" for color
            mode: "simplex" or "duplex"
            shipping: "national" or "international"
            registered: Optional registered mail type ("r1" or "r2")

        Returns:
            Price in EUR
        """
        payload = {
            "letter": {
                "specification": {
                    "pages": pages,
                    "color": color,
                    "mode": mode,
                    "shipping": shipping
                }
            }
        }

        if registered:
            payload["letter"]["registered"] = registered

        response = self._make_request("GET", "/price", payload)

        if response.get("status") == 200:
            return response.get("data", {}).get("price", 0.0)

        raise Exception(f"Failed to get price: {response.get('message')}")

    def _encode_pdf(self, pdf_path: Path) -> Tuple[str, str]:
        """
        Encode PDF to base64 and calculate MD5 checksum.

        Args:
            pdf_path: Path to PDF file

        Returns:
            Tuple of (base64_string, md5_checksum)
        """
        with open(pdf_path, 'rb') as f:
            pdf_bytes = f.read()

        base64_string = base64.b64encode(pdf_bytes).decode('utf-8')
        md5_checksum = hashlib.md5(base64_string.encode('utf-8')).hexdigest()

        return base64_string, md5_checksum

    def submit_letter(
        self,
        pdf_path: Path,
        color: str = "4",
        mode: str = "duplex",
        shipping: str = "national",
        registered: Optional[str] = None,
        dispatch_date: Optional[str] = None,
        notice: Optional[str] = None,
        filename_original: Optional[str] = None
    ) -> Dict:
        """
        Submit a letter for printing and sending.

        Args:
            pdf_path: Path to PDF file
            color: "1" for black/white, "4" for color
            mode: "simplex" or "duplex"
            shipping: "national" or "international"
            registered: Optional registered mail ("r1" for Einschreiben Einwurf, "r2" for Einschreiben)
            dispatch_date: Optional dispatch date in YYYY-MM-DD format
            notice: Optional notice/reference text (max 255 chars)
            filename_original: Optional original filename

        Returns:
            Response data with job ID and details
        """
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")

        base64_file, checksum = self._encode_pdf(pdf_path)

        payload = {
            "letter": {
                "base64_file": base64_file,
                "base64_file_checksum": checksum,
                "specification": {
                    "color": color,
                    "mode": mode,
                    "shipping": shipping
                }
            }
        }

        # Add optional parameters
        if registered:
            payload["letter"]["registered"] = registered

        if dispatch_date:
            payload["letter"]["dispatch_date"] = dispatch_date

        if notice:
            payload["letter"]["notice"] = notice[:255]

        if filename_original:
            payload["letter"]["filename_original"] = filename_original

        response = self._make_request("POST", "/printjobs", payload)

        if response.get("status") == 200:
            return response.get("data", {})

        raise Exception(f"Failed to submit letter: {response.get('message')}")

    def submit_letters_batch(
        self,
        pdf_paths: List[Path],
        color: str = "4",
        mode: str = "duplex",
        shipping: str = "national",
        registered: Optional[str] = None,
        notice_prefix: str = ""
    ) -> List[Dict]:
        """
        Submit multiple letters in batch.

        Args:
            pdf_paths: List of PDF file paths
            color: "1" for black/white, "4" for color
            mode: "simplex" or "duplex"
            shipping: "national" or "international"
            registered: Optional registered mail type
            notice_prefix: Prefix for notice field

        Returns:
            List of response data for each submitted letter
        """
        results = []

        for i, pdf_path in enumerate(pdf_paths):
            try:
                notice = f"{notice_prefix} {i+1}/{len(pdf_paths)}" if notice_prefix else None
                filename = pdf_path.name

                result = self.submit_letter(
                    pdf_path=pdf_path,
                    color=color,
                    mode=mode,
                    shipping=shipping,
                    registered=registered,
                    notice=notice,
                    filename_original=filename
                )

                results.append({
                    "success": True,
                    "filename": filename,
                    "data": result
                })

                logger.info(f"Successfully submitted {filename} (ID: {result.get('id')})")

            except Exception as e:
                logger.error(f"Failed to submit {pdf_path.name}: {e}")
                results.append({
                    "success": False,
                    "filename": pdf_path.name,
                    "error": str(e)
                })

        return results

    def get_job(self, job_id: int) -> Dict:
        """
        Get details of a print job.

        Args:
            job_id: Job ID

        Returns:
            Job details
        """
        response = self._make_request("GET", f"/printjobs/{job_id}")

        if response.get("status") == 200:
            return response.get("data", {})

        raise Exception(f"Failed to get job: {response.get('message')}")

    def list_jobs(self, filter_type: Optional[str] = None) -> List[Dict]:
        """
        List print jobs.

        Args:
            filter_type: Optional filter - "queue", "hold", "done", "canceled", "draft"

        Returns:
            List of jobs
        """
        # Use GET request with JSON body (like balance API)
        payload = {}
        if filter_type:
            payload["filter"] = filter_type

        response = self._make_request("GET", "/printjobs", payload if filter_type else None)

        if response.get("status") == 200:
            return response.get("data", {}).get("printjobs", [])

        raise Exception(f"Failed to list jobs: {response.get('message')}")


def main():
    """Test the LetterXpress client."""
    logging.basicConfig(level=logging.INFO)

    try:
        client = LetterXpressClient(mode="test")

        # Check balance
        balance, currency = client.check_balance()
        print(f"Account balance: {balance} {currency}")

        # Get price for a 2-page color letter
        price = client.get_price(pages=2, color="4", mode="duplex")
        print(f"Price for 2-page color letter: {price} EUR")

    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()
