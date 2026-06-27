"""First-name extraction and AI-based salutation/gender determination.

Extracted verbatim from web_app.py (the local copies used by the Flask routes).
No behaviour change. Note: invoice_tracker.py contains parallel implementations
used during import; consolidating the two is a deliberate follow-up.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import requests


def extract_first_name(customer_name: str) -> Optional[str]:
    """
    Extract the first name from a customer name.
    Handles various formats like "Max Mustermann", "Mustermann, Max", etc.

    Args:
        customer_name: Full customer name

    Returns:
        First name or None if extraction fails
    """
    if not customer_name:
        return None

    # Remove common titles
    name_clean = customer_name.strip()
    for title in ["Dr.", "Prof.", "Dipl.-Ing.", "Ing."]:
        name_clean = name_clean.replace(title, "").strip()

    # Try different patterns
    # Pattern 1: "Vorname Nachname" or "Vorname Mittelname Nachname"
    parts = name_clean.split()
    if len(parts) >= 2:
        # First part is likely the first name
        return parts[0].strip()

    # Pattern 2: "Nachname, Vorname"
    if "," in name_clean:
        parts = name_clean.split(",")
        if len(parts) >= 2:
            return parts[1].strip().split()[0] if parts[1].strip() else None

    # If only one part, return it
    if len(parts) == 1:
        return parts[0].strip()

    return None


def determine_gender_via_ai(first_name: str) -> Optional[str]:
    """
    Use Nebius AI (Meta Llama 70B) to determine the gender based on first name.

    Args:
        first_name: The first name to analyze

    Returns:
        "Herr" for male, "Frau" for female, or None if uncertain
    """
    try:
        api_key = os.getenv('NEBIUS_API_KEY')
        if not api_key:
            logging.error("NEBIUS_API_KEY not found in environment")
            return None

        # Nebius Studio API endpoint (OpenAI-compatible)
        url = "https://api.studio.nebius.com/v1/chat/completions"

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        # Prompt for the AI
        prompt = f"""Bestimme das Geschlecht des Vornamens "{first_name}".
Antworte NUR mit einem dieser Wörter:
- "männlich" wenn der Name typischerweise männlich ist
- "weiblich" wenn der Name typischerweise weiblich ist
- "unbekannt" wenn du dir nicht sicher bist

Antwort:"""

        payload = {
            "model": "meta-llama/Llama-3.3-70B-Instruct",
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,
            "max_tokens": 10
        }

        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()

        data = response.json()
        ai_response = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip().lower()

        logging.info(f"AI response for '{first_name}': {ai_response}")

        # Parse response
        if "männlich" in ai_response or "male" in ai_response:
            return "Herr"
        elif "weiblich" in ai_response or "female" in ai_response:
            return "Frau"
        else:
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to call Nebius AI for '{first_name}': {e}")
        return None
    except Exception as e:
        logging.error(f"Error determining gender for '{first_name}': {e}")
        return None


def determine_salutation_for_customer(customer_name: str) -> Optional[str]:
    """
    Determine salutation for a customer by extracting first name and using AI.

    Args:
        customer_name: Full customer name

    Returns:
        "Herr", "Frau", or None
    """
    first_name = extract_first_name(customer_name)
    if not first_name:
        logging.warning(f"Could not extract first name from: {customer_name}")
        return None

    return determine_gender_via_ai(first_name)
