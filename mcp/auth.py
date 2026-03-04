"""
Authentication Middleware for MCP Server
=========================================
Provides API key authentication for remote MCP deployments (EC2, ECS, Lambda).

When running locally via stdio transport, auth is disabled.
When running remotely via SSE/streamable-http, requests must include
a valid API key in the Authorization header.

Usage:
    # In mcp/.env:
    MCP_API_KEY=your-secret-api-key-here

    # Client connects with:
    Authorization: Bearer your-secret-api-key-here
"""

import hashlib
import hmac
import logging
import os
import secrets
from functools import wraps
from typing import Optional

logger = logging.getLogger(__name__)


class AuthManager:
    """Manages API key authentication for the MCP server."""

    def __init__(self, api_key: Optional[str] = None, enabled: bool = True):
        """
        Initialize auth manager.

        Args:
            api_key: The API key to validate against. If None, auth is disabled.
            enabled: Whether authentication is enabled.
        """
        self.enabled = enabled and api_key is not None
        self._key_hash = (
            hashlib.sha256(api_key.encode()).hexdigest()
            if api_key
            else None
        )
        if self.enabled:
            logger.info("API key authentication enabled")
        else:
            logger.info("Authentication disabled (no API key configured)")

    def validate_key(self, provided_key: str) -> bool:
        """
        Validate a provided API key against the stored hash.

        Args:
            provided_key: The key provided by the client.

        Returns:
            True if valid, False otherwise.
        """
        if not self.enabled:
            return True

        if not provided_key:
            return False

        provided_hash = hashlib.sha256(provided_key.encode()).hexdigest()
        return hmac.compare_digest(provided_hash, self._key_hash)

    def extract_key_from_header(self, auth_header: str) -> Optional[str]:
        """
        Extract API key from Authorization header.

        Supports:
            - Bearer <key>
            - <key> (raw)

        Args:
            auth_header: The Authorization header value.

        Returns:
            The extracted key, or None.
        """
        if not auth_header:
            return None

        auth_header = auth_header.strip()
        if auth_header.lower().startswith("bearer "):
            return auth_header[7:].strip()

        return auth_header

    @staticmethod
    def generate_api_key() -> str:
        """Generate a cryptographically secure API key."""
        return secrets.token_urlsafe(32)


def get_auth_manager() -> AuthManager:
    """
    Create an AuthManager from environment configuration.

    Returns:
        Configured AuthManager instance.
    """
    api_key = os.environ.get("MCP_API_KEY")
    transport = os.environ.get("MCP_TRANSPORT", "stdio").lower()

    # Disable auth for local stdio connections
    if transport == "stdio":
        return AuthManager(api_key=None, enabled=False)

    return AuthManager(api_key=api_key, enabled=True)
