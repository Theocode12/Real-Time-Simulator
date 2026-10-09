from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time


class AuthService:
    """Issue and verify expiring, game-scoped owner control tokens."""

    def __init__(self, secret: str | None = None, token_ttl_seconds: int = 3600) -> None:
        if token_ttl_seconds <= 0:
            raise ValueError("token_ttl_seconds must be positive")
        self._secret = secret if secret is not None else os.getenv("GAME_CONTROL_TOKEN_SECRET")
        self.token_ttl_seconds = token_ttl_seconds

    @property
    def configured(self) -> bool:
        return bool(self._secret)

    def issue_token(self, game_id: str) -> str:
        if not self._secret:
            raise RuntimeError("GAME_CONTROL_TOKEN_SECRET is not configured")
        if not game_id:
            raise ValueError("game_id must not be empty")

        claims = {
            "v": 1,
            "game_id": game_id,
            "exp": int(time.time()) + self.token_ttl_seconds,
        }
        encoded_claims = self._encode(json.dumps(claims, separators=(",", ":")).encode())
        signature = hmac.new(self._secret.encode(), encoded_claims.encode(), hashlib.sha256).digest()
        return f"{encoded_claims}.{self._encode(signature)}"

    def validate(self, token: str) -> bool:
        return self._decode_claims(token) is not None

    def validate_for_game(self, token: str, game_id: str) -> bool:
        claims = self._decode_claims(token)
        token_game_id = claims.get("game_id") if claims else None
        return isinstance(token_game_id, str) and isinstance(game_id, str) and hmac.compare_digest(
            token_game_id.encode(), game_id.encode()
        )

    def _decode_claims(self, token: str) -> dict[str, object] | None:
        if not self._secret or not token:
            return None

        try:
            encoded_claims, encoded_signature = token.split(".", maxsplit=1)
            expected_signature = hmac.new(self._secret.encode(), encoded_claims.encode(), hashlib.sha256).digest()
            supplied_signature = self._decode(encoded_signature)
            if not hmac.compare_digest(expected_signature, supplied_signature):
                return None

            claims = json.loads(self._decode(encoded_claims))
            if not isinstance(claims, dict):
                return None
            expires_at = claims.get("exp")
            if (
                claims.get("v") != 1
                or not isinstance(claims.get("game_id"), str)
                or not claims["game_id"]
                or not isinstance(expires_at, int)
                or isinstance(expires_at, bool)
                or expires_at <= time.time()
            ):
                return None
            return claims
        except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
            return None

    @staticmethod
    def _encode(value: bytes) -> str:
        return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")

    @staticmethod
    def _decode(value: str) -> bytes:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
