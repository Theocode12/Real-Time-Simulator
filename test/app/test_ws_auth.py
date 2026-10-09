from __future__ import annotations

from app.core.ws_auth import AuthService


def test_issued_token_is_valid_only_for_its_game() -> None:
    auth = AuthService(secret="test-secret")
    token = auth.issue_token("game-a")

    assert auth.validate(token)
    assert auth.validate_for_game(token, "game-a")
    assert not auth.validate_for_game(token, "game-b")
    assert not AuthService(secret="different-secret").validate(token)


def test_expired_token_is_rejected(monkeypatch) -> None:
    auth = AuthService(secret="test-secret", token_ttl_seconds=1)
    monkeypatch.setattr("app.core.ws_auth.time.time", lambda: 100)
    token = auth.issue_token("game-a")

    monkeypatch.setattr("app.core.ws_auth.time.time", lambda: 101)
    assert not auth.validate(token)
