import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import base64
import json

import pytest
from fastapi import HTTPException

from ogrre.internal.identity_provider import (
    AuthResult,
    CilogonIdentityProvider,
    GoogleIdentityProvider,
    IdentityProviderError,
    MultiIdentityProvider,
)


def _make_fake_jwt(payload: dict) -> str:
    header = base64.urlsafe_b64encode(json.dumps({"alg": "RS256"}).encode()).rstrip(b"=").decode()
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    return f"{header}.{body}.signature"


def test_cilogon_verify_id_token_uses_instance_client_id(monkeypatch):
    monkeypatch.setattr(
        "ogrre.internal.identity_provider.auth.get_cilogon_credentials",
        lambda: ("https://cilogon.org/oauth2/token", "cilogon-client-123", "secret"),
    )
    provider = CilogonIdentityProvider()
    fake_signing_key = SimpleNamespace(key="public-key")
    mock_jwks = MagicMock()
    mock_jwks.get_signing_key_from_jwt.return_value = fake_signing_key
    monkeypatch.setattr("ogrre.internal.identity_provider.jwks_client", mock_jwks)

    captured = {}

    def fake_decode(token, key, algorithms, audience, issuer):
        captured["audience"] = audience
        captured["issuer"] = issuer
        return {"sub": "http://cilogon.org/serverA/users/123", "iss": issuer, "aud": audience}

    monkeypatch.setattr("ogrre.internal.identity_provider.jwt.decode", fake_decode)

    claims = provider.verify_id_token("raw-jwt")
    assert claims["sub"] == "http://cilogon.org/serverA/users/123"
    assert claims["provider"] == "cilogon"
    assert captured["audience"] == "cilogon-client-123"
    assert captured["issuer"] == "https://cilogon.org"


def test_cilogon_verify_id_token_wraps_errors(monkeypatch):
    monkeypatch.setattr(
        "ogrre.internal.identity_provider.auth.get_cilogon_credentials",
        lambda: ("https://cilogon.org/oauth2/token", "cilogon-client-123", "secret"),
    )
    provider = CilogonIdentityProvider()
    mock_jwks = MagicMock()
    mock_jwks.get_signing_key_from_jwt.side_effect = ValueError("invalid jwt")
    monkeypatch.setattr("ogrre.internal.identity_provider.jwks_client", mock_jwks)

    with pytest.raises(IdentityProviderError) as exc_info:
        provider.verify_id_token("bad-jwt")
    assert exc_info.value.status_code == 401


def test_cilogon_exchange_code_merges_userinfo_and_passes_redirect_uri(monkeypatch):
    monkeypatch.setattr(
        "ogrre.internal.identity_provider.auth.get_cilogon_credentials",
        lambda: ("https://cilogon.org/oauth2/token", "cilogon-client-123", "secret-xyz"),
    )
    provider = CilogonIdentityProvider()
    monkeypatch.setattr(
        provider,
        "verify_id_token",
        lambda token: {"sub": "http://cilogon.org/serverA/users/123", "iss": "https://cilogon.org"},
    )

    post_calls = []

    def fake_post(url, data=None, headers=None, timeout=None):
        post_calls.append({"url": url, "data": data, "headers": headers})
        if url == "https://cilogon.org/oauth2/token":
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                "id_token": "cilogon-id-token",
                "access_token": "cilogon-access-token",
                "refresh_token": "cilogon-refresh-token",
            }
            return resp
        if url == "https://cilogon.org/oauth2/userinfo":
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                "email": "Scientist@LBL.gov",
                "given_name": "Ada",
                "family_name": "Lovelace",
                "idp_name": "Lawrence Berkeley National Laboratory",
            }
            return resp
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("ogrre.internal.identity_provider.requests.post", fake_post)

    result = provider.exchange_authorization_code(
        "auth-code-abc", redirect_uri="https://ogrre.example.org/login"
    )
    assert post_calls[0]["data"]["redirect_uri"] == "https://ogrre.example.org/login"
    assert result.id_token == "cilogon-id-token"
    assert result.provider == "cilogon"
    assert result.user_info["email"] == "scientist@lbl.gov"
    assert result.user_info["name"] == "Ada Lovelace"
    assert result.user_info["idp_name"] == "Lawrence Berkeley National Laboratory"


def test_multi_identity_provider_routes_by_issuer_and_provider():
    google = MagicMock(spec=GoogleIdentityProvider)
    google.is_configured.return_value = True
    google.client_id = "google-client-id"
    cilogon = MagicMock(spec=CilogonIdentityProvider)
    cilogon.is_configured.return_value = True
    cilogon.client_id = "cilogon-client-id"
    cilogon.authorize_uri = "https://cilogon.org/authorize"

    google.verify_id_token.return_value = {"email": "user@gmail.com", "iss": "https://accounts.google.com"}
    cilogon.verify_id_token.return_value = {"sub": "cilogon-sub-1", "iss": "https://cilogon.org"}

    multi = MultiIdentityProvider(
        default_provider="google",
        google_provider=google,
        cilogon_provider=cilogon,
    )

    enabled = multi.get_enabled_providers()
    assert enabled["google"]["enabled"] is True
    assert enabled["cilogon"]["enabled"] is True
    assert enabled["cilogon"]["client_id"] == "cilogon-client-id"

    cilogon_jwt = _make_fake_jwt({"iss": "https://cilogon.org", "sub": "cilogon-sub-1"})
    google_jwt = _make_fake_jwt({"iss": "https://accounts.google.com", "sub": "google-sub-1"})

    assert multi.verify_id_token(cilogon_jwt)["sub"] == "cilogon-sub-1"
    cilogon.verify_id_token.assert_called_once_with(cilogon_jwt)

    assert multi.verify_id_token(google_jwt)["email"] == "user@gmail.com"
    google.verify_id_token.assert_called_once_with(google_jwt)

    multi.exchange_authorization_code("code-1", redirect_uri="http://localhost:3000/login", provider="cilogon")
    cilogon.exchange_authorization_code.assert_called_once_with(
        "code-1", redirect_uri="http://localhost:3000/login"
    )


def test_data_manager_persists_and_resolves_cilogon_sub(manager):
    manager.db.users.insert_one(
        {
            "email": "researcher@lbl.gov",
            "name": "",
            "picture": "https://example.com/avatar.png",
            "hd": "",
            "default_team": "LBL",
            "roles": {"system": [], "team": {"LBL": ["team_member"]}},
        }
    )

    updated = manager.updateUserObject(
        {
            "email": "researcher@lbl.gov",
            "name": "Dr. Researcher",
            "sub": "http://cilogon.org/serverA/users/999",
            "idp_name": "Lawrence Berkeley National Laboratory",
            "provider": "cilogon",
        }
    )
    assert updated["cilogon_sub"] == "http://cilogon.org/serverA/users/999"

    found = manager.getUserBySub("http://cilogon.org/serverA/users/999")
    assert found is not None
    assert found["email"] == "researcher@lbl.gov"
    assert found["name"] == "Dr. Researcher"
    assert found["picture"] == "https://example.com/avatar.png"
    assert found["idp_name"] == "Lawrence Berkeley National Laboratory"


def test_router_auth_endpoints_support_google_and_cilogon(manager, monkeypatch):
    from ogrre.routers import router

    manager.db.users.insert_one(
        {
            "email": "researcher@lbl.gov",
            "name": "Dr. Researcher",
            "picture": "",
            "hd": "lbl.gov",
            "default_team": "LBL",
            "roles": {"system": ["sys_admin"], "team": {"LBL": ["team_member"]}},
        }
    )

    fake_idp = MagicMock()
    fake_idp.get_enabled_providers.return_value = {
        "google": {"enabled": True, "client_id": "g-id"},
        "cilogon": {"enabled": True, "client_id": "c-id", "authorize_uri": "https://cilogon.org/authorize"},
    }

    def fake_exchange(code, redirect_uri=None, provider=None):
        if code == "cilogon-code":
            return AuthResult(
                id_token="cilogon-jwt",
                access_token="cilogon-access",
                refresh_token="cilogon-refresh",
                user_info={
                    "email": "researcher@lbl.gov",
                    "name": "Dr. Researcher",
                    "sub": "http://cilogon.org/serverA/users/999",
                    "idp_name": "Lawrence Berkeley National Laboratory",
                    "provider": "cilogon",
                },
                provider="cilogon",
            )
        if code == "unauthorized-code":
            return AuthResult(
                id_token="unauth-jwt",
                access_token="unauth-access",
                refresh_token="",
                user_info={
                    "email": "stranger@example.org",
                    "name": "Stranger",
                    "sub": "http://cilogon.org/serverA/users/000",
                    "provider": "cilogon",
                },
                provider="cilogon",
            )
        if code == "no-email-code":
            return AuthResult(
                id_token="no-email-jwt",
                access_token="no-email-access",
                refresh_token="",
                user_info={
                    "sub": "http://cilogon.org/serverA/users/111",
                    "provider": "cilogon",
                },
                provider="cilogon",
            )
        raise IdentityProviderError("Invalid code", status_code=401)

    fake_idp.exchange_authorization_code.side_effect = fake_exchange
    fake_idp.verify_id_token.return_value = {
        "sub": "http://cilogon.org/serverA/users/999",
        "iss": "https://cilogon.org",
    }

    monkeypatch.setattr(router, "data_manager", manager)
    monkeypatch.setattr(router, "REQUIRE_AUTH", True)
    monkeypatch.setattr(router, "identity_provider", fake_idp)

    providers_resp = router.get_auth_providers()
    assert providers_resp["google"]["enabled"] is True
    assert providers_resp["cilogon"]["enabled"] is True

    def make_request(body=None, cookies=None, headers=None, method="POST"):
        req = MagicMock()
        req.json = AsyncMock(return_value=body)
        req.cookies = cookies or {}
        req.headers = headers or {}
        req.method = method
        return req

    with pytest.raises(HTTPException) as unauth_exc:
        asyncio.run(
            router.auth_login(
                make_request({"code": "unauthorized-code", "provider": "cilogon"})
            )
        )
    assert unauth_exc.value.status_code == 403

    with pytest.raises(HTTPException) as no_email_exc:
        asyncio.run(
            router.auth_login(
                make_request({"code": "no-email-code", "provider": "cilogon"})
            )
        )
    assert no_email_exc.value.status_code == 403

    login_response = asyncio.run(
        router.auth_login(
            make_request(
                {
                    "code": "cilogon-code",
                    "provider": "cilogon",
                    "redirect_uri": "http://localhost:3000/login",
                }
            )
        )
    )
    assert login_response.status_code == 200
    set_cookie_headers = [
        v.decode() for k, v in login_response.raw_headers if k.lower() == b"set-cookie"
    ]
    assert any("ogrre_session=cilogon-jwt" in h for h in set_cookie_headers)
    assert any("ogrre_auth_provider=cilogon" in h for h in set_cookie_headers)

    # Subsequent request where CILogon id_token only has `sub` resolves email via cilogon_sub in DB
    auth_user = router.authenticate(
        make_request(
            cookies={
                "ogrre_session": "cilogon-jwt",
                "ogrre_csrf": "csrf-123",
                "ogrre_auth_provider": "cilogon",
            },
            headers={"x-csrf-token": "csrf-123"},
            method="POST",
        ),
        token=None,
    )
    assert auth_user["email"] == "researcher@lbl.gov"

