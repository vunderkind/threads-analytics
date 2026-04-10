#!/usr/bin/env python3
"""Interactive one-time OAuth helper for the Threads API.

Flow:
  1. You open the printed authorize URL in a browser, log in with your
     Threads account, and grant permissions.
  2. Meta redirects to https://localhost/?code=... (which will show a
     browser error since nothing is listening there — that's expected).
  3. You copy the `code` query param out of the browser URL bar and paste
     it into this script.
  4. The script exchanges the code for a short-lived token, then upgrades
     it to a long-lived (60-day) token, looks up your user ID, and writes
     both back into the .env file.
"""

from __future__ import annotations

import argparse
import sys
import urllib.parse
from pathlib import Path

import httpx
from dotenv import dotenv_values

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"

AUTH_BASE = "https://threads.net/oauth/authorize"
TOKEN_BASE = "https://graph.threads.net/oauth/access_token"
LONG_LIVED_BASE = "https://graph.threads.net/access_token"
ME_BASE = "https://graph.threads.net/v1.0/me"

# threads_keyword_search and threads_read_replies are included so that once
# the app is approved for them, a fresh token will automatically have the
# scopes. Meta silently ignores unapproved scopes during the consent dialog.
SCOPES = "threads_basic,threads_manage_insights,threads_keyword_search,threads_read_replies"


PLACEHOLDER_VALUES = {
    "your_meta_app_id",
    "your_meta_app_secret",
    "your_anthropic_api_key",
    "",
}


def main() -> int:
    if not ENV_PATH.exists():
        print(f"ERROR: {ENV_PATH} does not exist. Copy .env.example to .env first.", file=sys.stderr)
        return 1

    env = dotenv_values(ENV_PATH)
    app_id = (env.get("META_APP_ID") or "").strip()
    app_secret = (env.get("META_APP_SECRET") or "").strip()
    redirect_uri = (env.get("META_REDIRECT_URI") or "https://localhost/").strip()

    if app_id in PLACEHOLDER_VALUES:
        print(
            f"ERROR: META_APP_ID in {ENV_PATH} is still a placeholder ({app_id!r}).\n"
            "Edit .env and replace it with your real Threads app ID from\n"
            "https://developers.facebook.com/apps → your app → Threads product.",
            file=sys.stderr,
        )
        return 1
    if app_secret in PLACEHOLDER_VALUES:
        print(
            f"ERROR: META_APP_SECRET in {ENV_PATH} is still a placeholder.\n"
            "Edit .env and replace it with your real Threads app secret.",
            file=sys.stderr,
        )
        return 1
    if not app_id.isdigit():
        print(
            f"ERROR: META_APP_ID={app_id!r} does not look like a numeric app ID.\n"
            "A real Meta app ID is a long number like 1234567890123456.",
            file=sys.stderr,
        )
        return 1

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--code",
        default=None,
        help="Authorization code from the OAuth redirect (skips the interactive prompt)",
    )
    args = parser.parse_args()

    print(f"Using META_APP_ID={app_id}")
    print(f"Using META_REDIRECT_URI={redirect_uri}")
    print(
        "\nIMPORTANT: the redirect URI above must be registered in your Meta app\n"
        "under Threads product → Settings → Valid OAuth Redirect URIs. If it is\n"
        "not, you will get an 'unknown error' (error_code 1).\n"
    )

    authorize_params = {
        "client_id": app_id,
        "redirect_uri": redirect_uri,
        "scope": SCOPES,
        "response_type": "code",
    }
    authorize_url = f"{AUTH_BASE}?{urllib.parse.urlencode(authorize_params)}"

    if args.code:
        code = args.code.strip()
    else:
        print("Step 1. Open this URL in your browser, log in, and click Allow:\n")
        print(f"  {authorize_url}\n")
        print("Step 2. After approving, Meta will redirect you to a URL that")
        print("        starts with your redirect URI and looks like:")
        print(f"        {redirect_uri}?code=AQD...#_")
        print("        (the page will fail to load — that's fine, we just need the code)\n")
        code = input("Paste the `code` value from that URL: ").strip()

    if not code:
        print("no code given, aborting", file=sys.stderr)
        return 1
    # Meta appends a #_ fragment to the redirect URL — strip it.
    if code.endswith("#_"):
        code = code[:-2]
    # Also strip URL fragment if someone pasted the full URL
    if code.startswith("http"):
        try:
            parsed = urllib.parse.urlparse(code)
            qs = urllib.parse.parse_qs(parsed.query)
            code = qs.get("code", [""])[0]
        except Exception:
            pass

    print(f"Exchanging code (length={len(code)}, starts with {code[:8]}...) for token...")

    with httpx.Client(timeout=30.0) as client:
        # 1. Short-lived token
        resp = client.post(
            TOKEN_BASE,
            data={
                "client_id": app_id,
                "client_secret": app_secret,
                "grant_type": "authorization_code",
                "redirect_uri": redirect_uri,
                "code": code,
            },
        )
        if resp.status_code >= 400:
            print(
                f"\n✗ short-lived token exchange failed with {resp.status_code}",
                file=sys.stderr,
            )
            print(f"  response body: {resp.text}", file=sys.stderr)
            print(
                "\n  Note: Meta consumes auth codes on the FIRST exchange attempt\n"
                "  (success or failure). This code is now burnt — you need to\n"
                "  re-run the authorize URL above to get a fresh code.",
                file=sys.stderr,
            )
            return 1
        short = resp.json()
        short_token = short["access_token"]
        user_id = str(short.get("user_id", ""))
        print(f"✓ short-lived token acquired (user_id={user_id})")

        # 2. Long-lived token
        resp = client.get(
            LONG_LIVED_BASE,
            params={
                "grant_type": "th_exchange_token",
                "client_secret": app_secret,
                "access_token": short_token,
            },
        )
        if resp.status_code >= 400:
            print(
                f"\n✗ long-lived token exchange failed with {resp.status_code}",
                file=sys.stderr,
            )
            print(f"  response body: {resp.text}", file=sys.stderr)
            return 1
        long_data = resp.json()
        long_token = long_data["access_token"]
        expires_in = long_data.get("expires_in")
        print(f"✓ long-lived token acquired (expires_in={expires_in}s)")

        # 3. /me to confirm + grab user_id if we didn't get it
        resp = client.get(
            ME_BASE,
            params={"fields": "id,username", "access_token": long_token},
        )
        if resp.status_code >= 400:
            print(
                f"\n✗ /me failed with {resp.status_code}: {resp.text}",
                file=sys.stderr,
            )
            return 1
        me = resp.json()
        if not user_id:
            user_id = str(me.get("id", ""))
        username = me.get("username", "")
        print(f"✓ logged in as @{username} (id={user_id})")

    _update_env(ENV_PATH, "THREADS_ACCESS_TOKEN", long_token)
    _update_env(ENV_PATH, "THREADS_USER_ID", user_id)
    if username:
        _update_env(ENV_PATH, "THREADS_HANDLE", username)
    print(f"\n✓ .env updated at {ENV_PATH}")
    print("You can now run: threads-analytics run")
    return 0


def _update_env(path: Path, key: str, value: str) -> None:
    lines: list[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    found = False
    out: list[str] = []
    for line in lines:
        if line.startswith(f"{key}="):
            out.append(f"{key}={value}\n")
            found = True
        else:
            out.append(line)
    if not found:
        if out and not out[-1].endswith("\n"):
            out[-1] += "\n"
        out.append(f"{key}={value}\n")
    path.write_text("".join(out), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
