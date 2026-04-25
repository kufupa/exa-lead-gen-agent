#!/usr/bin/env python3
"""One-shot Browser Use Cloud session to sign in to Outlook web and persist cookies on a profile.

Loads repo-root `.env` if present (KEY=value lines; no python-dotenv dependency).

Needs BROWSER_USE_API_KEY (starts with bu_). Optional BROWSER_USE_PROFILE_ID - if unset, creates
a new named profile and prints its id (save in .env for later send scripts).

Flow:
  1) Create or load profile
  2) Open idle cloud browser session (keep_alive) with live_url
  3) Optional: agent navigates to Outlook (costs a small LLM run)
  4) You complete Microsoft login in the live viewer
  5) Press Enter, then sessions.stop() so profile state persists (per Browser Use docs)

Docs: https://docs.browser-use.com/cloud/guides/authentication
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

DEFAULT_OUTLOOK_URL = "https://outlook.office.com/mail/"


def _load_dotenv(path: Path) -> None:
    """Set os.environ from KEY=value lines; does not override existing env."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        if not key:
            continue
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        if key not in os.environ:
            os.environ[key] = val


def _require_api_key() -> str:
    _load_dotenv(_ROOT / ".env")
    key = (os.environ.get("BROWSER_USE_API_KEY") or "").strip()
    if not key:
        print(
            "Set BROWSER_USE_API_KEY in .env or env (bu_...). "
            "See https://cloud.browser-use.com/settings",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return key


async def _async_main(args: argparse.Namespace) -> int:
    try:
        from browser_use_sdk.v3 import AsyncBrowserUse
    except ImportError as e:
        print(f"pip install browser-use-sdk  ({e})", file=sys.stderr)
        return 2

    _require_api_key()
    client = AsyncBrowserUse()

    profile_id = (args.profile_id or os.environ.get("BROWSER_USE_PROFILE_ID") or "").strip()
    created_new_profile = False
    if not profile_id:
        prof = await client.profiles.create(name=args.profile_name)
        profile_id = str(prof.id)
        created_new_profile = True
        print(f"Created profile name={args.profile_name!r} id={profile_id}")
        print(f"Add to .env: BROWSER_USE_PROFILE_ID={profile_id}")
    else:
        print(f"Using profile id={profile_id}")

    session = await client.sessions.create(
        None,
        profile_id=profile_id,
        keep_alive=True,
        agentmail=False,
        skills=False,
    )
    sid = str(session.id)
    print(f"session_id={sid}")
    if session.live_url:
        print(f"live_url={session.live_url}")
    print("Open live_url in browser. Complete Microsoft / Outlook sign-in until inbox loads.")

    try:
        if args.agent_navigate:
            task = (
                f"Navigate the active tab to {args.outlook_url!r}. "
                "Do not send any email. If a Microsoft sign-in page appears, stop and output AWAITING_USER. "
                "If the Outlook mail UI (inbox) is visible, output INBOX_READY."
            )
            run = client.run(
                task,
                session_id=sid,
                model=args.model,
                keep_alive=True,
                agentmail=False,
                skills=False,
            )
            result = await run
            print(f"agent_output={result.output!r} status={result.session.status.value}")

        if not args.skip_wait:
            input("When Outlook inbox is ready, press Enter to stop session and save profile... ")
        else:
            print("--skip-wait: stopping immediately (profile may not have login cookies yet).")

        await client.sessions.stop(sid)
        print("sessions.stop() done - profile cookies should persist if login completed.")
        if created_new_profile:
            print(f"Remember: BROWSER_USE_PROFILE_ID={profile_id}")
    except (KeyboardInterrupt, EOFError):
        print("\nInterrupted - stopping session to avoid orphan charges...", file=sys.stderr)
        try:
            await client.sessions.stop(sid)
        except Exception as e:
            print(f"sessions.stop failed: {e}", file=sys.stderr)
        return 130
    finally:
        await client.close()

    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--profile-name",
        default="outlook-bootstrap",
        help="Name for newly created profile when BROWSER_USE_PROFILE_ID unset.",
    )
    p.add_argument(
        "--profile-id",
        default=None,
        help="Override env BROWSER_USE_PROFILE_ID.",
    )
    p.add_argument(
        "--outlook-url",
        default=DEFAULT_OUTLOOK_URL,
        help="Target Outlook web URL.",
    )
    p.add_argument(
        "--agent-navigate",
        action="store_true",
        help="Run one cheap agent task to open Outlook URL (uses LLM).",
    )
    p.add_argument(
        "--model",
        default="gemini-3-flash",
        help="Model id when --agent-navigate (default fast/cheap).",
    )
    p.add_argument(
        "--skip-wait",
        action="store_true",
        help="Do not prompt; stop session immediately after optional navigate.",
    )
    args = p.parse_args()
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
