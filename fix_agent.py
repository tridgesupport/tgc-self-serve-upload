#!/usr/bin/env python3
"""
TGC Fix Agent
==============
Reads test_results.json (produced by test_agent.py), maps each failure to
the most likely source file, calls Claude API with the error context + code,
and prints a suggested fix.

Optionally applies the fix automatically with --apply.

Usage:
    python fix_agent.py                  # analyse failures, print fixes
    python fix_agent.py --apply          # also write the fix to disk
    python fix_agent.py --run-tests      # run test_agent first, then fix
    python fix_agent.py --url https://...  --run-tests  # custom URL + auto-run

Requirements:
    ANTHROPIC_API_KEY env var (or in .env)
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Colours ──────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

ROOT = Path(__file__).parent

# ── Failure → source file mapping ────────────────────────────────────────────
# Maps keywords in test names to the files most likely responsible.

FILE_MAP = [
    (r"health",                    ["backend/main.py"]),
    (r"frontend|GET /",            ["frontend/index.html"]),
    (r"public.page|/support|/terms|/privacy|/cancellation",
                                   ["backend/main.py"]),
    (r"pages.api|api/pages",       ["backend/main.py"]),
    (r"page.edit|PUT.*pages",      ["backend/main.py"]),
    (r"detect.platform",           ["backend/main.py", "frontend/index.html"]),
    (r"vendor.register|POST.*vendor",
                                   ["backend/main.py", "backend/database.py"]),
    (r"vendor.list|GET.*vendor",   ["backend/main.py", "backend/database.py"]),
    (r"vendor.activate|PATCH.*vendor",
                                   ["backend/main.py", "backend/database.py"]),
    (r"vendor.notes",              ["backend/main.py", "backend/database.py"]),
    (r"scrape.status|scrape",      ["backend/main.py", "backend/scraper.py"]),
    (r"imagekit|upload",           ["backend/imagekit_client.py", "backend/main.py"]),
    (r"drive",                     ["backend/drive_client.py", "backend/main.py"]),
    (r"webhook",                   ["backend/shopify_webhooks.py", "backend/main.py"]),
]

MAX_FILE_CHARS = 8000   # characters per source file sent to Claude
MAX_FILES      = 2      # max source files per failure


def resolve_files(test_name: str) -> list[Path]:
    """Return source files most likely responsible for a failing test."""
    found = []
    for pattern, files in FILE_MAP:
        if re.search(pattern, test_name, re.IGNORECASE):
            for f in files:
                p = ROOT / f
                if p.exists() and p not in found:
                    found.append(p)
            if found:
                break
    # Fallback: backend/main.py
    if not found:
        p = ROOT / "backend/main.py"
        if p.exists():
            found.append(p)
    return found[:MAX_FILES]


def read_file_snippet(path: Path) -> str:
    """Read a source file, truncating to MAX_FILE_CHARS."""
    try:
        text = path.read_text(encoding="utf-8")
        if len(text) > MAX_FILE_CHARS:
            text = text[:MAX_FILE_CHARS] + f"\n\n... [truncated — full file is {len(text):,} chars]"
        return text
    except Exception as e:
        return f"[Could not read {path}: {e}]"


def call_claude(prompt: str, api_key: str) -> str:
    """Call Claude API and return the text response."""
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         api_key,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
        },
        json={
            "model":      "claude-sonnet-4-6",
            "max_tokens": 2048,
            "messages":   [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]


def extract_code_blocks(text: str) -> list[tuple[str, str]]:
    """Return list of (language, code) from markdown code blocks."""
    pattern = r"```(\w*)\n(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL)
    return [(lang or "text", code.strip()) for lang, code in matches]


def apply_fix(fix_text: str, source_files: list[Path]) -> bool:
    """
    Best-effort application of a fix suggested by Claude.
    Looks for unified diff blocks or full-file replacements.
    Returns True if something was written.
    """
    blocks = extract_code_blocks(fix_text)
    applied = False

    for lang, code in blocks:
        # Try to match the code block to a source file by language
        if lang in ("python", "py") or not lang:
            candidates = [f for f in source_files if f.suffix in (".py",)]
        elif lang in ("html", "javascript", "js"):
            candidates = [f for f in source_files if f.suffix in (".html", ".js")]
        else:
            continue

        for cand in candidates:
            original = cand.read_text(encoding="utf-8")
            # Only replace if the block looks like a full rewrite (>50% of original length)
            if len(code) > len(original) * 0.4:
                backup = cand.with_suffix(cand.suffix + ".bak")
                backup.write_text(original, encoding="utf-8")
                cand.write_text(code, encoding="utf-8")
                print(f"  {GREEN}✓ Applied fix to {cand} (backup: {backup.name}){RESET}")
                applied = True
                break

    return applied


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TGC fix agent")
    parser.add_argument("--results", default="test_results.json",
                        help="Path to test_results.json (default: test_results.json)")
    parser.add_argument("--apply",  action="store_true",
                        help="Attempt to apply suggested fixes automatically")
    parser.add_argument("--run-tests", action="store_true",
                        help="Run test_agent.py first before analysing")
    parser.add_argument("--url",
                        default="https://tgc-self-serve-upload.onrender.com",
                        help="Base URL passed to test_agent when --run-tests is set")
    args = parser.parse_args()

    # ── Optionally run tests first ───────────────────────────────────────────
    if args.run_tests:
        print(f"{CYAN}→ Running test_agent.py against {args.url}…{RESET}\n")
        ret = subprocess.run(
            [sys.executable, str(ROOT / "test_agent.py"), "--url", args.url],
            check=False,
        )
        print()
        if ret.returncode == 0:
            print(f"{GREEN}All tests passed — nothing to fix.{RESET}")
            return

    # ── Load results ─────────────────────────────────────────────────────────
    results_path = Path(args.results)
    if not results_path.exists():
        print(f"{RED}✗ {results_path} not found. Run test_agent.py first.{RESET}")
        sys.exit(1)

    with open(results_path) as f:
        data = json.load(f)

    failures = [t for t in data.get("tests", []) if not t["passed"]]
    if not failures:
        print(f"{GREEN}No failures in {results_path} — nothing to fix.{RESET}")
        return

    # ── API key ──────────────────────────────────────────────────────────────
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print(f"{RED}✗ ANTHROPIC_API_KEY is not set. Export it or add it to .env{RESET}")
        sys.exit(1)

    print(f"\n{BOLD}TGC Fix Agent{RESET}")
    print(f"Analysing {RED}{len(failures)}{RESET} failure(s) from {CYAN}{results_path}{RESET}")
    print("─" * 60)

    fixed_count = 0

    for i, failure in enumerate(failures, 1):
        test_name   = failure["test"]
        detail      = failure.get("detail", "")
        status_code = failure.get("status_code", "—")
        body        = failure.get("body_snippet", "")

        print(f"\n{BOLD}[{i}/{len(failures)}] {RED}{test_name}{RESET}")
        print(f"  Status code : {status_code}")
        if detail:
            print(f"  Detail      : {detail[:200]}")

        source_files = resolve_files(test_name)
        if not source_files:
            warn_txt = f"  {YELLOW}⚠ Could not map to a source file — skipping{RESET}"
            print(warn_txt)
            continue

        print(f"  Source files: {', '.join(str(f.relative_to(ROOT)) for f in source_files)}")

        # Build prompt
        file_sections = "\n\n".join(
            f"=== {f.relative_to(ROOT)} ===\n{read_file_snippet(f)}"
            for f in source_files
        )

        prompt = f"""You are a senior Python/FastAPI/JavaScript developer helping debug a production web application called "The Gift Collective Self-Serve Upload" (TGC).

The app is a FastAPI backend + vanilla JS + React (Babel CDN) frontend deployed on Render.

A test agent reported the following failure:

TEST NAME: {test_name}
HTTP STATUS CODE: {status_code}
ERROR DETAIL:
{detail}

RESPONSE BODY SNIPPET:
{body}

RELEVANT SOURCE FILES:
{file_sections}

TASK:
1. Identify the most likely root cause of this test failure.
2. Provide a precise, minimal fix — do NOT rewrite unrelated code.
3. If the fix is a code change, show it as a diff or clearly marked replacement block.
4. If the fix requires an environment variable or external service change, say so explicitly.
5. Be concise — max 300 words + code block.

Reply in this format:
**Root cause:** one sentence
**Fix:**
```<language>
<fixed code>
```
**Notes:** any caveats (optional)
"""

        print(f"  {CYAN}→ Asking Claude for fix…{RESET}")
        try:
            response = call_claude(prompt, api_key)
        except Exception as e:
            print(f"  {RED}✗ Claude API error: {e}{RESET}")
            continue

        print(f"\n{DIM}{'─'*60}{RESET}")
        print(response)
        print(f"{DIM}{'─'*60}{RESET}")

        if args.apply:
            print(f"\n  {CYAN}→ Attempting to apply fix…{RESET}")
            if apply_fix(response, source_files):
                fixed_count += 1
            else:
                print(f"  {YELLOW}⚠ Could not auto-apply — review and apply manually{RESET}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"{BOLD}Fix Agent Summary{RESET}")
    print(f"  Failures analysed : {len(failures)}")
    if args.apply:
        print(f"  Auto-applied      : {fixed_count}")
        if fixed_count:
            print(f"  {YELLOW}Backup files (.bak) created for modified files.{RESET}")
            print(f"  {CYAN}→ Review changes, then: git add -p && git commit{RESET}")
    else:
        print(f"  {CYAN}→ Re-run with --apply to attempt automatic patching{RESET}")


if __name__ == "__main__":
    main()
