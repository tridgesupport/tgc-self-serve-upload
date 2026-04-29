#!/usr/bin/env python3
"""
TGC Workflow Test Agent
========================
Hits every key endpoint on the live Render deployment and simulates the
full vendor registration → admin activation → product pull workflow.

Usage:
    python test_agent.py                              # tests live Render URL
    python test_agent.py --url http://localhost:8000  # tests local dev server
    python test_agent.py --url https://tgc-self-serve-upload.onrender.com

Output:
    Prints a colour-coded pass/fail report and writes test_results.json
    (consumed by fix_agent.py).
"""

import argparse
import json
import sys
import time
import traceback
import uuid
from datetime import datetime

import requests

# ── Colours ──────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}✓{RESET} {msg}")
def fail(msg): print(f"  {RED}✗{RESET} {msg}")
def warn(msg): print(f"  {YELLOW}⚠{RESET} {msg}")
def info(msg): print(f"  {CYAN}→{RESET} {msg}")


# ── Test runner ───────────────────────────────────────────────────────────────

class TestAgent:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base = base_url.rstrip("/")
        self.timeout = timeout
        self.results: list[dict] = []
        self._test_vendor_id = f"TEST-{uuid.uuid4().hex[:6].upper()}"

    # ── helpers ───────────────────────────────────────────────────────────────

    def _record(self, name: str, passed: bool, detail: str = "", response=None):
        entry = {
            "test":   name,
            "passed": passed,
            "detail": detail,
            "ts":     datetime.utcnow().isoformat(),
        }
        if response is not None:
            entry["status_code"] = response.status_code
            try:
                entry["body_snippet"] = response.text[:400]
            except Exception:
                pass
        self.results.append(entry)
        return passed

    def _get(self, path: str, **kwargs):
        return requests.get(f"{self.base}{path}", timeout=self.timeout, **kwargs)

    def _post(self, path: str, **kwargs):
        return requests.post(f"{self.base}{path}", timeout=self.timeout, **kwargs)

    def _patch(self, path: str, **kwargs):
        return requests.patch(f"{self.base}{path}", timeout=self.timeout, **kwargs)

    def _put(self, path: str, **kwargs):
        return requests.put(f"{self.base}{path}", timeout=self.timeout, **kwargs)

    # ── individual tests ──────────────────────────────────────────────────────

    def test_health(self):
        name = "GET /api/health"
        try:
            r = self._get("/api/health")
            if r.status_code == 200 and r.json().get("status") == "ok":
                ok(name)
                return self._record(name, True, "status=ok", r)
            fail(f"{name} → {r.status_code}: {r.text[:200]}")
            return self._record(name, False, r.text[:200], r)
        except Exception as e:
            fail(f"{name} → exception: {e}")
            return self._record(name, False, str(e))

    def test_public_pages(self):
        pages = [
            ("/support",      "Customer Support"),
            ("/terms",        "Terms of Service"),
            ("/privacy",      "Privacy Policy"),
            ("/cancellation", "Cancellation"),
        ]
        all_ok = True
        for path, label in pages:
            name = f"GET {path}"
            try:
                r = self._get(path)
                if r.status_code == 200 and "<!DOCTYPE html" in r.text:
                    ok(f"{name} ({label})")
                    self._record(name, True, "HTML page returned", r)
                else:
                    fail(f"{name} → {r.status_code}")
                    self._record(name, False, r.text[:200], r)
                    all_ok = False
            except Exception as e:
                fail(f"{name} → {e}")
                self._record(name, False, str(e))
                all_ok = False
        return all_ok

    def test_pages_api(self):
        name = "GET /api/pages"
        try:
            r = self._get("/api/pages")
            data = r.json()
            slugs = list(data.keys())
            expected = {"customer-support", "terms-of-service", "privacy-policy", "cancellation-policy"}
            missing = expected - set(slugs)
            if r.status_code == 200 and not missing:
                ok(f"{name} → {len(slugs)} pages returned")
                return self._record(name, True, f"slugs={slugs}", r)
            detail = f"missing={missing}" if missing else f"status={r.status_code}"
            fail(f"{name} → {detail}")
            return self._record(name, False, detail, r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    def test_page_edit(self):
        slug = "customer-support"
        name = f"PUT /api/pages/{slug}"
        try:
            r_before = self._get(f"/api/pages/{slug}")
            original = r_before.json().get("content", "")
            marker = f"<!-- test-edit-{uuid.uuid4().hex[:8]} -->"
            new_content = original + marker
            r = self._put(f"/api/pages/{slug}", json={"content": new_content})
            if r.status_code != 200:
                fail(f"{name} → {r.status_code}: {r.text[:200]}")
                return self._record(name, False, r.text[:200], r)
            # Verify round-trip
            r_after = self._get(f"/api/pages/{slug}")
            if marker in r_after.json().get("content", ""):
                ok(f"{name} → edit persisted")
                # Restore original
                self._put(f"/api/pages/{slug}", json={"content": original})
                return self._record(name, True, "edit + restore OK", r)
            fail(f"{name} → edit did not persist on re-fetch")
            return self._record(name, False, "content mismatch after PUT", r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    def test_detect_platform_shopify_dot(self):
        name = "POST /api/detect-platform (.myshopify.com)"
        try:
            r = self._post("/api/detect-platform",
                           json={"url": "demo.myshopify.com"}, timeout=15)
            data = r.json()
            if r.status_code == 200 and data.get("isShopify") is True:
                ok(f"{name} → confidence={data.get('confidence')}")
                return self._record(name, True, str(data), r)
            fail(f"{name} → {data}")
            return self._record(name, False, str(data), r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    def test_detect_platform_shopify_custom(self):
        name = "POST /api/detect-platform (custom Shopify domain)"
        try:
            r = self._post("/api/detect-platform",
                           json={"url": "theuchistore.com"}, timeout=20)
            data = r.json()
            if r.status_code == 200 and data.get("isShopify") is True:
                ok(f"{name} → method={data.get('method')}, confidence={data.get('confidence')}")
                return self._record(name, True, str(data), r)
            # Not a hard failure — site may be down
            warn(f"{name} → isShopify={data.get('isShopify')} (site may be slow/down)")
            return self._record(name, False,
                                f"isShopify={data.get('isShopify')}, method={data.get('method')}", r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    def test_detect_platform_non_shopify(self):
        name = "POST /api/detect-platform (non-Shopify)"
        try:
            r = self._post("/api/detect-platform",
                           json={"url": "example.wixsite.com/demo"}, timeout=15)
            data = r.json()
            if r.status_code == 200 and data.get("isShopify") is False:
                ok(f"{name} → correctly identified as non-Shopify")
                return self._record(name, True, str(data), r)
            fail(f"{name} → expected isShopify=False, got {data}")
            return self._record(name, False, str(data), r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    def test_vendor_register(self):
        name = "POST /api/vendors (register)"
        payload = {
            "vendorId":       self._test_vendor_id,
            "brandName":      "Test Brand Agent",
            "fullLegalName":  "Test Brand Agent Pvt Ltd",
            "storeUrl":       "testbrand.myshopify.com",
            "plan":           "basic",
            "currency":       "INR",
            "contactName":    "Test User",
            "contactEmail":   "test@testbrand.com",
            "contactPhone":   "+91 99999 00000",
            "pan":            "ABCDE1234F",
            "billingCity":    "Mumbai",
            "billingState":   "Maharashtra",
            "billingPin":     "400001",
            "billingCountry": "India",
            "categories":     ["Candles & Diffusers"],
            "processingDays": "3",
            "shippingRegion": "India only",
            "warehouseCities":["Mumbai"],
            "accountName":    "Test Brand Agent",
            "accountNumber":  "123456789012",
            "ifsc":           "HDFC0001234",
            "apiVersion":     "2025-04",
            "submittedAt":    datetime.utcnow().isoformat(),
            "createdAt":      datetime.utcnow().strftime("%d %b %Y"),
            "submitted":      True,
        }
        try:
            r = self._post("/api/vendors", json=payload)
            data = r.json()
            if r.status_code in (200, 201) and data.get("id") == self._test_vendor_id:
                ok(f"{name} → id={data['id']}, status={data.get('status')}")
                return self._record(name, True, f"vendor_id={data['id']}", r)
            fail(f"{name} → {r.status_code}: {r.text[:300]}")
            return self._record(name, False, r.text[:300], r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    def test_vendor_list(self):
        name = "GET /api/vendors"
        try:
            r = self._get("/api/vendors")
            data = r.json()
            vendors = data.get("vendors", [])
            ids = [v["id"] for v in vendors]
            if r.status_code == 200 and self._test_vendor_id in ids:
                ok(f"{name} → {data['count']} vendors, test vendor present")
                return self._record(name, True, f"count={data['count']}", r)
            if r.status_code == 200:
                warn(f"{name} → {data['count']} vendors returned but test vendor not found (may be expected)")
                return self._record(name, False, "test vendor missing from list", r)
            fail(f"{name} → {r.status_code}")
            return self._record(name, False, r.text[:200], r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    def test_vendor_activate(self):
        name = f"PATCH /api/vendors/{self._test_vendor_id} (activate)"
        try:
            r = self._patch(f"/api/vendors/{self._test_vendor_id}",
                            json={"status": "active"})
            data = r.json()
            if r.status_code == 200 and data.get("status") == "active":
                ok(f"{name} → activated_at={data.get('activated_at', '—')}")
                return self._record(name, True, f"status=active", r)
            fail(f"{name} → {r.status_code}: {r.text[:200]}")
            return self._record(name, False, r.text[:200], r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    def test_vendor_notes(self):
        name = f"PATCH /api/vendors/{self._test_vendor_id} (notes)"
        try:
            note = f"Automated test note {uuid.uuid4().hex[:6]}"
            r = self._patch(f"/api/vendors/{self._test_vendor_id}", json={"notes": note})
            data = r.json()
            if r.status_code == 200 and data.get("notes") == note:
                ok(f"{name} → notes persisted")
                return self._record(name, True, "notes updated", r)
            fail(f"{name} → {r.status_code}: {r.text[:200]}")
            return self._record(name, False, r.text[:200], r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    def test_scrape_status_unknown_job(self):
        name = "GET /api/scrape-status/{unknown_id}"
        try:
            r = self._get(f"/api/scrape-status/nonexistent-job-{uuid.uuid4().hex}")
            if r.status_code == 404:
                ok(f"{name} → correctly returns 404")
                return self._record(name, True, "404 as expected", r)
            fail(f"{name} → expected 404, got {r.status_code}")
            return self._record(name, False, f"status={r.status_code}", r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    def test_frontend_loads(self):
        name = "GET / (frontend SPA)"
        try:
            r = self._get("/")
            if r.status_code == 200 and "TGC" in r.text and "<!DOCTYPE html" in r.text:
                ok(f"{name} → SPA loaded ({len(r.text):,} bytes)")
                return self._record(name, True, f"size={len(r.text)}", r)
            fail(f"{name} → {r.status_code} or unexpected content")
            return self._record(name, False, r.text[:200], r)
        except Exception as e:
            fail(f"{name} → {e}")
            return self._record(name, False, str(e))

    # ── run all ───────────────────────────────────────────────────────────────

    def run(self):
        print(f"\n{BOLD}TGC Workflow Test Agent{RESET}")
        print(f"Target: {CYAN}{self.base}{RESET}")
        print(f"Time:   {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print("─" * 60)

        sections = [
            ("Infrastructure",   [self.test_health, self.test_frontend_loads]),
            ("Public Pages",     [self.test_public_pages, self.test_pages_api, self.test_page_edit]),
            ("Platform Detection", [
                self.test_detect_platform_shopify_dot,
                self.test_detect_platform_shopify_custom,
                self.test_detect_platform_non_shopify,
            ]),
            ("Vendor Workflow",  [
                self.test_vendor_register,
                self.test_vendor_list,
                self.test_vendor_activate,
                self.test_vendor_notes,
            ]),
            ("Job Endpoints",    [self.test_scrape_status_unknown_job]),
        ]

        for section, tests in sections:
            print(f"\n{BOLD}{section}{RESET}")
            for t in tests:
                try:
                    t()
                except Exception:
                    name = getattr(t, "__name__", str(t))
                    tb = traceback.format_exc()
                    fail(f"{name} → unhandled exception")
                    self._record(name, False, tb)

        # ── Summary ───────────────────────────────────────────────────────────
        passed  = sum(1 for r in self.results if r["passed"])
        failed  = sum(1 for r in self.results if not r["passed"])
        total   = len(self.results)
        pct     = int(passed / total * 100) if total else 0
        colour  = GREEN if failed == 0 else (YELLOW if pct >= 70 else RED)

        print("\n" + "─" * 60)
        print(f"{BOLD}Results: {colour}{passed}/{total} passed ({pct}%){RESET}")

        if failed:
            print(f"\n{RED}{BOLD}Failed tests:{RESET}")
            for r in self.results:
                if not r["passed"]:
                    print(f"  {RED}✗{RESET} {r['test']}")
                    if r.get("detail"):
                        snippet = r["detail"][:120].replace("\n", " ")
                        print(f"    {YELLOW}{snippet}{RESET}")

        # Write results file for fix_agent
        out_file = "test_results.json"
        meta = {
            "base_url":    self.base,
            "run_at":      datetime.utcnow().isoformat(),
            "passed":      passed,
            "failed":      failed,
            "total":       total,
            "test_vendor_id": self._test_vendor_id,
            "tests":       self.results,
        }
        with open(out_file, "w") as f:
            json.dump(meta, f, indent=2)
        print(f"\n{CYAN}→ Results written to {out_file}{RESET}")

        return failed == 0


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TGC workflow test agent")
    parser.add_argument(
        "--url",
        default="https://tgc-self-serve-upload.onrender.com",
        help="Base URL of the deployment to test",
    )
    parser.add_argument(
        "--timeout", type=int, default=30,
        help="Per-request timeout in seconds (default 30)",
    )
    args = parser.parse_args()

    agent = TestAgent(args.url, timeout=args.timeout)
    success = agent.run()
    sys.exit(0 if success else 1)
