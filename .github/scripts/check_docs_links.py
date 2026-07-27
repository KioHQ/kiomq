#!/usr/bin/env python3
"""Fail the build if the docs site has a broken internal link, anchor or asset."""

import pathlib
import re
import sys

DOCS = pathlib.Path(__file__).resolve().parents[2] / "docs"
PAGES = {p.name: p.read_text() for p in sorted(DOCS.glob("*.html"))}
IDS = {name: set(re.findall(r'id="([^"]+)"', html)) for name, html in PAGES.items()}

broken = []

for name, html in PAGES.items():
    for ref in re.findall(r'(?:href|src)="([^"]+)"', html):
        if ref.startswith(("http://", "https://", "mailto:", "data:")):
            continue
        page, _, anchor = ref.partition("#")
        if page and page not in PAGES:
            if not (DOCS / page).exists():
                broken.append(f"{name}: missing target {ref}")
            continue
        target = page or name
        if anchor and anchor not in IDS[target]:
            broken.append(f"{name}: missing anchor {ref}")

    # Every tab button must point at a pane that exists.
    for pane in re.findall(r'aria-controls="([^"]+)"', html):
        if pane not in IDS[name]:
            broken.append(f"{name}: missing tab panel #{pane}")

if broken:
    print("\n".join(broken))
    sys.exit(1)

print(f"ok: {len(PAGES)} pages, no broken links")
