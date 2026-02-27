#!/usr/bin/env python3
"""
Render a simple HTML gallery of storage-mode charts grouped by backend/profile.

Outputs: benchmarks/output_storage/gallery.html
"""

from __future__ import annotations

import html
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
CHARTS = ROOT / "benchmarks" / "output_storage" / "charts"
OUT = ROOT / "benchmarks" / "output_storage" / "gallery.html"


def main() -> None:
    if not CHARTS.exists():
        print(f"No charts found under {CHARTS}")
        return

    groups = defaultdict(list)
    for img in sorted(CHARTS.glob("*.png")):
        parts = img.stem.split("_")
        if len(parts) < 3:
            groups["misc"].append(img)
            continue
        kind = parts[0]  # latency or memory
        backend = parts[1]
        profile = parts[2]
        groups[(backend, profile, kind)].append(img)

    lines = [
        "<html><head><meta charset='utf-8'><title>Storage Benchmarks</title></head><body>",
        "<h1>Storage Benchmarks</h1>",
    ]
    for (backend, profile, kind), imgs in sorted(groups.items()):
        lines.append(f"<h2>{html.escape(kind.title())} – {backend} / {profile}</h2>")
        for img in imgs:
            rel = img.relative_to(CHARTS.parent)
            lines.append(f"<div><img src='{rel.as_posix()}' alt='{html.escape(img.stem)}' style='max-width:900px'></div>")
        lines.append("<hr/>")
    lines.append("</body></html>")
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
