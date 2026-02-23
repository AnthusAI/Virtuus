from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import load_db


def main() -> None:
    db = load_db()
    result = db.execute({"customers": {"pk": "144"}})
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
