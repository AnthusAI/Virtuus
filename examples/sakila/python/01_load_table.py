from __future__ import annotations

import json

from common import load_db


def main() -> None:
    db = load_db()
    result = db.execute({"customers": {"pk": "144"}})
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
