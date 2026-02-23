from __future__ import annotations

import json

from common import load_db


def main() -> None:
    db = load_db()
    query = {
        "customers": {
            "index": "by_email",
            "where": {"email": "CLARA.SHAW@sakilacustomer.org"},
        }
    }
    result = db.execute(query)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
