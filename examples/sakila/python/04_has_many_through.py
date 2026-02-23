from __future__ import annotations

import json

from common import load_db


def main() -> None:
    db = load_db()
    query = {
        "films": {
            "pk": "714",
            "include": {
                "actors": {"fields": ["actor_id", "first_name", "last_name"]}
            },
        }
    }
    result = db.execute(query)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
