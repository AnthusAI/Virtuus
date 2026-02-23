from __future__ import annotations

import json

from common import load_db


def main() -> None:
    db = load_db()
    query = {
        "inventory": {
            "pk": "3243",
            "include": {
                "film": {"fields": ["film_id", "title", "rating"]},
                "rentals": {
                    "fields": ["rental_id", "rental_date", "customer_id", "customer"],
                    "include": {
                        "customer": {
                            "fields": [
                                "customer_id",
                                "first_name",
                                "last_name",
                                "email",
                            ]
                        }
                    },
                },
            },
        }
    }
    result = db.execute(query)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
