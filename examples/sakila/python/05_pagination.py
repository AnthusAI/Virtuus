from __future__ import annotations

from common import load_db


def main() -> None:
    db = load_db()
    query = {
        "rentals": {
            "index": "by_customer",
            "where": {"customer_id": "144"},
            "sort_direction": "desc",
            "limit": 5,
            "fields": ["rental_id", "rental_date", "customer_id"],
        }
    }

    page1 = db.execute(query)
    print(f"page1 items: {len(page1['items'])}")
    print(f"page1 next_token: {page1.get('next_token')}")

    if "next_token" in page1:
        query["rentals"]["next_token"] = page1["next_token"]
        page2 = db.execute(query)
        print(f"page2 items: {len(page2['items'])}")
        print(f"page2 next_token: {page2.get('next_token')}")


if __name__ == "__main__":
    main()
