#!/usr/bin/env python3
"""Download a Sakila SQLite database and export a small JSON dataset."""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from pathlib import Path
from typing import Iterable
from urllib.request import Request, urlopen

DEFAULT_DB_URL = "https://sq.io/testdata/sakila.db"

PRIMARY_KEYS = {
    "customers": "customer_id",
    "rentals": "rental_id",
    "payments": "payment_id",
    "inventory": "inventory_id",
    "films": "film_id",
    "actors": "actor_id",
    "film_actor": "id",
}

ID_FIELDS = {
    "customers": ["customer_id"],
    "rentals": ["rental_id", "inventory_id", "customer_id"],
    "payments": ["payment_id", "customer_id", "rental_id"],
    "inventory": ["inventory_id", "film_id"],
    "films": ["film_id"],
    "actors": ["actor_id"],
    "film_actor": ["id", "film_id", "actor_id"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Sakila SQLite DB and export a small JSON dataset.",
    )
    parser.add_argument(
        "--output-dir",
        default=Path(__file__).resolve().parent,
        type=Path,
        help="Destination directory (default: examples/sakila).",
    )
    parser.add_argument(
        "--db-url",
        default=DEFAULT_DB_URL,
        help="URL to a Sakila SQLite database.",
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=3,
        help="Number of customers to include (default: 3).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing data output.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-download the SQLite database even if cached.",
    )
    return parser.parse_args()


def download_db(url: str, target_path: Path, refresh: bool) -> None:
    if not url.startswith("https://"):
        raise ValueError(f"Only https:// URLs are allowed, got: {url!r}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and not refresh:
        return
    request = Request(url, headers={"User-Agent": "virtuus-sakila-builder"})
    try:
        with urlopen(request) as response:
            data = response.read()
    except Exception as exc:  # pragma: no cover - network errors vary
        raise RuntimeError(f"Failed to download {url}: {exc}") from exc
    target_path.write_bytes(data)


def connect_db(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"SQLite DB not found at {path}")
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def fetch_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: Iterable[str],
    where_clause: str = "",
    params: Iterable[object] = (),
    order_by: str | None = None,
) -> list[dict]:
    column_sql = ", ".join(columns)
    query = f"SELECT {column_sql} FROM {table}"
    if where_clause:
        query += f" WHERE {where_clause}"
    if order_by:
        query += f" ORDER BY {order_by}"
    rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(row) for row in rows]


def in_clause(values: list[object]) -> tuple[str, list[object]]:
    if not values:
        raise ValueError("Expected non-empty values for IN clause")
    placeholders = ", ".join(["?"] * len(values))
    return f"({placeholders})", values


def write_table(output_root: Path, table: str, rows: list[dict]) -> None:
    pk_field = PRIMARY_KEYS[table]
    table_dir = output_root / table
    table_dir.mkdir(parents=True, exist_ok=True)
    root_resolved = output_root.resolve()
    for row in rows:
        pk_value = row[pk_field]
        path = (table_dir / f"{pk_value}.json").resolve()
        if not str(path).startswith(str(root_resolved)):
            raise ValueError(f"Refusing path traversal for pk {pk_value!r} in table {table!r}")
        with path.open("w", encoding="utf-8") as handle:
            json.dump(row, handle, indent=2, sort_keys=True)


def stringify_fields(rows: list[dict], fields: Iterable[str]) -> None:
    for row in rows:
        for field in fields:
            if field in row and row[field] is not None:
                row[field] = str(row[field])


def ensure_id_field(rows: list[dict], pk_field: str) -> None:
    if pk_field == "id":
        return
    for row in rows:
        if "id" not in row:
            row["id"] = row.get(pk_field)


def build_dataset(conn: sqlite3.Connection, customers: int) -> dict[str, list[dict]]:
    customer_rows = conn.execute(
        """
        SELECT customer_id
        FROM rental
        GROUP BY customer_id
        ORDER BY COUNT(*) DESC, customer_id ASC
        LIMIT ?
        """,
        (customers,),
    ).fetchall()
    customer_ids = [row["customer_id"] for row in customer_rows]
    if not customer_ids:
        raise RuntimeError("No customers found in rental table")

    customer_clause, customer_params = in_clause(customer_ids)
    customers_rows = fetch_rows(
        conn,
        "customer",
        [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "active",
            "create_date",
        ],
        f"customer_id IN {customer_clause}",
        customer_params,
        order_by="customer_id ASC",
    )

    rentals_rows = fetch_rows(
        conn,
        "rental",
        [
            "rental_id",
            "rental_date",
            "inventory_id",
            "customer_id",
            "return_date",
        ],
        f"customer_id IN {customer_clause}",
        customer_params,
        order_by="rental_date ASC",
    )
    rental_ids = [row["rental_id"] for row in rentals_rows]
    if not rental_ids:
        raise RuntimeError("No rentals found for selected customers")

    rental_clause, rental_params = in_clause(rental_ids)
    payments_rows = fetch_rows(
        conn,
        "payment",
        [
            "payment_id",
            "customer_id",
            "rental_id",
            "amount",
            "payment_date",
        ],
        f"rental_id IN {rental_clause}",
        rental_params,
        order_by="payment_date ASC",
    )

    inventory_ids = sorted({row["inventory_id"] for row in rentals_rows})
    inventory_clause, inventory_params = in_clause(inventory_ids)
    inventory_rows = fetch_rows(
        conn,
        "inventory",
        ["inventory_id", "film_id", "store_id"],
        f"inventory_id IN {inventory_clause}",
        inventory_params,
        order_by="inventory_id ASC",
    )

    film_ids = sorted({row["film_id"] for row in inventory_rows})
    film_clause, film_params = in_clause(film_ids)
    films_rows = fetch_rows(
        conn,
        "film",
        ["film_id", "title", "rating", "release_year", "length"],
        f"film_id IN {film_clause}",
        film_params,
        order_by="film_id ASC",
    )

    film_actor_rows = fetch_rows(
        conn,
        "film_actor",
        ["film_id", "actor_id"],
        f"film_id IN {film_clause}",
        film_params,
        order_by="film_id ASC, actor_id ASC",
    )
    for row in film_actor_rows:
        row["id"] = f"{row['film_id']}__{row['actor_id']}"

    actor_ids = sorted({row["actor_id"] for row in film_actor_rows})
    actor_clause, actor_params = in_clause(actor_ids)
    actors_rows = fetch_rows(
        conn,
        "actor",
        ["actor_id", "first_name", "last_name"],
        f"actor_id IN {actor_clause}",
        actor_params,
        order_by="actor_id ASC",
    )

    stringify_fields(customers_rows, ID_FIELDS["customers"])
    stringify_fields(rentals_rows, ID_FIELDS["rentals"])
    stringify_fields(payments_rows, ID_FIELDS["payments"])
    stringify_fields(inventory_rows, ID_FIELDS["inventory"])
    stringify_fields(films_rows, ID_FIELDS["films"])
    stringify_fields(actors_rows, ID_FIELDS["actors"])
    stringify_fields(film_actor_rows, ID_FIELDS["film_actor"])

    ensure_id_field(customers_rows, PRIMARY_KEYS["customers"])
    ensure_id_field(rentals_rows, PRIMARY_KEYS["rentals"])
    ensure_id_field(payments_rows, PRIMARY_KEYS["payments"])
    ensure_id_field(inventory_rows, PRIMARY_KEYS["inventory"])
    ensure_id_field(films_rows, PRIMARY_KEYS["films"])
    ensure_id_field(actors_rows, PRIMARY_KEYS["actors"])

    return {
        "customers": customers_rows,
        "rentals": rentals_rows,
        "payments": payments_rows,
        "inventory": inventory_rows,
        "films": films_rows,
        "film_actor": film_actor_rows,
        "actors": actors_rows,
    }


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    data_root = output_dir / "data"
    cache_root = output_dir / ".cache"
    db_path = cache_root / "sakila.db"

    download_db(args.db_url, db_path, args.refresh)

    if data_root.exists():
        if args.force:
            if not str(data_root.resolve()).startswith(str(output_dir.resolve())):
                raise ValueError(f"Refusing to delete directory outside output_dir: {data_root}")
            shutil.rmtree(data_root)
        else:
            raise SystemExit(
                f"Output data directory already exists: {data_root}. "
                "Use --force to overwrite."
            )

    conn = connect_db(db_path)
    try:
        tables = build_dataset(conn, args.customers)
    finally:
        conn.close()

    for table, rows in tables.items():
        write_table(data_root, table, rows)

    total = sum(len(rows) for rows in tables.values())
    print("Sakila export complete.")
    print(f"Output directory: {data_root}")
    for table, rows in tables.items():
        print(f"  {table}: {len(rows)} records")
    print(f"Total records: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
