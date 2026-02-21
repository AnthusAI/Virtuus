"""Minimal database container for tables."""

from __future__ import annotations

from typing import Dict

from virtuus._python.table import Table


class Database:
    """Collection of tables with helper cache operations."""

    def __init__(self) -> None:
        """Initialize an empty database."""
        self.tables: Dict[str, Table] = {}

    def add_table(self, name: str, table: Table) -> None:
        """
        Register a table by name.

        :param name: Table name.
        :type name: str
        :param table: Table instance to register.
        :type table: Table
        :return: None
        :rtype: None
        """
        self.tables[name] = table

    def warm(self) -> None:
        """
        Refresh all tables proactively.

        :return: None
        :rtype: None
        """
        for table in self.tables.values():
            table.warm()

    def check(self) -> dict[str, dict[str, int]]:
        """
        Run dry-run checks on all tables.

        :return: Mapping of table name to change summary.
        :rtype: dict[str, dict[str, int]]
        """
        return {name: table.check() for name, table in self.tables.items()}
