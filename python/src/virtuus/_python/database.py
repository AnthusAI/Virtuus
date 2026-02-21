"""Database container with schema loading and query execution."""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, Optional

import yaml

from virtuus._python.table import Table


class Database:
    """Collection of tables with schema loading and query helpers."""

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

    def check(self) -> dict[str, dict[str, int]]:  # pragma: no cover
        """
        Run dry-run checks on all tables.

        :return: Mapping of table name to change summary.
        :rtype: dict[str, dict[str, int]]
        """
        return {name: table.check() for name, table in self.tables.items()}

    @classmethod
    def from_schema(  # pragma: no cover
        cls, path: str, data_root: Optional[str] = None
    ) -> "Database":
        """
        Load a database from a YAML schema file.

        :param path: Path to schema YAML file.
        :type path: str
        :param data_root: Optional root directory for table data.
        :type data_root: str | None
        :return: Initialized database.
        :rtype: Database
        """
        with open(path, "r", encoding="utf-8") as handle:
            schema = yaml.safe_load(handle) or {}
        tables_conf = schema.get("tables", {})
        db = cls()
        for name, conf in tables_conf.items():
            directory = conf.get("directory")
            if directory is not None and data_root is not None:
                directory = os.path.join(data_root, directory)
            table = Table(
                name,
                primary_key=conf.get("primary_key"),
                partition_key=conf.get("partition_key"),
                sort_key=conf.get("sort_key"),
                directory=directory,
                validation="warn",
            )
            for gsi_name, gsi_conf in conf.get("gsis", {}).items():
                table.add_gsi(
                    gsi_name,
                    gsi_conf["partition_key"],
                    gsi_conf.get("sort_key"),
                )
            for assoc_name, assoc_conf in conf.get("associations", {}).items():
                kind = assoc_conf.get("type")
                if kind == "belongs_to":
                    table.add_belongs_to(
                        assoc_name,
                        assoc_conf["table"],
                        assoc_conf["foreign_key"],
                    )
                elif kind == "has_many":
                    table.add_has_many(
                        assoc_name,
                        assoc_conf["table"],
                        assoc_conf["index"],
                    )
                elif kind == "has_many_through":
                    table.add_has_many_through(
                        assoc_name,
                        assoc_conf["through"],
                        assoc_conf["index"],
                        assoc_conf["table"],
                        assoc_conf["foreign_key"],
                    )
            db.add_table(name, table)
        for table in db.tables.values():
            if table.directory is not None:
                table.load_from_dir()
        return db

    def describe(self) -> dict[str, dict[str, Any]]:
        """
        Describe all tables in the database.

        :return: Mapping of table name to description dict.
        :rtype: dict[str, dict[str, Any]]
        """
        return {
            name: {
                **table.describe(),
                "stale": table.is_stale(force_scan=False),
            }
            for name, table in self.tables.items()
        }

    def validate(self) -> list[dict[str, str]]:
        """
        Validate referential integrity for belongs_to associations.

        :return: List of violation dicts.
        :rtype: list[dict[str, str]]
        """
        violations: list[dict[str, str]] = []
        for table_name, table in self.tables.items():
            for assoc_name in table.associations:
                definition = table.association_defs.get(assoc_name, {})
                if definition.get("kind") != "belongs_to":
                    continue
                target_table = definition["target_table"]
                foreign_key = definition["foreign_key"]
                for record in table.scan():
                    fk_value = record.get(foreign_key)
                    if fk_value is None:
                        continue  # pragma: no cover
                    target = self.tables[target_table]  # pragma: no cover
                    if target.get(fk_value) is None:
                        violations.append(
                            {
                                "table": table_name,
                                "record_pk": str(record.get(table.primary_key)),
                                "association": assoc_name,
                                "foreign_key": foreign_key,
                                "missing_target": str(fk_value),
                            }
                        )
        return violations

    def execute(self, query: dict[str, Any]) -> Any:
        """
        Execute a query dictionary against the database.

        :param query: Query mapping table name to directives.
        :type query: dict[str, Any]
        :return: Result payload.
        :rtype: Any
        """
        if len(query) != 1:  # pragma: no cover
            raise ValueError("query must target exactly one table")
        table_name, directive = next(iter(query.items()))
        if table_name not in self.tables:  # pragma: no cover
            raise KeyError(f'table "{table_name}" does not exist')
        table = self.tables[table_name]
        directive = directive or {}
        if "pk" in directive:
            result = table.get(str(directive["pk"]), directive.get("sort"))
            if directive.get("fields"):
                result = self._project(result, directive["fields"])
            return self._apply_includes(table_name, result, directive.get("include"))

        if "index" in directive:
            gsi_name = directive["index"]
            if gsi_name not in table.gsis:
                raise KeyError(f'GSI "{gsi_name}" does not exist')
            where = directive.get("where", {})
            partition_field = table.gsis[gsi_name].partition_key
            partition_value = where.get(partition_field)
            sort_condition = self._build_sort_condition(directive.get("sort"))
            descending = directive.get("sort_direction", "asc") == "desc"
            records = table.query_gsi(
                gsi_name, partition_value, sort_condition, descending
            )
        else:
            where = directive.get("where", {})
            records = [
                record for record in table.scan() if self._record_matches(record, where)
            ]

        items = [self._project(record, directive.get("fields")) for record in records]
        start = int(directive.get("next_token", 0) or 0)
        limit = directive.get("limit")
        next_token: Optional[str] = None
        if limit is not None:
            end = start + int(limit)
            if end < len(items):
                next_token = str(end)
            items = items[start:end]
        result: dict[str, Any] = {"items": items}
        if next_token is not None:
            result["next_token"] = next_token
        if directive.get("include"):
            for index, base_record in enumerate(items):  # pragma: no cover
                items[index] = self._apply_includes(
                    table_name, base_record, directive["include"]
                )
        return result

    def _record_matches(self, record: dict[str, Any], where: dict[str, Any]) -> bool:
        for key, expected in where.items():
            if record.get(key) != expected:
                return False
        return True

    def _project(self, record: dict[str, Any], fields: Optional[Iterable[str]]):
        if record is None:  # pragma: no cover
            return None
        if not fields:
            return record
        return {field: record.get(field) for field in fields}

    def _build_sort_condition(  # pragma: no cover
        self, sort: Optional[dict[str, Any]]
    ) -> Optional[callable[[Any], bool]]:
        if not sort:
            return None
        op, value = next(iter(sort.items()))
        from virtuus import Sort

        factory = {
            "eq": Sort.eq,
            "ne": Sort.ne,
            "lt": Sort.lt,
            "lte": Sort.lte,
            "gt": Sort.gt,
            "gte": Sort.gte,
            "between": lambda val: Sort.between(val[0], val[1]),
            "begins_with": Sort.begins_with,
            "contains": Sort.contains,
        }.get(op)
        if factory is None:
            return None
        if op == "between" and isinstance(value, (list, tuple)) and len(value) == 2:
            return factory(value)
        return factory(value)

    def _apply_includes(  # pragma: no cover
        self,
        table_name: str,
        record: Optional[dict[str, Any]],
        includes: Optional[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        if record is None or not includes:
            return record
        enriched = dict(record)
        table = self.tables[table_name]
        for assoc_name, assoc_directive in includes.items():
            related = table.resolve_association(
                assoc_name, record[table.primary_key], self.tables
            )
            assoc_def = table.association_defs.get(assoc_name, {})
            target_table = assoc_def.get("target_table", table_name)
            if related is None:
                enriched[assoc_name] = None
                continue
            if isinstance(related, list):
                nested_items = []
                for item in related:
                    if assoc_directive.get("include"):
                        item = self._apply_includes(
                            target_table, item, assoc_directive["include"]
                        )
                    item = self._project(item, assoc_directive.get("fields"))
                    nested_items.append(item)
                enriched[assoc_name] = nested_items
            else:
                if assoc_directive.get("include"):
                    related = self._apply_includes(
                        target_table, related, assoc_directive["include"]
                    )
                enriched[assoc_name] = self._project(
                    related, assoc_directive.get("fields")
                )
        return enriched
