"""Table storage implementation."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from virtuus._python.gsi import GSI


@dataclass(frozen=True)
class TableKey:
    """Primary key representation for composite keys."""

    partition: str
    sort: str


class Table:
    """File-backed in-memory table with optional GSIs.

    :param name: Table name.
    :type name: str
    :param primary_key: Primary key field name for simple PKs.
    :type primary_key: str | None
    :param partition_key: Partition key field name for composite PKs.
    :type partition_key: str | None
    :param sort_key: Sort key field name for composite PKs.
    :type sort_key: str | None
    :param directory: Optional directory for file-backed persistence.
    :type directory: str | None
    :param validation: Validation mode: "silent", "warn", or "error".
    :type validation: str
    """

    def __init__(
        self,
        name: str,
        primary_key: Optional[str] = None,
        partition_key: Optional[str] = None,
        sort_key: Optional[str] = None,
        directory: Optional[str] = None,
        validation: str = "silent",
    ) -> None:
        if primary_key is None and partition_key is None:
            raise ValueError("primary_key or partition_key is required")
        if primary_key is not None and partition_key is not None:
            raise ValueError("use either primary_key or partition_key")
        if partition_key is not None and sort_key is None:
            raise ValueError("sort_key is required for composite primary keys")
        if validation not in {"silent", "warn", "error"}:
            raise ValueError("validation must be silent, warn, or error")
        self.name = name
        self.primary_key = primary_key
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.directory = directory
        self.validation = validation
        self.records: dict[Any, dict[str, Any]] = {}
        self.gsis: dict[str, GSI] = {}
        self.warnings: list[str] = []
        self.hook_errors: list[str] = []
        self.on_put: list[Callable[[dict[str, Any]], None]] = []
        self.on_delete: list[Callable[[dict[str, Any]], None]] = []
        self.associations: list[str] = []
        self.last_write_used_atomic: bool = False

    def add_gsi(
        self, name: str, partition_key: str, sort_key: Optional[str] = None
    ) -> None:
        """Register a GSI for this table.

        :param name: GSI name.
        :type name: str
        :param partition_key: GSI partition key field.
        :type partition_key: str
        :param sort_key: Optional sort key field.
        :type sort_key: str | None
        :return: None
        :rtype: None
        """
        self.gsis[name] = GSI(name, partition_key, sort_key)

    def put(self, record: dict[str, Any]) -> None:
        """Insert or update a record.

        :param record: Record data.
        :type record: dict[str, Any]
        :return: None
        :rtype: None
        """
        pk = self._extract_pk(record)
        if pk is None:
            return
        self._validate_gsi_fields(record)
        existing = self.records.get(pk)
        if existing is not None:
            self._remove_from_gsis(pk, existing)
        self.records[pk] = record
        self._index_in_gsis(pk, record)
        if self.directory is not None:
            self._write_record_to_disk(pk, record)
        self._fire_hooks(self.on_put, record)

    def get(self, pk: str, sort: Optional[str] = None) -> Optional[dict[str, Any]]:
        """Get a record by primary key.

        :param pk: Primary key value or partition key for composite.
        :type pk: str
        :param sort: Sort key value for composite keys.
        :type sort: str | None
        :return: Record data or None.
        :rtype: dict[str, Any] | None
        """
        key = self._compose_key(pk, sort)
        return self.records.get(key)

    def delete(self, pk: str, sort: Optional[str] = None) -> None:
        """Delete a record by primary key.

        :param pk: Primary key value or partition key for composite.
        :type pk: str
        :param sort: Sort key value for composite keys.
        :type sort: str | None
        :return: None
        :rtype: None
        """
        key = self._compose_key(pk, sort)
        record = self.records.pop(key, None)
        if record is None:
            return
        self._remove_from_gsis(key, record)
        if self.directory is not None:
            self._delete_record_from_disk(key)
        self._fire_hooks(self.on_delete, record)

    def scan(self) -> list[dict[str, Any]]:
        """Return all records.

        :return: List of record dicts.
        :rtype: list[dict[str, Any]]
        """
        return list(self.records.values())

    def bulk_load(self, records: Iterable[dict[str, Any]]) -> None:
        """Bulk load multiple records.

        :param records: Iterable of record dicts.
        :type records: Iterable[dict[str, Any]]
        :return: None
        :rtype: None
        """
        for record in records:
            self.put(record)

    def count(self, index: Optional[str] = None, value: Optional[Any] = None) -> int:
        """Count records in the table or GSI partition.

        :param index: Optional GSI name.
        :type index: str | None
        :param value: Partition value for GSI count.
        :type value: Any
        :return: Record count.
        :rtype: int
        """
        if index is None:
            return len(self.records)
        gsi = self.gsis.get(index)
        if gsi is None:
            return 0
        return len(gsi.query(value))

    def describe(self) -> dict[str, Any]:
        """Return table metadata.

        :return: Description dict.
        :rtype: dict[str, Any]
        """
        description: dict[str, Any] = {
            "name": self.name,
            "record_count": len(self.records),
            "gsis": list(self.gsis.keys()),
            "associations": list(self.associations),
        }
        if self.primary_key is not None:
            description["primary_key"] = self.primary_key
        else:
            description["partition_key"] = self.partition_key
            description["sort_key"] = self.sort_key
        return description

    def query_gsi(self, name: str, partition_value: Any) -> list[dict[str, Any]]:
        """Query a GSI and return full records.

        :param name: GSI name.
        :type name: str
        :param partition_value: Partition key value.
        :type partition_value: Any
        :return: List of records.
        :rtype: list[dict[str, Any]]
        :raises KeyError: If the GSI does not exist.
        """
        gsi = self.gsis.get(name)
        if gsi is None:
            raise KeyError(f"GSI {name} does not exist")
        result = []
        for pk in gsi.query(partition_value):
            record = self.records.get(pk)
            if record is not None:
                result.append(record)
        return result

    def load_from_dir(self, directory: Optional[str] = None) -> None:
        """Load records from JSON files in a directory.

        :param directory: Directory path override.
        :type directory: str | None
        :return: None
        :rtype: None
        """
        target = directory or self.directory
        if target is None:
            raise ValueError("directory is required")
        if not os.path.exists(target):
            return
        for name in os.listdir(target):
            if not name.endswith(".json"):
                continue
            path = os.path.join(target, name)
            with open(path, "r", encoding="utf-8") as handle:
                record = json.load(handle)
            self.put(record)

    def export(self, directory: str) -> None:
        """Export all records to a directory.

        :param directory: Target directory.
        :type directory: str
        :return: None
        :rtype: None
        """
        os.makedirs(directory, exist_ok=True)
        for pk, record in self.records.items():
            self._validate_pk_for_path(pk)
            filename = self._filename_for_pk(pk)
            path = os.path.join(directory, filename)
            self._write_json_atomic(path, record)

    def _extract_pk(self, record: dict[str, Any]) -> Optional[Any]:
        if self.primary_key is not None:
            pk_value = record.get(self.primary_key)
            if pk_value is None:
                return self._handle_validation(
                    f"missing primary key {self.primary_key}"
                )
            return pk_value
        partition_value = record.get(self.partition_key)
        sort_value = record.get(self.sort_key)
        if partition_value is None or sort_value is None:
            return self._handle_validation("missing composite primary key")
        return TableKey(str(partition_value), str(sort_value))

    def _compose_key(self, pk: str, sort: Optional[str]) -> Any:
        if self.primary_key is not None:
            return pk
        if sort is None:
            raise ValueError("sort key is required for composite primary keys")
        return TableKey(str(pk), str(sort))

    def _index_in_gsis(self, pk: Any, record: dict[str, Any]) -> None:
        for gsi in self.gsis.values():
            gsi.put(pk, record)

    def _remove_from_gsis(self, pk: Any, record: dict[str, Any]) -> None:
        for gsi in self.gsis.values():
            gsi.remove(pk, record)

    def _handle_validation(self, message: str) -> Optional[Any]:
        if self.validation == "silent":
            return None
        if self.validation == "warn":
            self.warnings.append(message)
            return None
        raise ValueError(message)

    def _validate_gsi_fields(self, record: dict[str, Any]) -> None:
        for gsi in self.gsis.values():
            if gsi.partition_key not in record:
                self._handle_validation(f"missing GSI field {gsi.partition_key}")
            if gsi.sort_key is not None and gsi.sort_key not in record:
                self._handle_validation(f"missing GSI field {gsi.sort_key}")

    def _filename_for_pk(self, pk: Any) -> str:
        if isinstance(pk, TableKey):
            name = f"{pk.partition}__{pk.sort}.json"
        else:
            name = f"{pk}.json"
        return name

    def _write_record_to_disk(self, pk: Any, record: dict[str, Any]) -> None:
        self._validate_pk_for_path(pk)
        os.makedirs(self.directory, exist_ok=True)
        filename = self._filename_for_pk(pk)
        path = os.path.join(self.directory, filename)
        self._write_json_atomic(path, record)

    def _delete_record_from_disk(self, pk: Any) -> None:
        self._validate_pk_for_path(pk)
        filename = self._filename_for_pk(pk)
        path = os.path.join(self.directory, filename)
        if os.path.exists(path):
            os.remove(path)

    def _validate_pk_for_path(self, pk: Any) -> None:
        if isinstance(pk, TableKey):
            parts = [pk.partition, pk.sort]
        else:
            parts = [str(pk)]
        for part in parts:
            if "/" in part or "\\" in part:
                raise ValueError("invalid PK characters")

    def _write_json_atomic(self, path: str, record: dict[str, Any]) -> None:
        directory = os.path.dirname(path)
        fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(record, handle)
            os.replace(temp_path, path)
            self.last_write_used_atomic = True
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def _fire_hooks(
        self, hooks: list[Callable[[dict[str, Any]], None]], record: dict[str, Any]
    ) -> None:
        for hook in hooks:
            try:
                hook(record)
            except Exception as exc:
                self.hook_errors.append(str(exc))
