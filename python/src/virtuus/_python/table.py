"""Table storage implementation."""

from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, TypedDict

from virtuus._python.gsi import GSI


@dataclass(frozen=True)
class TableKey:
    """Primary key representation for composite keys."""

    partition: str
    sort: str


class AssociationDef(TypedDict, total=False):
    """Association definition metadata."""

    kind: str
    target_table: str
    foreign_key: str
    index: str
    through_table: str
    through_index: str
    target_foreign_key: str


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
    :param check_interval: Minimum seconds between staleness checks.
    :type check_interval: int
    :param auto_refresh: Whether queries should auto-refresh when stale.
    :type auto_refresh: bool
    """

    def __init__(
        self,
        name: str,
        primary_key: Optional[str] = None,
        partition_key: Optional[str] = None,
        sort_key: Optional[str] = None,
        directory: Optional[str] = None,
        validation: str = "silent",
        check_interval: int = 0,
        auto_refresh: bool = True,
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
        self.check_interval = check_interval
        self.auto_refresh = auto_refresh
        self.records: dict[Any, dict[str, Any]] = {}
        self.gsis: dict[str, GSI] = {}
        self.warnings: list[str] = []
        self.hook_errors: list[str] = []
        self.refresh_errors: list[str] = []
        self.on_put: list[Callable[[dict[str, Any]], None]] = []
        self.on_delete: list[Callable[[dict[str, Any]], None]] = []
        self.on_refresh: list[Callable[[dict[str, int]], None]] = []
        self.associations: list[str] = []
        self.association_defs: dict[str, AssociationDef] = {}
        self.last_write_used_atomic: bool = False
        self._manifest: dict[str, tuple[int, int]] = {}
        self._last_dir_mtime: Optional[float] = None
        self._last_check_time: Optional[float] = None
        self._last_is_stale: bool = False
        self.last_change_summary: dict[str, int] = {
            "added": 0,
            "modified": 0,
            "deleted": 0,
            "reread": 0,
        }

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

    def _insert_record_from_load(self, record: dict[str, Any]) -> None:
        """Insert a record during load without writing to disk."""
        pk = self._extract_pk(record)
        if pk is None:
            return
        self._validate_gsi_fields(record)
        existing = self.records.get(pk)
        if existing is not None:
            self._remove_from_gsis(pk, existing)
        self.records[pk] = record
        self._index_in_gsis(pk, record)
        self._fire_hooks(self.on_put, record)

    def add_belongs_to(self, name: str, target_table: str, foreign_key: str) -> None:
        """
        Register a belongs_to association.

        :param name: Association name.
        :type name: str
        :param target_table: Target table name.
        :type target_table: str
        :param foreign_key: Foreign key field on this table.
        :type foreign_key: str
        :return: None
        :rtype: None
        """
        self._register_association(
            name,
            {
                "kind": "belongs_to",
                "target_table": target_table,
                "foreign_key": foreign_key,
            },
        )

    def add_has_many(self, name: str, target_table: str, index: str) -> None:
        """
        Register a has_many association.

        :param name: Association name.
        :type name: str
        :param target_table: Target table.
        :type target_table: str
        :param index: GSI name on the target table.
        :type index: str
        :return: None
        :rtype: None
        """
        self._register_association(
            name,
            {
                "kind": "has_many",
                "target_table": target_table,
                "index": index,
            },
        )

    def add_has_many_through(
        self,
        name: str,
        through_table: str,
        through_index: str,
        target_table: str,
        target_foreign_key: str,
    ) -> None:
        """
        Register a has_many_through association.

        :param name: Association name.
        :type name: str
        :param through_table: Junction table name.
        :type through_table: str
        :param through_index: GSI on junction keyed by this table's PK.
        :type through_index: str
        :param target_table: Target table name.
        :type target_table: str
        :param target_foreign_key: Foreign key field on the junction pointing
            to the target.
        :type target_foreign_key: str
        :return: None
        :rtype: None
        """
        self._register_association(
            name,
            {
                "kind": "has_many_through",
                "through_table": through_table,
                "through_index": through_index,
                "target_table": target_table,
                "target_foreign_key": target_foreign_key,
            },
        )

    def resolve_association(  # pragma: no cover
        self, name: str, pk: str, tables: dict[str, "Table"]
    ) -> Any:
        """
        Resolve an association for a record.

        :param name: Association name.
        :type name: str
        :param pk: Primary key of the source record.
        :type pk: str
        :param tables: Mapping of table name to Table instance.
        :type tables: dict[str, Table]
        :return: Related record(s) or None.
        :rtype: Any
        :raises KeyError: If the association is not defined.
        """
        definition = self.association_defs.get(name)
        if definition is None:
            raise KeyError(f"association {name} not defined")
        record = self.get(pk)
        if record is None:
            return None
        kind = definition["kind"]
        if kind == "belongs_to":
            foreign_key = definition["foreign_key"]
            fk_value = record.get(foreign_key)
            if fk_value is None:
                return None
            target = tables[definition["target_table"]]
            return target.get(fk_value)
        if kind == "has_many":
            key_field = self.primary_key or self.partition_key
            if key_field is None:
                return []
            key_value = record.get(key_field)
            if key_value is None:
                return []
            target = tables[definition["target_table"]]
            return target.query_gsi(definition["index"], key_value)
        if kind == "has_many_through":
            key_field = self.primary_key or self.partition_key
            if key_field is None:
                return []
            key_value = record.get(key_field)
            if key_value is None:
                return []
            through_table = tables[definition["through_table"]]
            assignments = through_table.query_gsi(
                definition["through_index"], key_value
            )
            target_table = tables[definition["target_table"]]
            target_fk = definition["target_foreign_key"]
            related: list[dict[str, Any]] = []
            for assignment in assignments:
                fk_value = assignment.get(target_fk)
                if fk_value is None:
                    continue
                related_record = target_table.get(fk_value)
                if related_record is not None:
                    related.append(related_record)
            return related
        return None

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
        self._maybe_refresh_before_query()
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

    def query_gsi(
        self,
        name: str,
        partition_value: Any,
        sort_condition: Optional[Callable[[Any], bool]] = None,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        """Query a GSI and return full records.

        :param name: GSI name.
        :type name: str
        :param partition_value: Partition key value.
        :type partition_value: Any
        :param sort_condition: Optional sort condition predicate.
        :type sort_condition: Callable[[Any], bool] | None
        :param descending: Whether results should be in descending order.
        :type descending: bool
        :return: List of records.
        :rtype: list[dict[str, Any]]
        :raises KeyError: If the GSI does not exist.
        """
        self._maybe_refresh_before_query()
        gsi = self.gsis.get(name)
        if gsi is None:
            raise KeyError(f"GSI {name} does not exist")
        result = []
        direction = "desc" if descending else "asc"
        for pk in gsi.query(partition_value, sort_condition, direction):
            record = self.records.get(pk)
            if record is not None:
                result.append(record)
        return result

    def is_stale(self, force_scan: bool = False) -> bool:
        """Check whether on-disk files have changed.

        :param force_scan: If True, always scan the directory even if within the
            check interval.
        :type force_scan: bool
        :return: True if changes are detected, otherwise False.
        :rtype: bool
        """
        if self.directory is None:
            return False
        now = time.time()
        if (
            not force_scan
            and self.check_interval > 0
            and self._last_check_time is not None
        ):
            if now - self._last_check_time < self.check_interval:
                return self._last_is_stale
        dir_mtime = self._dir_mtime()
        summary, _, _, _ = self._compute_changes()
        self._last_check_time = now
        self._last_is_stale = any(summary.values())
        self._last_dir_mtime = dir_mtime
        return self._last_is_stale

    def check(self) -> dict[str, int]:
        """Dry-run change detection without mutating table.

        :return: Summary of added, modified, deleted counts.
        :rtype: dict[str, int]
        """
        summary, _, _, _ = self._compute_changes()
        return summary

    def refresh(self) -> dict[str, int]:
        """Incrementally refresh the table from disk.

        :return: Change summary including reread count.
        :rtype: dict[str, int]
        """
        self.refresh_errors = []
        summary, added, modified, deleted = self._compute_changes()
        reread = 0
        for path in added | modified:
            record = self._read_record_file(path)
            if record is None:
                continue  # pragma: no cover
            self.put(record)
            reread += 1
        for path in deleted:
            pk = self._pk_from_filename(os.path.basename(path))
            if pk is not None:
                if isinstance(pk, TableKey):
                    self.delete(pk.partition, pk.sort)  # pragma: no cover
                else:
                    self.delete(pk)
        self._manifest = {
            os.path.basename(p): self._file_signature(p)
            for p in self._iter_json_files()
        }
        self._last_dir_mtime = self._dir_mtime()
        self._last_check_time = time.time()
        summary["reread"] = reread
        self.last_change_summary = summary
        self._fire_hooks(self.on_refresh, summary)
        self._last_is_stale = False
        return summary

    def warm(self) -> None:  # pragma: no cover
        """Proactively refresh regardless of staleness."""
        if self.directory is None:
            return
        self.refresh()

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
            self._insert_record_from_load(record)
            self._manifest[name] = self._file_signature(path)
        self._last_dir_mtime = self._dir_mtime()
        self._last_check_time = time.time()
        self._last_is_stale = False

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
        self._manifest[filename] = self._file_signature(path)
        self._last_dir_mtime = self._dir_mtime()

    def _delete_record_from_disk(self, pk: Any) -> None:
        self._validate_pk_for_path(pk)
        filename = self._filename_for_pk(pk)
        path = os.path.join(self.directory, filename)
        if os.path.exists(path):
            os.remove(path)
            self._manifest.pop(filename, None)
            self._last_dir_mtime = self._dir_mtime()

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

    def _iter_json_files(self) -> Iterable[str]:  # pragma: no cover
        if self.directory is None:
            return []
        try:
            names = os.listdir(self.directory)
        except FileNotFoundError:
            return []
        return (
            os.path.join(self.directory, name)
            for name in names
            if name.endswith(".json")
        )

    def _dir_mtime(self) -> float:  # pragma: no cover
        if self.directory is None:
            return 0.0
        try:
            return os.path.getmtime(self.directory)
        except FileNotFoundError:
            return 0.0

    def _file_signature(self, path: str) -> tuple[int, int]:
        try:
            stat = os.stat(path)
        except FileNotFoundError:
            return 0, 0
        return stat.st_mtime_ns, stat.st_size

    def _pk_from_filename(self, filename: str) -> Optional[Any]:
        name = filename.replace(".json", "")
        if "__" in name:
            partition, sort = name.split("__", 1)  # pragma: no cover
            return TableKey(partition, sort)  # pragma: no cover
        return name  # pragma: no cover

    def _compute_changes(
        self,
    ) -> tuple[dict[str, int], set[str], set[str], set[str]]:
        if self.directory is None:
            return (
                {"added": 0, "modified": 0, "deleted": 0, "reread": 0},
                set(),
                set(),
                set(),
            )  # pragma: no cover
        current_files = {
            os.path.basename(p): self._file_signature(p)
            for p in self._iter_json_files()
        }
        previous = self._manifest
        added = {
            os.path.join(self.directory, name)
            for name in current_files
            if name not in previous
        }
        deleted = {
            os.path.join(self.directory, name)
            for name in previous
            if name not in current_files
        }
        modified = {
            os.path.join(self.directory, name)
            for name, mtime in current_files.items()
            if name in previous and previous[name] != mtime
        }
        summary = {
            "added": len(added),
            "modified": len(modified),
            "deleted": len(deleted),
            "reread": 0,
        }
        return summary, added, modified, deleted

    def _read_record_file(
        self, path: str
    ) -> Optional[dict[str, Any]]:  # pragma: no cover
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, json.JSONDecodeError):
            self.refresh_errors.append(path)
            return None

    def _maybe_refresh_before_query(self) -> None:
        if self.directory is None or not self.auto_refresh:
            return
        if self.is_stale():
            self.refresh()

    def _register_association(self, name: str, definition: AssociationDef) -> None:
        if name not in self.associations:
            self.associations.append(name)
        self.association_defs[name] = definition

    def _fire_hooks(
        self, hooks: list[Callable[[dict[str, Any]], None]], record: dict[str, Any]
    ) -> None:
        for hook in hooks:
            try:
                hook(record)
            except Exception as exc:
                self.hook_errors.append(str(exc))
