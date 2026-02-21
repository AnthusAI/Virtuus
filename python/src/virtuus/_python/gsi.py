"""Global Secondary Index implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class _GsiEntry:
    pk: str
    sort_value: Optional[Any]


class GSI:
    """Global Secondary Index with hash partition and optional range key.

    :param name: Name of the index.
    :type name: str
    :param partition_key: Field used as the partition key.
    :type partition_key: str
    :param sort_key: Optional field used as the sort (range) key.
    :type sort_key: str | None
    """

    def __init__(self, name: str, partition_key: str, sort_key: Optional[str] = None):
        self._name = name
        self._partition_key = partition_key
        self._sort_key = sort_key
        self._buckets: dict[Any, list[_GsiEntry]] = {}

    @property
    def name(self) -> str:
        """Return the index name.

        :return: Index name.
        :rtype: str
        """

        return self._name

    @property
    def partition_key(self) -> str:
        """Return the partition key field.

        :return: Partition key field name.
        :rtype: str
        """

        return self._partition_key

    @property
    def sort_key(self) -> Optional[str]:
        """Return the sort key field if present.

        :return: Sort key field name or None.
        :rtype: str | None
        """

        return self._sort_key

    def put(self, pk: str, record: dict[str, Any]) -> None:
        """Insert a record into the index.

        :param pk: Primary key of the record.
        :type pk: str
        :param record: Record data.
        :type record: dict[str, Any]
        :return: None
        :rtype: None
        """

        partition_value = record.get(self._partition_key)
        if partition_value is None:
            return
        sort_value = self._extract_sort_value(record)
        if self._sort_key is not None and sort_value is None:
            return
        bucket = self._buckets.setdefault(partition_value, [])
        bucket.append(_GsiEntry(pk=pk, sort_value=sort_value))

    def remove(self, pk: str, record: dict[str, Any]) -> None:
        """Remove a record from the index.

        :param pk: Primary key of the record.
        :type pk: str
        :param record: Record data.
        :type record: dict[str, Any]
        :return: None
        :rtype: None
        """

        partition_value = record.get(self._partition_key)
        if partition_value is None:
            return
        sort_value = self._extract_sort_value(record)
        if self._sort_key is not None and sort_value is None:
            return
        bucket = self._buckets.get(partition_value)
        if not bucket:
            return
        self._buckets[partition_value] = [
            entry
            for entry in bucket
            if not (entry.pk == pk and entry.sort_value == sort_value)
        ]
        if not self._buckets[partition_value]:
            self._buckets.pop(partition_value, None)

    def update(self, pk: str, old_record: dict[str, Any], new_record: dict[str, Any]) -> None:
        """Update an indexed record.

        :param pk: Primary key of the record.
        :type pk: str
        :param old_record: Previous record data.
        :type old_record: dict[str, Any]
        :param new_record: Updated record data.
        :type new_record: dict[str, Any]
        :return: None
        :rtype: None
        """

        self.remove(pk, old_record)
        self.put(pk, new_record)

    def query(
        self,
        partition_value: Any,
        sort_condition: Optional[Callable[[Any], bool]] = None,
        sort_direction: str = "asc",
    ) -> list[str]:
        """Query by partition key with optional sort condition and direction.

        :param partition_value: Partition key value to query.
        :type partition_value: Any
        :param sort_condition: Optional predicate over sort values.
        :type sort_condition: Callable[[Any], bool] | None
        :param sort_direction: "asc" or "desc".
        :type sort_direction: str
        :return: List of primary keys.
        :rtype: list[str]
        :raises ValueError: If sort_direction is not "asc" or "desc".
        """

        if sort_direction not in {"asc", "desc"}:
            raise ValueError("sort_direction must be 'asc' or 'desc'")
        bucket = list(self._buckets.get(partition_value, []))
        if sort_condition is not None:
            bucket = [
                entry
                for entry in bucket
                if entry.sort_value is not None and sort_condition(entry.sort_value)
            ]
        if self._sort_key is not None:
            bucket = sorted(bucket, key=lambda entry: _order_key(entry.sort_value))
        if sort_direction == "desc":
            bucket = list(reversed(bucket))
        return [entry.pk for entry in bucket]

    def _extract_sort_value(self, record: dict[str, Any]) -> Optional[Any]:
        if self._sort_key is None:
            return None
        return record.get(self._sort_key)


def _value_rank(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return 1
    if isinstance(value, (int, float)):
        return 2
    if isinstance(value, str):
        return 3
    if isinstance(value, list):
        return 4
    if isinstance(value, dict):
        return 5
    return 6


def _normalize_for_order(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float, str)):
        return value
    if isinstance(value, list):
        return tuple(_order_key(item) for item in value)
    if isinstance(value, dict):
        return tuple(sorted((str(k), _order_key(v)) for k, v in value.items()))
    return str(value)


def _order_key(value: Any) -> tuple[int, Any]:
    return _value_rank(value), _normalize_for_order(value)

