"""Sort condition predicates for DynamoDB-style range key filtering."""

from __future__ import annotations

from typing import Any, Callable


def _coerce(value: Any) -> Any:
    """Coerce a value to int or float if it is a string that parses as one.

    Non-string values are returned unchanged.

    :param value: The value to coerce.
    :type value: Any
    :return: Numeric value if the input is a parseable string, otherwise unchanged.
    :rtype: Any
    """
    if not isinstance(value, str):
        return value
    try:
        return int(value)
    except (ValueError, TypeError):
        pass
    try:
        return float(value)
    except (ValueError, TypeError):
        pass
    return value


class Sort:
    """Factory for sort condition predicates.

    Each factory method returns a callable that accepts a single value and
    returns ``True`` if the value satisfies the condition.
    """

    @staticmethod
    def eq(value: Any) -> Callable[[Any], bool]:
        """Return a predicate that matches values equal to *value*.

        :param value: The comparison target.
        :type value: Any
        :return: Predicate callable.
        :rtype: Callable[[Any], bool]
        """
        coerced = _coerce(value)

        def predicate(input_value: Any) -> bool:
            if input_value is None:
                return False
            return _coerce(input_value) == coerced

        return predicate

    @staticmethod
    def ne(value: Any) -> Callable[[Any], bool]:
        """Return a predicate that matches values not equal to *value*.

        :param value: The comparison target.
        :type value: Any
        :return: Predicate callable.
        :rtype: Callable[[Any], bool]
        """
        coerced = _coerce(value)

        def predicate(input_value: Any) -> bool:
            if input_value is None:
                return False
            return _coerce(input_value) != coerced

        return predicate

    @staticmethod
    def lt(value: Any) -> Callable[[Any], bool]:
        """Return a predicate that matches values strictly less than *value*.

        :param value: The upper bound (exclusive).
        :type value: Any
        :return: Predicate callable.
        :rtype: Callable[[Any], bool]
        """
        coerced = _coerce(value)

        def predicate(input_value: Any) -> bool:
            if input_value is None:
                return False
            return _coerce(input_value) < coerced

        return predicate

    @staticmethod
    def lte(value: Any) -> Callable[[Any], bool]:
        """Return a predicate that matches values less than or equal to *value*.

        :param value: The upper bound (inclusive).
        :type value: Any
        :return: Predicate callable.
        :rtype: Callable[[Any], bool]
        """
        coerced = _coerce(value)

        def predicate(input_value: Any) -> bool:
            if input_value is None:
                return False
            return _coerce(input_value) <= coerced

        return predicate

    @staticmethod
    def gt(value: Any) -> Callable[[Any], bool]:
        """Return a predicate that matches values strictly greater than *value*.

        :param value: The lower bound (exclusive).
        :type value: Any
        :return: Predicate callable.
        :rtype: Callable[[Any], bool]
        """
        coerced = _coerce(value)

        def predicate(input_value: Any) -> bool:
            if input_value is None:
                return False
            return _coerce(input_value) > coerced

        return predicate

    @staticmethod
    def gte(value: Any) -> Callable[[Any], bool]:
        """Return a predicate that matches values greater than or equal to *value*.

        :param value: The lower bound (inclusive).
        :type value: Any
        :return: Predicate callable.
        :rtype: Callable[[Any], bool]
        """
        coerced = _coerce(value)

        def predicate(input_value: Any) -> bool:
            if input_value is None:
                return False
            return _coerce(input_value) >= coerced

        return predicate

    @staticmethod
    def between(low: Any, high: Any) -> Callable[[Any], bool]:
        """Return a predicate that matches values in [*low*, *high*] inclusive.

        :param low: The lower bound (inclusive).
        :type low: Any
        :param high: The upper bound (inclusive).
        :type high: Any
        :return: Predicate callable.
        :rtype: Callable[[Any], bool]
        """
        coerced_low = _coerce(low)
        coerced_high = _coerce(high)

        def predicate(input_value: Any) -> bool:
            if input_value is None:
                return False
            v = _coerce(input_value)
            return coerced_low <= v <= coerced_high

        return predicate

    @staticmethod
    def begins_with(prefix: str) -> Callable[[Any], bool]:
        """Return a predicate that matches string values beginning with *prefix*.

        :param prefix: The required prefix.
        :type prefix: str
        :return: Predicate callable.
        :rtype: Callable[[Any], bool]
        """

        def predicate(input_value: Any) -> bool:
            if input_value is None:
                return False
            return str(input_value).startswith(prefix)

        return predicate

    @staticmethod
    def contains(substring: str) -> Callable[[Any], bool]:
        """Return a predicate that matches string values containing *substring*.

        :param substring: The required substring.
        :type substring: str
        :return: Predicate callable.
        :rtype: Callable[[Any], bool]
        """

        def predicate(input_value: Any) -> bool:
            if input_value is None:
                return False
            return substring in str(input_value)

        return predicate
