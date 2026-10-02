"""Virtuus error classes and exception hierarchy."""

from __future__ import annotations


class VirtuusError(Exception):
    """
    Base exception class for all Virtuus errors.

    :param message: The error message.
    :type message: str
    """

    def __init__(self, message: str) -> None:
        """
        Initialize a VirtuusError.

        :param message: The error message.
        :type message: str
        """
        self.message = message
        super().__init__(message)


class NotFoundError(VirtuusError):
    """
    Raised when a record or resource was not found.

    :param message: The error message.
    :type message: str
    """

    pass


class ConditionalCheckFailedError(VirtuusError):
    """
    Raised when a conditional check failed during an update operation.

    :param message: The error message.
    :type message: str
    """

    pass


class ValidationError(VirtuusError, ValueError):
    """
    Raised when a validation error occurs.

    :param message: The error message.
    :type message: str
    """

    pass


class UnknownTableError(VirtuusError, KeyError):
    """
    Raised when an unknown table is referenced.

    :param name: The name of the table.
    :type name: str
    """

    pass


class UnknownIndexError(VirtuusError, KeyError):
    """
    Raised when an unknown index is referenced.

    :param table: The name of the table.
    :type table: str
    :param name: The name of the index.
    :type name: str
    """

    pass


class InvalidTokenError(VirtuusError):
    """
    Raised when an invalid token is provided.

    :param reason: The reason the token is invalid.
    :type reason: str
    """

    pass


class IoError(VirtuusError, OSError):
    """
    Raised when an IO error occurs.

    :param path: The path involved in the error.
    :type path: str
    :param message: The error message.
    :type message: str
    """

    def __init__(self, path: str, message: str) -> None:
        """
        Initialize an IoError.

        :param path: The path involved in the error.
        :type path: str
        :param message: The error message.
        :type message: str
        """
        self.path = path
        self.message = message
        super().__init__(f"Io: {path}: {message}")


class ParseError(VirtuusError, ValueError):
    """
    Raised when a parse error occurs.

    :param path: The path of the file being parsed.
    :type path: str
    :param message: The error message.
    :type message: str
    """

    def __init__(self, path: str, message: str) -> None:
        """
        Initialize a ParseError.

        :param path: The path of the file being parsed.
        :type path: str
        :param message: The error message.
        :type message: str
        """
        self.path = path
        self.message = message
        super().__init__(f"Parse: {path}: {message}")


class LockedError(VirtuusError):
    """
    Raised when a database lock could not be acquired.

    :param message: Description of the lock.
    :type message: str
    """

    pass
