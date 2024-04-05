"""
https://peps.python.org/pep-0249/#exceptions
"""
from pydantic import ValidationError

ORMValidationError = ValidationError


class ORMError(ValueError):
    """Exception raised for errors that are related to the database."""


class PrimaryKeyMissingError(ORMError):
    ...


class PrimaryKeyModifyError(ORMError):
    ...


class FieldNotFoundError(ORMError):
    ...


class FieldAttributeConflictError(ORMError):
    ...


class AutoIncrementFieldExists(ORMError):
    ...


class IntegrityError(ORMError):
    """Exception raised when the relational integrity of the database is affected."""


class NotSupportedError(ORMError):
    """Exception raised in case a method or database API was used which is not supported by the database"""


class ForeignKeyValidationError(ORMError):
    ...


class PredicateEncodeError(ORMError):
    ...


class OperatorUnregisteredError(ORMError):
    ...
