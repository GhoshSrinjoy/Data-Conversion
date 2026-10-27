"""Exception hierarchy.  Everything dataconv raises on purpose derives from DataconvError."""

from __future__ import annotations


class DataconvError(Exception):
    """Base class for all dataconv errors."""


class MissingDependencyError(DataconvError, ImportError):
    """An optional package is needed for the requested operation."""


class FileError(DataconvError, OSError):
    """A file could not be read or written."""


class FormatError(DataconvError, ValueError):
    """A file format is unknown, unsupported, or does not match its contents."""


class SchemaError(DataconvError, ValueError):
    """A schema specification is invalid or cannot be applied."""


class DataError(DataconvError, ValueError):
    """Input data is malformed and the configured policy is to fail."""


class IntegrityError(DataconvError):
    """Stored checksums do not match the data (corruption or truncation)."""


class OutputExistsError(DataconvError, FileExistsError):
    """The destination exists and overwriting was not requested."""


class OptionsError(DataconvError, ValueError):
    """Invalid or conflicting options."""
