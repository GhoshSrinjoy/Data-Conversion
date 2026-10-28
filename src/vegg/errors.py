"""Exception hierarchy.  Everything vegg raises on purpose derives from VeggError."""

from __future__ import annotations


class VeggError(Exception):
    """Base class for all vegg errors."""


class MissingDependencyError(VeggError, ImportError):
    """An optional package is needed for the requested operation."""


class FileError(VeggError, OSError):
    """A file could not be read or written."""


class FormatError(VeggError, ValueError):
    """A file format is unknown, unsupported, or does not match its contents."""


class SchemaError(VeggError, ValueError):
    """A schema specification is invalid or cannot be applied."""


class DataError(VeggError, ValueError):
    """Input data is malformed and the configured policy is to fail."""


class IntegrityError(VeggError):
    """Stored checksums do not match the data (corruption or truncation)."""


class OutputExistsError(VeggError, FileExistsError):
    """The destination exists and overwriting was not requested."""


class OptionsError(VeggError, ValueError):
    """Invalid or conflicting options."""
