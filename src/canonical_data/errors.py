"""Fail-closed pipeline exceptions."""


class PipelineError(Exception):
    """Base error for rejected pipeline work."""


class SourceError(PipelineError):
    """A source is malformed, corrupt, substituted, or exceeds bounds."""


class IdentityError(PipelineError):
    """Official market identity or resolution evidence is invalid."""


class ReconstructionError(PipelineError):
    """A book stream cannot be reconstructed without ambiguity."""


class ConflictError(PipelineError):
    """An immutable identity already exists with different content."""


class ResourceLimitError(PipelineError):
    """A configured disk, memory, transfer, or asset bound was exceeded."""
