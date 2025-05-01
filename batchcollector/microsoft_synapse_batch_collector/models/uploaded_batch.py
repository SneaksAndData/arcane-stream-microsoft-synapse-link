"""
 Batch models.
"""
from dataclasses import dataclass

from adapta.storage.models import AdlsGen2Path


@dataclass
class UploadedBatch:
    """
    Summary for a processed batch.
    """

    source_path: AdlsGen2Path
    blobs: list[AdlsGen2Path]
