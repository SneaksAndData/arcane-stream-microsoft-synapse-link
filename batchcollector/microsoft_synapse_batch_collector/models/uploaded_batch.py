from dataclasses import dataclass

from adapta.storage.models import AdlsGen2Path, S3Path


@dataclass
class UploadedBatch:
    source_path: AdlsGen2Path
    target_path: S3Path
