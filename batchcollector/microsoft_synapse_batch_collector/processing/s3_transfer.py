import os
import shutil
from datetime import datetime, timedelta
from typing import Iterator

from adapta.logs import LoggerInterface
from adapta.metrics import MetricsProvider
from adapta.storage.blob.azure_storage_client import AzureStorageClient
from adapta.storage.blob.s3_storage_client import S3StorageClient
from adapta.storage.models import AdlsGen2Path, S3Path
from adapta.utils import operation_time

from microsoft_synapse_batch_collector.models.uploaded_batch import UploadedBatch
from microsoft_synapse_batch_collector.processing.synapse import filter_batches


def transform_path(azure_path: AdlsGen2Path, bucket: str, prefix: str) -> S3Path:
    """
    Transforms Synapse Link location into S3 prefix
    """
    segments = azure_path.path.split("/")
    return S3Path(
        bucket=bucket,
        path="/".join([prefix, segments[1], f"{segments[0].replace('.', '-')}_{segments[2]}"]),
    )


def upload_batches(
    source: AdlsGen2Path,
    bucket: str,
    prefix: str,
    threshold: int,
    logger: LoggerInterface,
    source_client: AzureStorageClient,
    target_client: S3StorageClient,
    metrics: MetricsProvider,
) -> Iterator[UploadedBatch]:
    """
    Process all files for the specified region, starting from dag_run_date
    """
    logger.info("Running with source: {source}, target: {bucket}/{prefix}", source=source, bucket=bucket, prefix=prefix)
    start_from_batch = (datetime.utcnow() - timedelta(days=threshold)).strftime("%Y-%m-%dT%H.%M.%SZ")

    for synapse_prefix in filter_batches(
        storage_client=source_client,
        base_path=source,
        start_from=start_from_batch,
        logger=logger,
    ):
        logger.info(
            "Batch {batch_folder} is older than {threshold} days, archiving",
            batch_folder=synapse_prefix,
            threshold=threshold,
        )
        associated_blobs = []
        for synapse_blob in source_client.list_blobs(
            blob_path=synapse_prefix, filter_predicate=lambda blob: blob.name.endswith(".csv")
        ):
            with operation_time() as ot:
                local_dirs = "/".join(["/tmp"] + synapse_blob.path.split("/")[:-1])
                target_path = transform_path(synapse_blob, bucket, prefix)

                logger.info(
                    "Copying {synapse_file} to {target_path}", synapse_file=synapse_blob.path, target_path=target_path
                )
                os.makedirs(local_dirs, exist_ok=True)

                source_client.download_blobs(synapse_blob, "/tmp")
                target_client.upload_blob(f"/tmp/{synapse_blob.path}", target_path, doze_period_ms=0)
                os.remove(f"/tmp/{synapse_blob.path}")

                logger.info(
                    "Successfully copied {synapse_file} to {target_path}",
                    synapse_file=synapse_blob.path,
                    target_path=target_path,
                )
                associated_blobs.append(synapse_blob)

            metrics.gauge("batch.copy_duration", ot.elapsed / 1e9, tags={"file": synapse_blob.path})

        logger.info("Finished archiving batch {batch}", batch=synapse_prefix.path)
        # include model.json for the batch
        associated_blobs.append(AdlsGen2Path.from_hdfs_path(f"{synapse_prefix.to_hdfs_path().rstrip('/')}/model.json"))

        yield UploadedBatch(
            source_path=synapse_prefix,
            blobs=associated_blobs,
        )
