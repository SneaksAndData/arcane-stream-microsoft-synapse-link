from adapta.logs import LoggerInterface
from adapta.metrics import MetricsProvider
from adapta.storage.blob.azure_storage_client import AzureStorageClient

from microsoft_synapse_batch_collector.models.uploaded_batch import UploadedBatch


def remove_batch(
    batch: UploadedBatch, client: AzureStorageClient, dry_run: bool, logger: LoggerInterface, metrics: MetricsProvider
) -> None:
    """
     Deletes all blobs that are part of the provided batch
    :param batch:
    :param client:
    :param dry_run:
    :param logger:
    :param metrics:
    :return:
    """
    logger.info("Removing archived Synapse batch {batch}", batch=batch.source_path)
    metrics.gauge(
        metric_name="batch.total_blobs",
        metric_value=len(batch.blobs) - 1,
        tags={
            "batch": batch.source_path.path.split("/")[0],
        },
    )
    for blob_to_delete in batch.blobs:
        if not dry_run:
            logger.info("Deleting blob: {blob}", blob=blob_to_delete.path)
            client.delete_leased_blob(blob_to_delete)
        else:
            logger.info(
                "Blob: {blob} qualifies for deletion. Skipping due to dry_run set to True", blob=blob_to_delete.path
            )
