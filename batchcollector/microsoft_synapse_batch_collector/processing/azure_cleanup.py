from adapta.logs import LoggerInterface
from adapta.storage.blob.azure_storage_client import AzureStorageClient

from microsoft_synapse_batch_collector.models.uploaded_batch import UploadedBatch


def remove_batch(batch: UploadedBatch, client: AzureStorageClient, dry_run: bool, logger: LoggerInterface) -> None:
    """
     Deletes all blobs that are part of the provided batch
    :param batch:
    :param client:
    :param dry_run:
    :param logger:
    :return:
    """
    logger.info("Removing archived Synapse batch {batch}", batch=batch.source_path)
    for blob_to_delete in batch.blobs:
        if not dry_run:
            logger.info("Deleting blob: {blob}", blob=blob_to_delete.path)
            client.delete_leased_blob(blob_to_delete)
        else:
            logger.info(
                "Blob: {blob} qualifies for deletion. Skipping due to dry_run set to True", blob=blob_to_delete.path
            )
