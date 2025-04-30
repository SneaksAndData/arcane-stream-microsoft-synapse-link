from adapta.logs import LoggerInterface
from adapta.storage.blob.azure_storage_client import AzureStorageClient

from microsoft_synapse_batch_collector.models.uploaded_batch import UploadedBatch


def remove_batch(batch: UploadedBatch, client: AzureStorageClient, logger: LoggerInterface) -> None:
    logger.info("Removing archived Synapse batch {batch}", batch=batch.source_path)
    for blob_to_delete in batch.blobs:
        logger.info("Deleting blob: {blob}", blob=blob_to_delete.path)
        client.delete_blob(blob_to_delete)
