import os

from adapta.security.clients import AwsClient
from adapta.security.clients.aws import ExplicitAwsCredentials
from adapta.storage.blob.s3_storage_client import S3StorageClient

from microsoft_synapse_batch_collector.models.run_config import RunConfig
from microsoft_synapse_batch_collector.processing.azure_cleanup import remove_batch
from microsoft_synapse_batch_collector.processing.s3_transfer import upload_batches
from microsoft_synapse_batch_collector.run_setup import setup_args, create_synapse_client, setup_logger


def main(config: RunConfig):
    synapse_client = create_synapse_client(config.synapse_source_path)
    logger = setup_logger()
    for synapse_batch in upload_batches(
        source=config.synapse_source_path,
        bucket=config.upload_bucket_name,
        prefix=config.prefix,
        threshold=config.age_threshold,
        logger=logger,
        source_client=synapse_client,
        target_client=S3StorageClient.create(
            auth=AwsClient(
                credentials=ExplicitAwsCredentials(
                    access_key=os.environ["S3_DESTINATION__ACCESS_KEY"],
                    access_key_id=os.environ["S3_DESTINATION__ACCESS_KEY_ID"],
                    region=os.environ["S3_DESTINATION__REGION"],
                    endpoint=os.environ["S3_DESTINATION__ENDPOINT"],
                ),
                allow_http=os.environ["S3_DESTINATION__ALLOW_HTTP"] == "1",
            )
        ),
    ):
        remove_batch(batch=synapse_batch, client=synapse_client, logger=logger, dry_run=config.delete_processed)


if __name__ == "__main__":
    assert os.getenv(
        "AZURE_STORAGE_ACCOUNT_KEY"
    ), "Account key must be provided in order to access Synapse batches in the storage account"

    parser = setup_args()
    main(RunConfig.from_input_args(parser.parse_args()))
