"""
 App setup.
"""
import os
import sys
from argparse import ArgumentParser
from logging import StreamHandler

from adapta.logs import LoggerInterface, SemanticLogger
from adapta.logs.handlers.datadog_api_handler import DataDogApiHandler
from adapta.logs.models import LogLevel
from adapta.security.clients import AzureClient
from adapta.storage.blob.azure_storage_client import AzureStorageClient
from adapta.storage.models import AdlsGen2Path


def setup_args(parser: ArgumentParser | None = None) -> ArgumentParser:
    """
    Adds command line args to the executable script

    :param parser: Existing argument parser, if any.
    :return: The existing argument parser (if provided) with additional arguments added.
    """
    if parser is None:
        parser = ArgumentParser()

    parser.add_argument("--upload-to-bucket", required=True, type=str, help="S3 bucket to upload batches to")
    parser.add_argument("--upload-to-prefix", required=True, type=str, help="Prefix to place batches under")
    parser.add_argument(
        "--batch-older-than-days",
        required=True,
        type=int,
        help="Number of days the batch must be older than to be eligible for collection",
    )
    parser.add_argument(
        "--synapse-source-path",
        required=True,
        type=str,
        help="Path to Synapse Incremental CSV batches, in a form of abfss://container@account.dfs.core.windows.net",
    )
    parser.add_argument(
        "--remove-processed-batches",
        dest="remove_processed_batches",
        required=False,
        action="store_true",
        help="Whether to remove processed batches from Azure after they were successfully copied to S3. Defaults to True",
    )
    parser.set_defaults(remove_processed_batches=True)

    return parser


def create_synapse_client(source_path: AdlsGen2Path) -> AzureStorageClient:
    """
    Configures HTTP session and a storage client.
    """
    if "AZURE_STORAGE_ACCOUNT_NAME" not in os.environ:
        os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = source_path.account

    return AzureStorageClient(base_client=AzureClient(), path=source_path, implicit_login=False)


def setup_logger() -> LoggerInterface:
    """
     Configures logger interface for the application.

    :return:
    """
    log_handlers = [StreamHandler(sys.stdout)]

    if "PROTEUS__DD_API_KEY" in os.environ and "PROTEUS__DD_APP_KEY" in os.environ and "PROTEUS__DD_SITE" in os.environ:
        log_handlers.append(DataDogApiHandler(buffer_size=1))

    return SemanticLogger().add_log_source(
        log_source_name="synapse-batch-collector",
        min_log_level=LogLevel.INFO,
        log_handlers=log_handlers,
        is_default=True,
    )
