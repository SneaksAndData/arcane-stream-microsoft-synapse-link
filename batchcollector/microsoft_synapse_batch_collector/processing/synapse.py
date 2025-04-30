from typing import Iterator

from adapta.logs import LoggerInterface
from adapta.storage.blob.azure_storage_client import AzureStorageClient
from adapta.storage.models import AdlsGen2Path, DataPath

EXCLUDED_SEGMENTS = ["OptionsetMetadata", "Microsoft.Athena.TrickleFeedService", "Changelog", "EntitySyncFailure"]


def filter_prefixes_for_run(
    storage_client: AzureStorageClient,
    base_path: AdlsGen2Path,
    start_from: str,
    excluded_segments: list[str],
    logger: LoggerInterface,
) -> Iterator[AdlsGen2Path]:
    """
      Filters prefixes in Synapse Linked storage account starting from `start_date`

    :param storage_client: Storage client to read the CSV files
    :param base_path: Root location to list from
    :param start_from: Batch to start yielding from
    :param excluded_segments: Prefixes to exclude, if they contain these segments
    """

    def is_valid_path(adls_path: DataPath) -> bool:
        for excluded_segment in excluded_segments:
            if excluded_segment in adls_path.path:
                return False

            if adls_path.path < start_from:
                return False

        return True

    # generate prefixes to look for files in a format yyyy-MM-ddTHH
    logger.info("Listing prefixes to process, source path: {path}", path=base_path.to_hdfs_path())
    for prefix in storage_client.list_matching_prefixes(
        AdlsGen2Path.from_hdfs_path(base_path.to_hdfs_path()),
    ):
        if is_valid_path(prefix):
            yield prefix
