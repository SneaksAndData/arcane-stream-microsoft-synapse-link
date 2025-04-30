from typing import Iterator

from adapta.logs import LoggerInterface
from adapta.storage.blob.azure_storage_client import AzureStorageClient
from adapta.storage.models import AdlsGen2Path, DataPath

EXCLUDED_SEGMENTS = ["OptionsetMetadata", "Microsoft.Athena.TrickleFeedService", "Changelog", "EntitySyncFailure"]


def filter_batches(
    storage_client: AzureStorageClient,
    base_path: AdlsGen2Path,
    start_from: str,
    logger: LoggerInterface,
    excluded_segments: list[str] | None = None,
) -> Iterator[AdlsGen2Path]:
    """
      Filters prefixes in Synapse Linked storage account starting from `start_date`

    :param storage_client: Storage client to read the CSV files
    :param base_path: Root location to list from
    :param start_from: Batch to start yielding from
    :param excluded_segments: Prefixes to exclude, if they contain these segments
    :param logger: Logger to use
    """

    def is_valid_path(adls_path: DataPath) -> bool:
        for excluded_segment in excluded_segments:
            if excluded_segment in adls_path.path:
                logger.debug("Skipping {path}: filtered out by exclusion ruleset", path=adls_path.path)
                return False

            if adls_path.path < start_from:
                return False

        return True

    if excluded_segments is None:
        excluded_segments = EXCLUDED_SEGMENTS

    # generate prefixes to look for files in a format yyyy-MM-ddTHH
    logger.info(
        "Listing prefixes starting from {start}, source path: {path}", start=start_from, path=base_path.to_hdfs_path()
    )
    for prefix in storage_client.list_matching_prefixes(
        AdlsGen2Path.from_hdfs_path(base_path.to_hdfs_path()),
    ):
        if is_valid_path(prefix):
            yield prefix
