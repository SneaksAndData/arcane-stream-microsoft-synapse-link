from argparse import ArgumentParser


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
