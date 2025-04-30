import os

from microsoft_synapse_batch_collector.models.run_config import RunConfig
from microsoft_synapse_batch_collector.run_setup import setup_args


def main(config: RunConfig):
    pass


if __name__ == "__main__":
    assert os.getenv(
        "AZURE_STORAGE_ACCOUNT_KEY"
    ), "Account key must be provided in order to access Synapse batches in the storage account"

    parser = setup_args()
    main(RunConfig.from_input_args(parser.parse_args()))
