from datetime import datetime

from adapta.storage.models import AdlsGen2Path

from microsoft_synapse_batch_collector.main import main as collector
from microsoft_synapse_batch_collector.models.run_config import RunConfig
from microsoft_synapse_batch_collector.processing.synapse import filter_batches
from microsoft_synapse_batch_collector.run_setup import create_synapse_client, setup_logger


def test_batch_collection():
    source_path = AdlsGen2Path.from_hdfs_path("abfss://synapse-archiving-test@devstoreaccount1.dfs.core.windows.net")
    collector(
        config=RunConfig(
            upload_bucket_name="synapse-archive",
            prefix="dev",
            age_threshold=1,
            synapse_source_path=source_path,
            delete_processed=True,
        )
    )

    remaining = list(
        filter_batches(
            storage_client=create_synapse_client(source_path),
            base_path=source_path,
            start_from=datetime.utcnow().strftime("%Y-%m-%dT%H.%M.%SZ"),
            logger=setup_logger(),
        )
    )

    assert len(remaining) == 23
