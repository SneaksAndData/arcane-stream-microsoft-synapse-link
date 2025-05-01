from adapta.storage.models import AdlsGen2Path

from microsoft_synapse_batch_collector.main import main as collector
from microsoft_synapse_batch_collector.models.run_config import RunConfig


def test_batch_collection():
    collector(
        config=RunConfig(
            upload_bucket_name="synapse-archive",
            prefix="dev-env",
            age_threshold=7,
            synapse_source_path=AdlsGen2Path.from_hdfs_path("abfss://archive-test@devstoreaccount1.dfs.core.windows.net"),
            delete_processed=True
        )
    )
