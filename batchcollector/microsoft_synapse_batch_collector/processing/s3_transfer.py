from typing import Iterator

from adapta.storage.models import AdlsGen2Path, S3Path


def transform_path(azure_path: AdlsGen2Path, bucket: str, prefix: str) -> S3Path:
    """
    Transforms Synapse Link location into S3 prefix
    """
    segments = azure_path.path.split("/")
    return S3Path(
        bucket=bucket,
        path="/".join([prefix, segments[1], f"{segments[0].replace('.', '-')}_{segments[2]}"]),
    )


def upload_batches(region: str, region_source_path: str, bucket: str, dag_run_date: str) -> Iterator[UploadedBatch]:
    """
    Process all files for the specified region, starting from dag_run_date
    """
    print(f"Running with source: {region_source_path}, target: {bucket}")
    source_path = AdlsGen2Path.from_hdfs_path(region_source_path)
    source_client = configure_for_region(source_path)
    target_client = get_storage_client(f"s3a://{bucket}")

    for synapse_file in filter_prefixes_for_run(
        storage_client=source_client,
        base_path=source_path,
        start_date=datetime.strptime(dag_run_date, "%Y-%m-%dT%H:%M:%S+00:00"),
        excluded_segments=EXCLUDED_SEGMENTS,
    ):
        local_dirs = "/".join(["/tmp"] + synapse_file.path.split("/")[:-1])
        target_path = transform_path(synapse_file, bucket, region)

        print(f"Copying {synapse_file.path} to {target_path}")
        os.makedirs(local_dirs, exist_ok=True)

        source_client.download_blobs(synapse_file, "/tmp")
        target_client.upload_blob(f"/tmp/{synapse_file.path}", target_path, doze_period_ms=0)