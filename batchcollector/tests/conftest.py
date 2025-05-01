import os

import pytest


@pytest.fixture(scope="session", autouse=True)
def set_env():
    os.environ |= {
        "ADAPTA__AZURE_STORAGE_BLOB_ENDPOINT": "http://localhost:10001/devstoreaccount1",
        "ADAPTA__AZURE_STORAGE_DEFAULT_PROTOCOL": "http",
        "AZURE_STORAGE_ACCOUNT_NAME": "devstoreaccount1",
        "AZURE_STORAGE_ACCOUNT_KEY": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        "S3_DESTINATION__ACCESS_KEY": "minioadmin",
        "S3_DESTINATION__ACCESS_KEY_ID": "minioadmin",
        "S3_DESTINATION__REGION": "us-east-1",
        "S3_DESTINATION__ENDPOINT": "http://localhost:9000",
        "S3_DESTINATION__ALLOW_HTTP": "1",
    }
