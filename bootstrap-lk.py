import requests

# boostrap
resp = requests.post(url='http://localhost:8181/management/v1/bootstrap', json={"accept-terms-of-use": True})
resp.raise_for_status()

# create warehouse
resp = requests.post(url='http://localhost:8181/management/v1/warehouse', json={
                                                                                 "warehouse-name": "demo",
                                                                                 "project-id": "00000000-0000-0000-0000-000000000000",
                                                                                 "storage-profile": {
                                                                                   "type": "s3",
                                                                                   "bucket": "tmp",
                                                                                   "key-prefix": "initial-warehouse",
                                                                                   "assume-role-arn": None,
                                                                                   "endpoint": "http://localhost:9000",
                                                                                   "region": "us-east-1",
                                                                                   "path-style-access": True,
                                                                                   "flavor": "minio",
                                                                                   "sts-enabled": False
                                                                                 },
                                                                                 "storage-credential": {
                                                                                   "type": "s3",
                                                                                   "credential-type": "access-key",
                                                                                   "aws-access-key-id": "minioadmin",
                                                                                   "aws-secret-access-key": "minioadmin"
                                                                                 }
                                                                               })

