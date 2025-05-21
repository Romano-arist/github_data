import boto3
from botocore.client import Config
from botocore import UNSIGNED


def get_s3_client(host: str = "localhost", port: int = 9000) -> boto3.client:
    """
    Create an S3 client for MinIO.
    """
    client =  boto3.client(
        's3',
        endpoint_url=f'http://{host}:{port}',
        config=Config(signature_version=UNSIGNED),
    )
    return client
