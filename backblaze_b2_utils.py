import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from file_utils import printToConsole
import re

def get_b2_resource(endpoint, keyID, applicationKey):
    """
    Create and return a Boto3 S3 resource configured for Backblaze B2.

    This resource is used to interact with Backblaze B2 via the S3 compatible API.

    Args:
        endpoint (str): The Backblaze B2 endpoint URL.
        keyID (str): The application key ID for authentication.
        applicationKey (str): The application key for authentication.

    Returns:
        boto3.resources.base.ServiceResource: A Boto3 S3 resource instance configured for B2.

    Note:
        The resource is configured with signature version s3v4 which is required by B2.
    """
    b2 = boto3.resource(
        service_name="s3",
        endpoint_url=endpoint,
        aws_access_key_id=keyID,
        aws_secret_access_key=applicationKey,
        config=Config(
            signature_version="s3v4",
        ),
    )
    return b2

def upload_file(bucketName, filePath, fileName, b2, b2path=None):
    """
    Upload a file to a Backblaze B2 bucket.

    Args:
        bucketName (str): The name of the B2 bucket.
        filePath (str): The local path to the file to upload.
        fileName (str): The name of the file (used if b2path is not provided).
        b2 (boto3.resources.base.ServiceResource): The Boto3 S3 resource for B2.
        b2path (str, optional): The remote path (key) in the bucket. If not provided, uses fileName.

    Returns:
        dict: The response from the upload operation.

    Raises:
        ClientError: If the upload fails due to B2/client issues.
        Exception: For other unexpected errors during upload.

    Note:
        The remote path is normalized to use forward slashes.
    """
    # filePath = directory + '/' + file
    remotePath = b2path
    if remotePath is None:
        remotePath = fileName
    else:
        remotePath = re.sub(r"\\", "/", remotePath)
    printToConsole("remotePath " + remotePath)
    try:
        response = b2.Bucket(bucketName).upload_file(filePath, remotePath)
    except ClientError as ce:
        raise
    return response

