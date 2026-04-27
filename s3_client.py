"""
S3 Connector — S3 client wrapper
----------------------------------
Provides a thin wrapper around boto3 to list and retrieve objects from
an S3-compatible storage bucket.

Usage:
    from s3_client import S3Client, S3ConnectorError

    client = S3Client(
        endpoint_url="https://s3.example.com",   # None for AWS S3
        access_key="AKIAIOSFODNN7EXAMPLE",
        secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        bucket="my-bucket",
    )
    objects = client.list_objects(prefix="reports/")
    data = client.get_object_bytes("reports/Q1.xlsx")
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import boto3
import botocore.exceptions

logger = logging.getLogger("s3connector.s3_client")


class S3ConnectorError(Exception):
    """Raised when an S3 operation fails after logging."""


@dataclass
class S3Object:
    """Metadata for a single S3 object."""

    key: str
    size: int
    last_modified: datetime


class S3Client:
    """Wrapper around a boto3 S3 client scoped to a single bucket.

    Args:
        endpoint_url: Full URL of the S3-compatible endpoint.
                      Pass None or an empty string to use AWS S3.
        access_key: AWS / S3-compatible access key ID.
        secret_key: AWS / S3-compatible secret access key.
        bucket: Name of the bucket to operate on.
        region: AWS region name (default: "us-east-1").
    """

    def __init__(
        self,
        endpoint_url: Optional[str],
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str = "us-east-1",
    ) -> None:
        """Initialise the boto3 session and S3 client."""
        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url or None,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
        logger.info(
            "S3Client initialised — bucket='%s', endpoint='%s'.",
            bucket,
            endpoint_url or "AWS default",
        )

    def list_objects(self, prefix: str = "") -> List[S3Object]:
        """List all objects in the bucket under the given prefix.

        Handles S3 pagination automatically and returns a flat list.

        Args:
            prefix: Key prefix (folder path) used to filter results.
                    An empty string returns all objects in the bucket.

        Returns:
            List of S3Object instances sorted by key name.

        Raises:
            S3ConnectorError: If the S3 API call fails.
        """
        logger.info(
            "Listing objects in bucket='%s' with prefix='%s'.",
            self._bucket,
            prefix,
        )
        objects: List[S3Object] = []
        paginator = self._client.get_paginator("list_objects_v2")

        try:
            pages = paginator.paginate(Bucket=self._bucket, Prefix=prefix)
            for page in pages:
                for item in page.get("Contents", []):
                    objects.append(
                        S3Object(
                            key=item["Key"],
                            size=item["Size"],
                            last_modified=item["LastModified"],
                        )
                    )
        except botocore.exceptions.ClientError as exc:
            logger.exception(
                "Failed to list objects in bucket='%s' with prefix='%s'.",
                self._bucket,
                prefix,
            )
            raise S3ConnectorError(
                f"Could not list objects: {exc.response['Error']['Message']}"
            ) from exc

        logger.info("Found %d object(s).", len(objects))
        return sorted(objects, key=lambda o: o.key)

    def get_object_bytes(self, key: str) -> bytes:
        """Download the full content of an S3 object as bytes.

        Args:
            key: The full S3 object key to download.

        Returns:
            Raw bytes of the object body.

        Raises:
            S3ConnectorError: If the object cannot be retrieved.
        """
        logger.info("Downloading object key='%s' from bucket='%s'.", key, self._bucket)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            data: bytes = response["Body"].read()
            logger.debug("Downloaded %d bytes for key='%s'.", len(data), key)
            return data
        except botocore.exceptions.ClientError as exc:
            logger.exception(
                "Failed to download key='%s' from bucket='%s'.", key, self._bucket
            )
            raise S3ConnectorError(
                f"Could not download '{key}': {exc.response['Error']['Message']}"
            ) from exc

    def check_connection(self) -> None:
        """Verify that the configured credentials and bucket are accessible.

        Performs a lightweight head_bucket call to validate credentials and
        bucket existence without listing any objects.

        Raises:
            S3ConnectorError: If the bucket is unreachable or credentials are invalid.
        """
        logger.info("Checking connection to bucket='%s'.", self._bucket)
        try:
            self._client.head_bucket(Bucket=self._bucket)
            logger.info("Connection check passed for bucket='%s'.", self._bucket)
        except botocore.exceptions.ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            logger.exception(
                "Connection check failed for bucket='%s' (code=%s).",
                self._bucket,
                error_code,
            )
            raise S3ConnectorError(
                f"Cannot reach bucket '{self._bucket}' (HTTP {error_code})."
            ) from exc
