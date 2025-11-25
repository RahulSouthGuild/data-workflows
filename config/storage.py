"""
Azure Blob Storage configuration and client management.
"""

import logging
from typing import Optional
from azure.storage.blob import BlobServiceClient, ContainerClient
from config.settings import AZURE_CONFIG

logger = logging.getLogger(__name__)


class AzureStorageManager:
    """Manages Azure Blob Storage connections."""

    _blob_service_client: Optional[BlobServiceClient] = None
    _container_client: Optional[ContainerClient] = None

    @classmethod
    def get_blob_service_client(cls) -> BlobServiceClient:
        """Get or create Azure Blob Service Client."""
        if cls._blob_service_client is None:
            cls._blob_service_client = BlobServiceClient.from_connection_string(
                AZURE_CONFIG["connection_string"]
            )
            logger.info("Azure Blob Service Client initialized")
        return cls._blob_service_client

    @classmethod
    def get_container_client(cls, container_name: Optional[str] = None) -> ContainerClient:
        """Get or create Azure Container Client."""
        container = container_name or AZURE_CONFIG["container_name"]

        if cls._container_client is None or cls._container_client.container_name != container:
            blob_service_client = cls.get_blob_service_client()
            cls._container_client = blob_service_client.get_container_client(container)
            logger.info(f"Azure Container Client initialized: {container}")

        return cls._container_client

    @classmethod
    def close(cls) -> None:
        """Close Azure clients."""
        if cls._blob_service_client is not None:
            cls._blob_service_client.close()
            cls._blob_service_client = None
            cls._container_client = None
            logger.info("Azure Storage clients closed")


# Convenience functions
def get_blob_service_client() -> BlobServiceClient:
    """Get Azure Blob Service Client."""
    return AzureStorageManager.get_blob_service_client()


def get_container_client(container_name: Optional[str] = None) -> ContainerClient:
    """Get Azure Container Client."""
    return AzureStorageManager.get_container_client(container_name)
