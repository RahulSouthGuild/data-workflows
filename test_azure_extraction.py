#!/usr/bin/env python3
"""
Test Azure Extraction for a Tenant

Simple script to test if we can download files from Azure for Pidilite tenant
"""

import asyncio
import sys
from pathlib import Path

# Add project root
sys.path.append(str(Path(__file__).parent))

from orchestration.tenant_manager import TenantManager
from azure.storage.blob.aio import BlobServiceClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_azure_extraction(tenant_slug: str):
    """Test Azure blob extraction for a tenant"""

    logger.info("="*80)
    logger.info(f"TESTING AZURE EXTRACTION FOR: {tenant_slug.upper()}")
    logger.info("="*80)

    # Load tenant config
    tenant_manager = TenantManager(Path("configs"))
    tenant_config = tenant_manager.get_tenant_by_slug(tenant_slug)

    if not tenant_config:
        logger.error(f"Tenant not found: {tenant_slug}")
        return False

    logger.info(f"\n✓ Tenant: {tenant_config.tenant_name}")
    logger.info(f"  - Azure Container: {tenant_config.azure_container}")
    logger.info(f"  - Connection String: {'*' * 20}...{tenant_config.azure_connection_string[-20:]}")

    # Test Azure connection
    logger.info("\n[Step 1] Testing Azure connection...")

    try:
        async with BlobServiceClient.from_connection_string(
            tenant_config.azure_connection_string
        ) as blob_service_client:

            # Get container client
            container_client = blob_service_client.get_container_client(
                tenant_config.azure_container
            )

            # List blobs
            logger.info(f"\n[Step 2] Listing blobs in container: {tenant_config.azure_container}")

            blob_count = 0
            sample_blobs = []

            async for blob in container_client.list_blobs(name_starts_with="Incremental/"):
                blob_count += 1
                if blob_count <= 10:
                    sample_blobs.append(blob.name)

                if blob_count >= 100:  # Limit for testing
                    break

            logger.info(f"\n✓ Found {blob_count}+ blobs")
            logger.info(f"\nSample blobs:")
            for blob_name in sample_blobs[:10]:
                logger.info(f"  - {blob_name}")

            # Try downloading one blob
            if sample_blobs:
                test_blob = sample_blobs[0]
                logger.info(f"\n[Step 3] Testing download of: {test_blob}")

                blob_client = blob_service_client.get_blob_client(
                    container=tenant_config.azure_container,
                    blob=test_blob
                )

                download_stream = await blob_client.download_blob()
                data = await download_stream.readall()

                logger.info(f"✓ Downloaded {len(data)} bytes")

                # Check if it's a zip file
                if test_blob.endswith('.zip'):
                    logger.info(f"  - File is ZIP format")
                elif test_blob.endswith('.gz'):
                    logger.info(f"  - File is GZIP format")
                elif test_blob.endswith('.csv'):
                    logger.info(f"  - File is CSV format")

                logger.info("\n" + "="*80)
                logger.info("✅ AZURE EXTRACTION TEST SUCCESSFUL")
                logger.info("="*80)
                logger.info(f"Container: {tenant_config.azure_container}")
                logger.info(f"Blobs found: {blob_count}+")
                logger.info(f"Sample download: {len(data)} bytes")
                logger.info("="*80)

                return True

    except Exception as e:
        logger.error(f"\n❌ Azure extraction failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Test Azure extraction for a tenant')
    parser.add_argument('--tenant-slug', default='pidilite', help='Tenant slug')
    args = parser.parse_args()

    success = asyncio.run(test_azure_extraction(args.tenant_slug))

    if not success:
        sys.exit(1)
