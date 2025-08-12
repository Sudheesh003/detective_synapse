import os
from typing import Optional

from azure.storage.blob.aio import BlobServiceClient
from azure.core.exceptions import AzureError

from .base import AbstractStorage


class AzureBlobStorage(AbstractStorage):
    """
    Azure Blob Storage driver (async).
    """

    def __init__(self) -> None:
        # Accept either connection-string or account-url + credential
        conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
        credential = os.getenv("AZURE_STORAGE_CREDENTIAL")  # key or SAS

        if conn_str:
            self.client = BlobServiceClient.from_connection_string(conn_str)
        elif account_url and credential:
            self.client = BlobServiceClient(account_url=account_url, credential=credential)
        else:
            raise RuntimeError(
                "Need either AZURE_STORAGE_CONNECTION_STRING or "
                "AZURE_STORAGE_ACCOUNT_URL + AZURE_STORAGE_CREDENTIAL"
            )

        self.container = os.getenv("AZURE_CONTAINER_NAME", "uploads")

    async def upload(
        self,
        *,
        bucket: str,   # maps to Azure container
        key: str,      # maps to blob name
        data: bytes,
        length: int,
        content_type: Optional[str] = None,
    ) -> str:
        try:
            container_client = self.client.get_container_client(bucket)
            # create container if absent
            if not await container_client.exists():
                await container_client.create_container()

            blob_client = container_client.get_blob_client(key)
            await blob_client.upload_blob(
                data,
                length=length,
                overwrite=True,
                content_settings=None if content_type is None else {"content_type": content_type},
            )
            # stable HTTPS URL
            return blob_client.url
        except AzureError as exc:
            raise RuntimeError(str(exc)) from exc

    async def health(self) -> bool:
        try:
            async with self.client:
                container_client = self.client.get_container_client(self.container)
                await container_client.get_container_properties()
            return True
        except Exception:
            return False