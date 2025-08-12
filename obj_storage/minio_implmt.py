# obj_storage/minio_implmt.py
import os
from io import BytesIO
from typing import Optional

from minio import Minio
from minio.error import S3Error

from .base import AbstractStorage


class MinIOStorage(AbstractStorage):
    def __init__(self) -> None:
        self.client = Minio(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            secure=False,
        )
        self.bucket = os.getenv("MINIO_BUCKET", "uploads")


    async def upload(
        self,
        *,
        bucket: str,
        key: str,
        data: bytes,
        length: int,
        content_type: Optional[str] = None,
    ) -> str:
        try:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)

            self.client.put_object(
                bucket, key, BytesIO(data), length=length, content_type=content_type
            )
            return f"/{bucket}/{key}"
        except S3Error as exc:
            raise RuntimeError(str(exc)) from exc

    async def health(self) -> bool:
        try:
            return self.client.bucket_exists(self.bucket)
        except Exception:
            return False