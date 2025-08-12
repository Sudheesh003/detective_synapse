import os
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, File, UploadFile, Depends, HTTPException

from obj_storage.base import AbstractStorage
# ┌─────────────  Change ONLY the import below  ─────────────┐
from obj_storage.minio_implmt import MinIOStorage as Storage   # MinIO

# from obj_storage.blob_impl import AzureBlobStorage as Storage   # Azure Blob
# └──────────────────────────────────────────────────────────┘

# ------------------------------------------------------------------
storage: AbstractStorage = Storage()  # <-- provider-agnostic instance


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not await storage.health():
        raise RuntimeError("Storage backend is unhealthy")
    yield


app = FastAPI(title="File-Upload API", lifespan=lifespan)


async def get_storage() -> AbstractStorage:
    return storage


@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    st: Annotated[AbstractStorage, Depends(get_storage)] = ...,
):
    if not file.filename:
        raise HTTPException(400, "Filename missing")

    content = await file.read()
    reference = await st.upload(
        bucket=os.getenv("STORAGE_BUCKET", "uploads"),
        key=file.filename,
        data=content,
        length=len(content),
        content_type=file.content_type,
    )
    return {"reference": reference}