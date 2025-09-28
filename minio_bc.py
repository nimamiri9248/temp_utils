from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Iterator, BinaryIO, Optional

from minio import Minio
from minio.error import S3Error
from fastapi import Depends
from pydantic_settings import BaseSettings

from error_helper import Err, ErrorCode


class MinIOSettings(BaseSettings):
    
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "password123"
    minio_secure: bool = False
    minio_bucket_name: str = "hello"
    
    class Config:
        env_file = ".env"


class MinIOServiceInterface(ABC):
    
    @abstractmethod
    def stream_file(self, directory: str, filename: str) -> Iterator[bytes]:
        """Stream a file from a specific directory."""
        pass
    
    @abstractmethod
    def delete_file(self, directory: str, filename: str) -> bool:
        """Delete a file from a specific directory."""
        pass
    
    @abstractmethod
    def upload_stream(
        self,
        data: BinaryIO,
        directory: str,
        filename: str,
        *,
        content_type: Optional[str] = None,
        part_size: int = 10 * 1024 * 1024, 
    ) -> str:
        pass
    

class MinIOService(MinIOServiceInterface):
    
    def __init__(self, settings: MinIOSettings):
        self.settings = settings
        self.client = Minio(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure
        )
        self.bucket_name = settings.minio_bucket_name
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except S3Error as e:
            return Err(ErrorCode.BUCKET_ACCESS, str(e))
        except Exception as e:
            return Err(ErrorCode.UNKNOWN, e)
    
    def _build_object_name(self, directory: str, filename: str) -> str:
        directory = directory.strip("/")
        if directory:
            return f"{directory}/{filename}"
        return filename


    def upload_stream(
        self,
        data: BinaryIO,
        directory: str,
        filename: str,
        *,
        content_type: Optional[str] = None,
        part_size: int = 5 * 1024 * 1024,
    ) -> str:
        try:
            if not filename:
                raise ValueError("Filename must be provided")

            object_name = self._build_object_name(directory, filename)
            ct = content_type or "application/octet-stream"
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=data,
                length=-1,
                content_type=ct,
                part_size=part_size, 
            )

            return object_name

        except S3Error as e:
            return Err(ErrorCode.UPLOAD_FAILED, str(e))
        except Exception as e:
            return Err(ErrorCode.UNKNOWN, f"Upload error for '{object_name}': {e}")
        finally:
            if hasattr(data, 'close') and callable(data.close):
                try:
                    data.close()
                except Exception:
                    pass

    
    def stream_file(self, directory: str, filename: str) -> Iterator[bytes]:
        try:
            object_name = self._build_object_name(directory, filename)
            response = self.client.get_object(self.bucket_name, object_name)
            
            try:
                chunk_size = 8192
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
            finally:
                response.close()
                response.release_conn()
                
        except S3Error as e:
            if e.code == "NoSuchKey":
                return Err(ErrorCode.NOT_FOUND, f"Object not found: '{object_name}'")
            return Err(ErrorCode.STREAM_FAILED, f"Failed to open stream: {e}")
        except Exception as e:
            return Err(ErrorCode.UNKNOWN, e)
        finally:
            if response:
                response.close()
                response.release_conn()

    
    def delete_file(self, directory: str, filename: str) -> bool:
        try:
            object_name = self._build_object_name(directory, filename)
            self.client.remove_object(self.bucket_name, object_name)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False 
            return Err(ErrorCode.UNKNOWN, e)
        except Exception as e:
            return Err(ErrorCode.UNKNOWN, e)
    
    
    def generate_presigned_url(
        self,
        directory: str,
        filename: str,
        expiry: timedelta = timedelta(hours=1),
        method: str = "GET"
    ) -> str:
        try:
            object_name = self._build_object_name(directory, filename)
            return self.client.presigned_url(
                method=method,
                bucket_name=self.bucket_name,
                object_name=object_name,
                expires=expiry
            )
        except S3Error as e:
            return Err(ErrorCode.PRESIGN_FAILED, f"Failed to presign '{object_name}': {e}")
        except Exception as e:
            return Err(ErrorCode.UNKNOWN, e)


def get_minio_settings() -> MinIOSettings:
    return MinIOSettings()


def get_minio_service(settings: MinIOSettings = Depends(get_minio_settings)) -> MinIOServiceInterface:
    return MinIOService(settings)