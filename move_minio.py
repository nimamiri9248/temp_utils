from pydantic_settings import BaseSettings
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource


class MinIOSettings(BaseSettings):
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "password123"
    minio_secure: bool = False
    minio_bucket_name: str = "hello"

    class Config:
        env_file = ".env"


def init_minio(settings: MinIOSettings) -> Minio:
    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def normalize_prefix(p: str) -> str:
    if not p:
        return ""
    return p if p.endswith("/") else p + "/"


def move_minio_prefix(
    src_bucket: str,
    src_prefix: str,
    dest_bucket: str,
    dest_prefix: str,
    *,
    overwrite: bool = False,
):
    settings = MinIOSettings()
    client = init_minio(settings)

    src_prefix = normalize_prefix(src_prefix)
    dest_prefix = normalize_prefix(dest_prefix)

    if not client.bucket_exists(src_bucket):
        raise RuntimeError(f"Source bucket does not exist: {src_bucket}")
    ensure_bucket(client, dest_bucket)

    moved = copied = skipped = errors = 0

    for obj in client.list_objects(src_bucket, prefix=src_prefix, recursive=True):
        key = obj.object_name
        tail = key[len(src_prefix):] if src_prefix and key.startswith(src_prefix) else key
        dest_key = f"{dest_prefix}{tail}"

        if not overwrite:
            try:
                client.stat_object(dest_bucket, dest_key)
                print(f"SKIP (exists): s3://{dest_bucket}/{dest_key}")
                skipped += 1
                continue
            except S3Error as e:
                if e.code not in ("NoSuchKey", "NotFound"):
                    raise

        print(f"COPY: {src_bucket}/{key} {dest_bucket}/{dest_key}")
        try:
            client.copy_object(dest_bucket, dest_key, CopySource(src_bucket, key))
            copied += 1
            client.remove_object(src_bucket, key)
            moved += 1
            print(f"DELETE: {src_bucket}/{key}")
        except Exception as e:
            errors += 1
            print(f"ERROR moving {key}: {e}")
            
    print(f"moved={moved}, copied={copied}, skipped={skipped}, errors={errors}")


if __name__ == "__main__":
    src_bucket = "hello"
    src_prefix = "hello5/hello2"
    dest_bucket = "hello2"
    dest_prefix = "hello8/hello2"
    overwrite = False
    move_minio_prefix(
        src_bucket, src_prefix,
        dest_bucket, dest_prefix,
        overwrite=overwrite,
    )