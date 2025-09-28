from enum import Enum
from typing import Generic, Optional, TypeVar
from pydantic import BaseModel
from pydantic.generics import GenericModel

T = TypeVar("T")

class ErrorCode(str, Enum):
    NOT_FOUND = "NOT_FOUND"
    UPLOAD_FAILED = "UPLOAD_FAILED"
    STREAM_FAILED = "STREAM_FAILED"
    DELETE_FAILED = "DELETE_FAILED"
    PRESIGN_FAILED = "PRESIGN_FAILED"
    BUCKET_ACCESS = "BUCKET_ACCESS"
    UNKNOWN = "UNKNOWN"

class Error(BaseModel):
    code: ErrorCode
    message: str
    retryable: bool = False

class Result(GenericModel, Generic[T]):
    ok: bool
    value: Optional[T] = None
    error: Optional[Error] = None


def Err(code: ErrorCode, msg: str) -> Result[T]:
    return Result[T](ok=False, error=Error(code=code, message=msg))
