import json
import time
from typing import Any, Callable, Dict, List, MutableMapping, Optional, Tuple, Union

import httpx
from httpx import ConnectError, ConnectTimeout, ReadError, ReadTimeout
from pydantic import BaseModel, Field

from cmp_logging.logger import Logger
from cmp_logging.utils import get_correlation_id

from .exceptions import InvalidUpstreamResponse
from .utils import decode_secret_values, maske_secret_values  


class _HTTPResult(BaseModel):
    data: Union[Dict[str, Any], str] = Field(default_factory=dict)
    status_code: int = 500


class _IntegrationParams(BaseModel):
    url: str
    method: str
    request_headers: Union[Dict[str, Any], str, None] = Field(default_factory=dict)
    request_params: Union[Dict[str, Any], str, None] = Field(default_factory=dict)
    request_body: Union[Dict[str, Any], str, None] = Field(default_factory=dict)
    api_name: Optional[str] = ""
    service: Optional[str] = ""
    subservice_name: Optional[str] = ""
    subject: Optional[str] = ""
    is_internal: Optional[bool] = False
    prevent_additional_log: Optional[bool] = False
    is_successfull: Optional[bool] = None
    status_code: Optional[int] = None
    response_content: Union[Dict[str, Any], str] = Field(default_factory=dict)
    process_time: Optional[float] = None


class HttpxTransport:
    def __init__(
        self,
        logger: Logger,
        http_client: Optional[httpx.AsyncClient] = None,
        sync_http_client: Optional[httpx.Client] = None,
    ) -> None:
        self.logger = logger
        self.http_client = http_client or httpx.AsyncClient()
        self.sync_http_client = sync_http_client or httpx.Client()

    @staticmethod
    def _ensure_defaults(
        body: Optional[Dict[str, Any]],
        headers: Optional[MutableMapping[str, str]],
        params: Optional[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], MutableMapping[str, str], Dict[str, Any]]:
        return body or {}, headers or {}, params or {}

    def request(
        self,
        url: str,
        method: str,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[MutableMapping[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[httpx.Timeout] = None,
        api_name: Optional[str] = "",
        service: Optional[str] = "",
        subservice_name: Optional[str] = "",
        subject: Optional[str] = "",
        is_internal: Optional[bool] = False,
        prevent_additional_log: Optional[bool] = False,
        prevent_request_headers_log: Optional[bool] = False,
        prevent_request_body_log: Optional[bool] = False,
        prevent_response_body_log: Optional[bool] = False,
    ) -> Dict[str, Any]:
        body, headers, params = self._ensure_defaults(body, headers, params)

        if is_internal and not headers.get("x-correlation-id"):
            headers["x-correlation-id"] = get_correlation_id()

        result = _HTTPResult()
        kwargs: Dict[str, Any] = {"url": url, "headers": headers}
        if timeout:
            kwargs["timeout"] = timeout

        start = time.time()
        integration_params = _IntegrationParams(
            url=url,
            method=method,
            request_headers={} if prevent_request_headers_log else headers,
            request_params=params,
            request_body={} if prevent_request_body_log else body,
            api_name=api_name,
            service=service,
            subservice_name=subservice_name,
            subject=subject,
            is_internal=is_internal,
            prevent_additional_log=prevent_additional_log,
        )

        try:
            method_lower = method.lower()
            if method_lower == "get":
                if params:
                    kwargs["params"] = params
                resp = self.sync_http_client.get(**kwargs)
            elif method_lower == "post":
                if body:
                    kwargs["json"] = body
                resp = self.sync_http_client.post(**kwargs)
            elif method_lower in {"put", "delete"}:
                merged_headers = {**headers, "Content-Type": "application/json"}
                content = json.dumps(body).encode("utf-8") if body else b""
                resp = self.sync_http_client.request(
                    url=url,
                    method=method.upper(),
                    headers=merged_headers,
                    content=content,
                    timeout=kwargs.get("timeout"),
                )
            else:
                raise ValueError(f"Invalid method {method}")

            try:
                result.data = resp.json()
            except Exception:
                result.data = {}

            result.status_code = resp.status_code
            integration_params.is_successfull = True
            integration_params.status_code = result.status_code
            integration_params.response_content = {} if prevent_response_body_log else result.data
            integration_params.process_time = time.time() - start

            self.logger.sync_integration(**integration_params.model_dump())
            return result.model_dump()

        except (ConnectError, ConnectTimeout, ReadError, ReadTimeout):
            integration_params.is_successfull = False
            integration_params.status_code = 500
            integration_params.response_content = ""
            integration_params.process_time = time.time() - start
            self.logger.sync_integration(**integration_params.model_dump())
            raise
        except Exception:
            integration_params.is_successfull = False
            integration_params.status_code = 500
            integration_params.response_content = ""
            integration_params.process_time = time.time() - start
            self.logger.sync_integration(**integration_params.model_dump())
            raise InvalidUpstreamResponse
    
    async def async_request(
        self,
        url: str,
        method: str,
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[MutableMapping[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[httpx.Timeout] = None,
        api_name: Optional[str] = "",
        service: Optional[str] = "",
        subservice_name: Optional[str] = "",
        subject: Optional[str] = "",
        is_internal: Optional[bool] = False,
        prevent_additional_log: Optional[bool] = False,
        prevent_request_headers_log: Optional[bool] = False,
        prevent_request_body_log: Optional[bool] = False,
        prevent_response_body_log: Optional[bool] = False,
        request_parser:  Optional[List[Callable[[dict], dict]]] = None,
        response_parser: Optional[List[Callable[[dict], dict]]] = None,
    ) -> Dict[str, Any]:

        body, headers, params = self._ensure_defaults(body, headers, params)

        self.http_client.cookies.clear()

        if is_internal and not headers.get("x-correlation-id"):
            headers["x-correlation-id"] = get_correlation_id()

        upstream_headers = decode_secret_values(dict(headers))
        upstream_body = decode_secret_values(dict(body))

        if request_parser:
            for parser in request_parser:
                upstream_headers, upstream_body = parser(upstream_headers, upstream_body)

        request_headers_for_log = maske_secret_values(headers)
        request_body_for_log = maske_secret_values(body)

        result = _HTTPResult()
        kwargs: Dict[str, Any] = {"url": url, "headers": upstream_headers}
        if timeout:
            kwargs["timeout"] = timeout

        start = time.time()
        integration_params = _IntegrationParams(
            url=url,
            method=method,
            request_headers={} if prevent_request_headers_log else request_headers_for_log,
            request_params=params,
            request_body={} if prevent_request_body_log else request_body_for_log,
            api_name=api_name,
            service=service,
            subservice_name=subservice_name,
            subject=subject,
            is_internal=is_internal,
            prevent_additional_log=prevent_additional_log,
        )

        try:
            method_lower = method.lower()
            if method_lower == "get":
                if params:
                    kwargs["params"] = params
                resp = await self.http_client.get(**kwargs)
            elif method_lower == "post":
                if upstream_body:
                    kwargs["json"] = upstream_body
                resp = await self.http_client.post(**kwargs)
            elif method_lower in {"put", "delete"}:
                merged_headers = {**upstream_headers, "Content-Type": "application/json"}
                content = json.dumps(upstream_body).encode("utf-8") if upstream_body else b""
                resp = await self.http_client.request(
                    url=url,
                    method=method.upper(),
                    headers=merged_headers,
                    content=content,
                    timeout=kwargs.get("timeout"),
                )
            else:
                raise ValueError(f"Invalid method {method}")

            try:
                parsed = resp.json()
            except Exception:
                parsed = {}

            if response_parser:
                for parser in response_parser:
                    parsed = parser(parsed)

            result.data = parsed
            result.status_code = resp.status_code
            integration_params.is_successfull = True
            integration_params.status_code = result.status_code
            integration_params.response_content = {} if prevent_response_body_log else result.data
            integration_params.process_time = time.time() - start

            await self.logger.integration(**integration_params.model_dump())
            return result.model_dump()

        except (ConnectError, ConnectTimeout, ReadError, ReadTimeout):
            integration_params.is_successfull = False
            integration_params.status_code = 500
            integration_params.response_content = ""
            integration_params.process_time = time.time() - start
            await self.logger.integration(**integration_params.model_dump())
            raise
        except Exception:
            integration_params.is_successfull = False
            integration_params.status_code = 500
            integration_params.response_content = ""
            integration_params.process_time = time.time() - start
            await self.logger.integration(**integration_params.model_dump())
            raise InvalidUpstreamResponse

    async def request_soap(
        self,
        url: str,
        payload: str,
        headers: Optional[MutableMapping[str, str]] = None,
        timeout: Optional[httpx.Timeout] = None,
        api_name: Optional[str] = "",
        service: Optional[str] = "",
        subservice_name: Optional[str] = "",
        subject: Optional[str] = "",
        prevent_additional_log: Optional[bool] = False,
        prevent_request_headers_log: Optional[bool] = False,
        prevent_request_body_log: Optional[bool] = False,
        prevent_response_body_log: Optional[bool] = False,
    ) -> Dict[str, Any]:
        headers = headers or {}

        result = _HTTPResult(data="")
        kwargs: Dict[str, Any] = {"url": url, "headers": headers}
        if timeout:
            kwargs["timeout"] = timeout

        start = time.time()
        integration_params = _IntegrationParams(
            url=url,
            method="POST",
            request_headers={} if prevent_request_headers_log else headers,
            request_params="",
            request_body="" if prevent_request_body_log else payload,
            api_name=api_name,
            service=service,
            subservice_name=subservice_name,
            subject=subject,
            prevent_additional_log=prevent_additional_log,
        )

        try:
            kwargs["data"] = payload.encode("utf-8")
            resp = await self.http_client.post(**kwargs)
            result.data = resp.text
            result.status_code = resp.status_code

            integration_params.is_successfull = True
            integration_params.status_code = result.status_code
            integration_params.response_content = "" if prevent_response_body_log else result.data
            integration_params.process_time = time.time() - start

            await self.logger.integration(**integration_params.model_dump())
            return result.model_dump()

        except (ConnectError, ConnectTimeout, ReadError, ReadTimeout):
            integration_params.is_successfull = False
            integration_params.status_code = 500
            integration_params.response_content = ""
            integration_params.process_time = time.time() - start
            await self.logger.integration(**integration_params.model_dump())
            raise
        except Exception:
            integration_params.is_successfull = False
            integration_params.status_code = 500
            integration_params.response_content = ""
            integration_params.process_time = time.time() - start
            await self.logger.integration(**integration_params.model_dump())
            raise InvalidUpstreamResponse


    