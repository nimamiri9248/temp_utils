from typing import Protocol, MutableMapping, Optional, Dict, Any, List, Callable
import httpx


class Transport(Protocol):
    def request(
        self,
        url: str,
        method: str,
        body: Optional[Dict[str, Any]] = ...,
        headers: Optional[MutableMapping[str, str]] = ...,
        params: Optional[Dict[str, Any]] = ...,
        timeout: Optional[httpx.Timeout] = ...,
        api_name: Optional[str] = ...,
        service: Optional[str] = ...,
        subservice_name: Optional[str] = ...,
        subject: Optional[str] = ...,
        is_internal: Optional[bool] = ...,
        prevent_additional_log: Optional[bool] = ...,
        prevent_request_headers_log: Optional[bool] = ...,
        prevent_request_body_log: Optional[bool] = ...,
        prevent_response_body_log: Optional[bool] = ...,
    ) -> Dict[str, Any]: ...

    async def async_request(
        self,
        url: str,
        method: str,
        body: Optional[Dict[str, Any]] = ...,
        headers: Optional[MutableMapping[str, str]] = ...,
        params: Optional[Dict[str, Any]] = ...,
        timeout: Optional[httpx.Timeout] = ...,
        api_name: Optional[str] = ...,
        service: Optional[str] = ...,
        subservice_name: Optional[str] = ...,
        subject: Optional[str] = ...,
        is_internal: Optional[bool] = ...,
        prevent_additional_log: Optional[bool] = ...,
        prevent_request_headers_log: Optional[bool] = ...,
        prevent_request_body_log: Optional[bool] = ...,
        prevent_response_body_log: Optional[bool] = ...,
        request_parser:  Optional[List[Callable[[dict], dict]]] = ...,
        response_parser: Optional[List[Callable[[dict], dict]]] = ...,
    ) -> Dict[str, Any]: ...

    async def request_soap(
        self,
        url: str,
        payload: str,
        headers: Optional[MutableMapping[str, str]] = ...,
        timeout: Optional[httpx.Timeout] = ...,
        api_name: Optional[str] = ...,
        service: Optional[str] = ...,
        subservice_name: Optional[str] = ...,
        subject: Optional[str] = ...,
        prevent_additional_log: Optional[bool] = ...,
        prevent_request_headers_log: Optional[bool] = ...,
        prevent_request_body_log: Optional[bool] = ...,
        prevent_response_body_log: Optional[bool] = ...,
    ) -> Dict[str, Any]: ...
