from __future__ import annotations

import mimetypes
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

DEFAULT_MINERU_SECTION = "tiangong_mineru_with_image"


@dataclass(slots=True)
class MineruWithImagesConfig:
    url: str
    service_name: str
    transport: str
    api_key: str | None
    api_key_header: str
    api_key_prefix: str
    timeout: float
    provider: str | None
    model: str | None
    chunk_type: bool | None
    return_txt: bool | None
    verify_ssl: bool

    def auth_headers(self) -> dict[str, str]:
        if not self.api_key:
            return {}
        header = self.api_key_header.strip()
        prefix = self.api_key_prefix.strip()
        if prefix:
            value = f"{prefix} {self.api_key}".strip()
        else:
            value = self.api_key
        return {header: value}


class MineruWithImagesClient:
    def __init__(self, config: MineruWithImagesConfig) -> None:
        self._config = config

    def split_document(
        self,
        file_path: Path,
        *,
        provider: str | None = None,
        model: str | None = None,
        chunk_type: bool | None = None,
        return_txt: bool | None = None,
        timeout: float | None = None,
    ) -> Any:
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        if self._config.transport not in {"streamable_http", "http", "https"}:
            raise SystemExit(
                f"Unsupported MinerU transport '{self._config.transport}'. "
                "Expected one of: streamable_http, http, https."
            )
        payload = _build_payload(
            provider if provider is not None else self._config.provider,
            model if model is not None else self._config.model,
        )
        params = _build_params(
            chunk_type if chunk_type is not None else self._config.chunk_type,
            return_txt if return_txt is not None else self._config.return_txt,
        )
        timeout_seconds = timeout if timeout is not None else self._config.timeout
        headers = self._config.auth_headers()
        with httpx.Client(timeout=timeout_seconds, verify=self._config.verify_ssl, headers=headers) as client:
            with file_path.open("rb") as handle:
                content_type = _guess_content_type(file_path)
                files = {"file": (file_path.name, handle, content_type)}
                response = client.post(
                    self._config.url,
                    params=params,
                    data=payload,
                    files=files,
                )
                response.raise_for_status()
        return _parse_response(response)


def load_mineru_with_images_config() -> MineruWithImagesConfig:
    """Load MinerU-with-images config from environment variables only."""
    service_name = (
        _env_first(
            "TIANGONG_MINERU_WITH_IMAGE_SERVICE_NAME",
            "MINERU_WITH_IMAGES_SERVICE_NAME",
            "MINERU_SERVICE_NAME",
        )
        or DEFAULT_MINERU_SECTION
    )
    transport = (
        _env_first(
            "TIANGONG_MINERU_WITH_IMAGE_TRANSPORT",
            "MINERU_WITH_IMAGES_TRANSPORT",
            "MINERU_TRANSPORT",
        )
        or "streamable_http"
    ).strip().lower()

    url = _env_first("TIANGONG_MINERU_WITH_IMAGE_URL", "MINERU_WITH_IMAGES_URL", "MINERU_URL")
    if not url:
        raise SystemExit("Mineru service URL missing. Set TIANGONG_MINERU_WITH_IMAGE_URL.")
    if transport not in {"streamable_http", "http", "https"}:
        raise SystemExit(
            f"Unsupported MinerU transport '{transport}'. "
            "Expected one of: streamable_http, http, https."
        )

    api_key_header = _env_first("TIANGONG_MINERU_WITH_IMAGE_API_KEY_HEADER", "MINERU_WITH_IMAGES_API_KEY_HEADER") or "Authorization"
    api_key_prefix = _env_first("TIANGONG_MINERU_WITH_IMAGE_API_KEY_PREFIX", "MINERU_WITH_IMAGES_API_KEY_PREFIX")
    if api_key_prefix is None:
        api_key_prefix = "Bearer"

    api_key = _sanitize_api_key(
        _env_first(
            "TIANGONG_MINERU_WITH_IMAGE_API_KEY",
            "MINERU_WITH_IMAGES_API_KEY",
            "MINERU_API_KEY",
            "TIANGONG_MINERU_WITH_IMAGE_AUTHORIZATION",
        ),
        api_key_prefix,
    )
    timeout = _coerce_float(_env_first("TIANGONG_MINERU_WITH_IMAGE_TIMEOUT", "MINERU_WITH_IMAGES_TIMEOUT"))
    if timeout is None:
        timeout = 180.0

    provider = _optional_str(_env_first("TIANGONG_MINERU_WITH_IMAGE_PROVIDER", "MINERU_WITH_IMAGES_PROVIDER"))
    model = _optional_str(_env_first("TIANGONG_MINERU_WITH_IMAGE_MODEL", "MINERU_WITH_IMAGES_MODEL"))
    chunk_type = _coerce_bool(_env_first("TIANGONG_MINERU_WITH_IMAGE_CHUNK_TYPE", "MINERU_WITH_IMAGES_CHUNK_TYPE"), default=None)
    return_txt = _coerce_bool(
        _env_first(
            "TIANGONG_MINERU_WITH_IMAGE_RETURN_TXT",
            "MINERU_WITH_IMAGES_RETURN_TXT",
            "MINERU_RETURN_TXT",
        ),
        default=True,
    )
    if return_txt is None:
        return_txt = True
    verify_ssl = _coerce_bool(_env_first("TIANGONG_MINERU_WITH_IMAGE_VERIFY_SSL", "MINERU_WITH_IMAGES_VERIFY_SSL"), default=True)
    if verify_ssl is None:
        verify_ssl = True

    return MineruWithImagesConfig(
        url=url,
        service_name=service_name,
        transport=transport,
        api_key=api_key,
        api_key_header=api_key_header,
        api_key_prefix=api_key_prefix,
        timeout=timeout,
        provider=provider,
        model=model,
        chunk_type=chunk_type,
        return_txt=return_txt,
        verify_ssl=verify_ssl,
    )


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _build_payload(provider: str | None, model: str | None) -> dict[str, str]:
    payload: dict[str, str] = {}
    if provider:
        payload["provider"] = provider
    if model:
        payload["model"] = model
    return payload


def _build_params(chunk_type: bool | None, return_txt: bool | None) -> dict[str, str]:
    params: dict[str, str] = {}
    if chunk_type is not None:
        params["chunk_type"] = "true" if chunk_type else "false"
    if return_txt is not None:
        params["return_txt"] = "true" if return_txt else "false"
    return params


def _guess_content_type(file_path: Path) -> str:
    content_type, _ = mimetypes.guess_type(file_path.name)
    return content_type or "application/octet-stream"


def _parse_response(response: httpx.Response) -> Any:
    if not response.content:
        return None
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type.lower():
        return response.json()
    try:
        return response.json()
    except ValueError:
        return response.text


def _sanitize_api_key(value: Any, prefix: str | None) -> str | None:
    if not value:
        return None
    token = str(value).strip()
    if not token:
        return None
    prefix_text = prefix.strip() if isinstance(prefix, str) else ""
    if prefix_text and token.lower().startswith(f"{prefix_text.lower()} "):
        token = token[len(prefix_text) + 1 :].strip()
    return token or None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any, *, default: bool | None) -> bool | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def _optional_str(value: Any, *, allow_blank: bool = False) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if allow_blank:
        return text
    return text or None


__all__ = ["MineruWithImagesClient", "MineruWithImagesConfig", "load_mineru_with_images_config"]
