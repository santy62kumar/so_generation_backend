"""Shared input validation and request-size limits."""

import html
import io
import json
import zipfile
from decimal import Decimal, InvalidOperation


class InputValidationError(ValueError):
    pass


IMAGE_MIME_TYPES = {"image/gif", "image/jpeg", "image/png", "image/webp"}
MIB = 1024 * 1024
MAX_IMAGE_BYTES = 15 * MIB
MAX_REPORT_IMAGE_BYTES = 10 * MIB
MAX_XLSX_BYTES = 20 * MIB
MAX_XLSX_UNCOMPRESSED_BYTES = 200 * MIB
MAX_REPORT_PHOTOS = 20
MAX_BULK_IMAGES = 20


class RequestSizeLimitMiddleware:
    def __init__(self, app, limits: dict[str, int], default_limit: int):
        self.app = app
        self.limits = sorted(limits.items(), key=lambda item: len(item[0]), reverse=True)
        self.default_limit = default_limit

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        path = scope.get("path", "")
        limit = next((size for prefix, size in self.limits if path.startswith(prefix)), self.default_limit)
        headers = dict(scope.get("headers", []))
        try:
            if int(headers.get(b"content-length", b"0")) > limit:
                return await self._reject(send)
        except ValueError:
            return await self._reject(send)

        received = 0

        async def limited_receive():
            nonlocal received
            message = await receive()
            received += len(message.get("body", b""))
            if received > limit:
                raise InputValidationError("Request body is too large.")
            return message

        try:
            await self.app(scope, limited_receive, send)
        except InputValidationError as exc:
            if str(exc) != "Request body is too large.":
                raise
            await self._reject(send)

    @staticmethod
    async def _reject(send):
        body = b'{"detail":"Request body is too large."}'
        await send({
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        })
        await send({"type": "http.response.body", "body": body})


def escape_html(value) -> str:
    return html.escape(str(value or ""), quote=True)


def bounded_text(value, field: str, max_length: int, *, required: bool = False) -> str:
    if not isinstance(value, str):
        raise InputValidationError(f"{field} must be text.")
    value = value.strip()
    if required and not value:
        raise InputValidationError(f"{field} is required.")
    if len(value) > max_length:
        raise InputValidationError(f"{field} must not exceed {max_length} characters.")
    return value


def parse_json_object(raw: str, field: str) -> dict:
    try:
        value = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise InputValidationError(f"{field} must be valid JSON.") from exc
    if not isinstance(value, dict):
        raise InputValidationError(f"{field} must be a JSON object.")
    return value


def parse_json_list(raw: str, field: str, *, max_items: int = 50) -> list:
    try:
        value = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise InputValidationError(f"{field} must be valid JSON.") from exc
    if not isinstance(value, list):
        raise InputValidationError(f"{field} must be a JSON array.")
    if len(value) > max_items:
        raise InputValidationError(f"{field} must not contain more than {max_items} items.")
    return value


def validate_nonnegative_number(value: str, field: str, *, integer: bool = False) -> str:
    value = bounded_text(value, field, 30)
    if not value:
        return ""
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise InputValidationError(f"{field} must be a number.") from exc
    if not number.is_finite() or number < 0 or (integer and number != number.to_integral_value()):
        kind = "a non-negative whole number" if integer else "a non-negative number"
        raise InputValidationError(f"{field} must be {kind}.")
    return value


async def read_upload_limited(upload, max_bytes: int, field: str, *, allow_empty: bool = False) -> bytes:
    content = await upload.read(max_bytes + 1)
    if len(content) > max_bytes:
        raise InputValidationError(f"{field} must not exceed {max_bytes // (1024 * 1024)} MB.")
    if not content and not allow_empty:
        raise InputValidationError(f"{field} is empty.")
    return content


def validate_image(content: bytes, content_type: str | None, field: str) -> None:
    content_type = (content_type or "").lower()
    signatures = (
        content.startswith(b"\xff\xd8\xff"),
        content.startswith(b"\x89PNG\r\n\x1a\n"),
        content.startswith((b"GIF87a", b"GIF89a")),
        content.startswith(b"RIFF") and content[8:12] == b"WEBP",
    )
    if content_type not in IMAGE_MIME_TYPES or not any(signatures):
        raise InputValidationError(f"{field} must be a JPEG, PNG, GIF, or WebP image.")


def excel_safe(value):
    if isinstance(value, str) and value.startswith(("=", "+", "-", "@", "\t", "\r")):
        return f"'{value}"
    return value


def validate_xlsx_archive(content: bytes) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            files = archive.infolist()
            if len(files) > 2_000 or sum(item.file_size for item in files) > MAX_XLSX_UNCOMPRESSED_BYTES:
                raise InputValidationError("The XLSX file expands beyond the supported size.")
    except zipfile.BadZipFile as exc:
        raise InputValidationError("The uploaded file is not a valid XLSX workbook.") from exc
