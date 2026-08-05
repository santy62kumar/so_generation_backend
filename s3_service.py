import io
import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from PIL import Image, ImageOps


load_dotenv()


AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_FINISH_PREFIX = os.getenv(
    "AWS_FINISH_PREFIX",
    "modula-finish-assets/finishes",
).strip("/")


if not AWS_S3_BUCKET:
    raise RuntimeError("AWS_S3_BUCKET is missing from the environment.")

if not os.getenv("AWS_ACCESS_KEY_ID"):
    raise RuntimeError("AWS_ACCESS_KEY_ID is missing from the environment.")

if not os.getenv("AWS_SECRET_ACCESS_KEY"):
    raise RuntimeError("AWS_SECRET_ACCESS_KEY is missing from the environment.")


# Boto3 automatically reads:
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY
# AWS_SESSION_TOKEN, when applicable
s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
)


ALLOWED_CATEGORIES = {
    "cabinet",
    "glass",
    "gola",
    "skirting",
    "open-shelf",
    "handle-profile",
}

CONTENT_TYPE_EXTENSIONS = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/webp": ".webp",
}


def slugify(value: str) -> str:
    """
    Convert:
        Abyss Edge -> abyss-edge
        Canyon Ridge Gloss -> canyon-ridge-gloss
    """
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def validate_category(category: str) -> str:
    category = category.strip().lower()

    if category not in ALLOWED_CATEGORIES:
        raise ValueError(
            "Invalid category. Allowed categories are: "
            "cabinet, glass, skirting, open-shelf, gola."
        )

    return category


def create_thumbnail(
    file_content: bytes,
    size: tuple[int, int] = (160, 160),
    quality: int = 75,
) -> bytes:
    """
    Generate a square WebP thumbnail while preserving transparency
    for PNG images.
    """
    try:
        with Image.open(io.BytesIO(file_content)) as image:
            image.load()

            if image.mode in {"RGBA", "LA"} or (
                image.mode == "P" and "transparency" in image.info
            ):
                image = image.convert("RGBA")
            else:
                image = image.convert("RGB")

            thumbnail = ImageOps.fit(
                image,
                size,
                method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.5),
            )

            output = io.BytesIO()

            thumbnail.save(
                output,
                format="WEBP",
                quality=quality,
                method=6,
            )

            return output.getvalue()

    except Exception as exc:
        raise ValueError("Unable to process the uploaded image.") from exc


def build_public_s3_url(object_key: str) -> str:
    """
    Works only when the object is publicly readable.

    For a private bucket, use CloudFront or generate_presigned_url instead.
    """
    encoded_key = quote(object_key, safe="/")

    return (
        f"https://{AWS_S3_BUCKET}.s3."
        f"{AWS_REGION}.amazonaws.com/{encoded_key}"
    )


def generate_presigned_url(
    object_key: str,
    expires_in: int = 3600,
) -> str:
    """
    Create a temporary URL for a private S3 object.
    """
    return s3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": AWS_S3_BUCKET,
            "Key": object_key,
        },
        ExpiresIn=expires_in,
    )


def finish_thumb_key(category: str, color_id: str) -> str:
    """
    Build the deterministic S3 key for a finish's thumbnail, e.g.:
        modula-finish-assets/finishes/cabinet/abyss-edge-thumb.webp

    `category` and `color_id` are expected to already be the slug forms
    used throughout the app (see kitchenFinishColors.jsx / ColorSwatchSelect
    composite "category:id" values).
    """
    category = validate_category(category)
    slug = slugify(color_id)

    if not slug:
        raise ValueError("Finish id is invalid.")

    return f"{AWS_FINISH_PREFIX}/{category}/{slug}-thumb.webp"


def get_finish_thumbnail(category: str, color_id: str) -> bytes:
    """
    Fetch one finish's thumbnail image straight from S3, on demand.

    Used at PDF-generation time: given a composite value like
    "cabinet:abyss-edge", the caller splits it into (category, color_id)
    and calls this to get the actual swatch image bytes for that one
    color — never the whole catalog.
    """
    key = finish_thumb_key(category, color_id)
    return get_s3_file(key)


def upload_finish_to_s3(
    *,
    file_content: bytes,
    original_filename: str,
    content_type: str,
    category: str,
    finish_name: str,
) -> dict:
    """
    Upload:
        original master PNG/JPG
        generated WebP thumbnail

    Example keys:
        modula-finish-assets/finishes/cabinet/abyss-edge-master.png
        modula-finish-assets/finishes/cabinet/abyss-edge-thumb.webp
    """
    if not file_content:
        raise ValueError("The uploaded file is empty.")

    category = validate_category(category)
    slug = slugify(finish_name)

    if not slug:
        raise ValueError("Finish name is invalid.")

    normalized_content_type = content_type.lower().strip()
    extension = CONTENT_TYPE_EXTENSIONS.get(normalized_content_type)

    if not extension:
        # Filename fallback when the browser sends an incorrect content type.
        suffix = Path(original_filename).suffix.lower()

        if suffix in {".jpg", ".jpeg"}:
            extension = ".jpg"
            normalized_content_type = "image/jpeg"
        elif suffix == ".png":
            extension = ".png"
            normalized_content_type = "image/png"
        elif suffix == ".webp":
            extension = ".webp"
            normalized_content_type = "image/webp"
        else:
            raise ValueError(
                "Only PNG, JPG, JPEG and WebP images are supported."
            )

    master_key = (
        f"{AWS_FINISH_PREFIX}/"
        f"{category}/"
        f"{slug}-master{extension}"
    )

    thumb_key = finish_thumb_key(category, slug)

    thumbnail_content = create_thumbnail(file_content)

    try:
        # Original master image
        s3_client.put_object(
            Bucket=AWS_S3_BUCKET,
            Key=master_key,
            Body=file_content,
            ContentType=normalized_content_type,
            CacheControl="private, max-age=86400",
            ServerSideEncryption="AES256",
        )

        # UI thumbnail
        s3_client.put_object(
            Bucket=AWS_S3_BUCKET,
            Key=thumb_key,
            Body=thumbnail_content,
            ContentType="image/webp",
            CacheControl="public, max-age=31536000, immutable",
            ServerSideEncryption="AES256",
        )

    except (ClientError, BotoCoreError) as exc:
        # Prevent an incomplete finish if only one upload succeeds.
        try:
            s3_client.delete_object(
                Bucket=AWS_S3_BUCKET,
                Key=master_key,
            )
            s3_client.delete_object(
                Bucket=AWS_S3_BUCKET,
                Key=thumb_key,
            )
        except Exception:
            pass

        raise RuntimeError(
            "Unable to upload finish images to S3."
        ) from exc

    return {
        "category": category,
        "slug": slug,
        "name": finish_name.strip(),
        "master_key": master_key,
        "thumb_key": thumb_key,
        "master_content_type": normalized_content_type,
        "thumb_content_type": "image/webp",
        # Use this for private-bucket testing.
        "thumb_url": generate_presigned_url(
            thumb_key,
            expires_in=3600,
        ),
    }


def get_s3_file(object_key: str) -> bytes:
    try:
        response = s3_client.get_object(
            Bucket=AWS_S3_BUCKET,
            Key=object_key,
        )

        return response["Body"].read()

    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")

        if error_code in {"NoSuchKey", "404"}:
            raise FileNotFoundError(
                f"S3 object was not found: {object_key}"
            ) from exc

        raise RuntimeError(
            f"Unable to retrieve S3 object: {object_key}"
        ) from exc


def delete_s3_file(object_key: Optional[str]) -> None:
    if not object_key:
        return

    try:
        s3_client.delete_object(
            Bucket=AWS_S3_BUCKET,
            Key=object_key,
        )

    except ClientError as exc:
        raise RuntimeError(
            f"Unable to delete S3 object: {object_key}"
        ) from exc




def test_s3_connection() -> dict:
    try:
        response = s3_client.list_objects_v2(
            Bucket=AWS_S3_BUCKET,
            Prefix=AWS_FINISH_PREFIX,
            MaxKeys=5,
        )

        return {
            "connected": True,
            "bucket": AWS_S3_BUCKET,
            "region": AWS_REGION,
            "prefix": AWS_FINISH_PREFIX,
            "object_count": response.get("KeyCount", 0),
        }

    except ClientError as exc:
        return {
            "connected": False,
            "error": exc.response.get(
                "Error",
                {},
            ).get(
                "Message",
                str(exc),
            ),
        }


def test_s3_write() -> dict:
    test_key = f"{AWS_FINISH_PREFIX}/connection-write-test.txt"

    try:
        s3_client.put_object(
            Bucket=AWS_S3_BUCKET,
            Key=test_key,
            Body=b"S3 write connection successful",
            ContentType="text/plain",
        )

        s3_client.delete_object(
            Bucket=AWS_S3_BUCKET,
            Key=test_key,
        )

        return {
            "success": True,
            "key": test_key,
        }

    except Exception as exc:
        return {
            "success": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }