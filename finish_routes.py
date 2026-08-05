from typing import List, Literal, Optional
import asyncio

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from database import get_db
from finish_models import Finish
from s3_service import (
    delete_s3_file,
    generate_presigned_url,
    test_s3_connection,
    test_s3_write,
    upload_finish_to_s3,
)


router = APIRouter(
    prefix="/api/finishes",
    tags=["Finishes"],
)

FinishCategory = Literal["cabinet", "glass", "skirting", "open-shelf", "handle-profile", "gola"]

MAX_IMAGE_SIZE = 15 * 1024 * 1024  # 15 MB


def serialize_finish(finish: Finish) -> dict:
    return {
        "id": finish.id,
        "category": finish.category,
        "slug": finish.slug,
        "name": finish.name,
        "value": f"{finish.category}:{finish.slug}",
        "thumb_url": generate_presigned_url(
            finish.thumb_key,
            expires_in=3600,
        ),
        "is_active": finish.is_active,
    }


def _validate_image_bytes(content: bytes) -> None:
    if not content:
        raise HTTPException(status_code=400, detail="The uploaded image is empty.")
    if len(content) > MAX_IMAGE_SIZE:
        raise HTTPException(status_code=400, detail="The image must not exceed 15 MB.")


async def _cleanup_uploaded(uploaded: dict) -> None:
    """Best-effort removal of S3 objects after a failed DB write."""
    try:
        await asyncio.to_thread(delete_s3_file, uploaded["master_key"])
        await asyncio.to_thread(delete_s3_file, uploaded["thumb_key"])
    except Exception:
        pass


async def _persist_finish(
    db: Session,
    category: str,
    name: str,
    image_content: bytes,
    filename: Optional[str],
    content_type: Optional[str],
):
    """
    Upload one image to S3 and create the corresponding Finish row.
    Raises HTTPException on any failure; cleans up S3 objects on rollback.
    Shared by both the single-create and bulk-create endpoints.
    """
    clean_name = name.strip()
    if not clean_name:
        raise HTTPException(status_code=400, detail="Finish name is required.")

    _validate_image_bytes(image_content)

    uploaded = None
    try:
        # Blocking network/disk call -> run off the event loop so bulk
        # uploads can overlap instead of running strictly sequentially.
        uploaded = await asyncio.to_thread(
            upload_finish_to_s3,
            file_content=image_content,
            original_filename=filename or "finish-image",
            content_type=content_type or "",
            category=category,
            finish_name=clean_name,
        )

        finish = Finish(
            category=uploaded["category"],
            slug=uploaded["slug"],
            name=uploaded["name"],
            master_key=uploaded["master_key"],
            thumb_key=uploaded["thumb_key"],
            master_content_type=uploaded["master_content_type"],
            thumb_content_type=uploaded["thumb_content_type"],
            is_active=True,
        )

        db.add(finish)
        db.commit()
        db.refresh(finish)
        return finish

    except IntegrityError as exc:
        db.rollback()
        if uploaded:
            await _cleanup_uploaded(uploaded)
        raise HTTPException(
            status_code=409,
            detail=f"A finish named '{clean_name}' already exists in this category.",
        ) from exc

    except ValueError as exc:
        db.rollback()
        if uploaded:
            await _cleanup_uploaded(uploaded)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    except HTTPException:
        # Re-raise validation errors (e.g. empty/too-large image) untouched.
        raise

    except Exception as exc:
        db.rollback()
        if uploaded:
            await _cleanup_uploaded(uploaded)
        raise HTTPException(
            status_code=500,
            detail="Unable to upload and save the finish.",
        ) from exc


def _name_from_filename(filename: Optional[str]) -> str:
    """Fallback finish name derived from the uploaded filename."""
    if not filename:
        return "finish"
    stem = filename.rsplit(".", 1)[0]
    return stem.strip() or "finish"


@router.post("", status_code=201)
async def create_finish(
    category: FinishCategory = Form(...),
    name: str = Form(...),
    image: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    image_content = await image.read()
    finish = await _persist_finish(
        db=db,
        category=category,
        name=name,
        image_content=image_content,
        filename=image.filename,
        content_type=image.content_type,
    )
    return serialize_finish(finish)


@router.post("/bulk", status_code=201)
async def bulk_create_finishes(
    category: FinishCategory = Form(...),
    names: Optional[str] = Form(
        None,
        description=(
            "Optional comma-separated list of names, matched by position "
            "to 'images' (e.g. 'Tuscan Oak,Tundra'). Swagger UI and curl "
            "send repeated form fields as one comma-joined value, so this "
            "endpoint accepts that format directly rather than a JSON/"
            "list-typed field. If omitted, names are derived from each "
            "image's filename."
        ),
    ),
    images: List[UploadFile] = File(...),
    db: Session = Depends(get_db),
):
    """
    Upload multiple finish images at once, all under the same category.

    - `names` is optional. If provided, pass it as a single comma-separated
      string (e.g. "Tuscan Oak,Tundra") matched by position to `images`.
      If omitted, each finish's name is derived from its image filename
      (without extension).
    - Each image is processed independently: one failure (duplicate name,
      bad image, etc.) is reported but does not block or roll back the
      others.
    """
    if not images:
        raise HTTPException(status_code=400, detail="At least one image is required.")

    parsed_names: Optional[List[str]] = None
    if names is not None and names.strip():
        parsed_names = [item.strip() for item in names.split(",")]

    if parsed_names is not None and len(parsed_names) != len(images):
        raise HTTPException(
            status_code=400,
            detail=(
                f"'names' has {len(parsed_names)} item(s) but 'images' has "
                f"{len(images)}. Pass one comma-separated name per image "
                "(e.g. 'Tuscan Oak,Tundra'), or omit 'names' entirely to "
                "derive names from filenames."
            ),
        )

    # Read all uploads concurrently upfront so a slow client stream doesn't
    # serialize the whole batch, and so each UploadFile is fully consumed
    # before we start doing per-item DB work.
    contents = await asyncio.gather(*(img.read() for img in images))

    created = []
    errors = []

    for index, (image, content) in enumerate(zip(images, contents)):
        finish_name = (
            parsed_names[index]
            if parsed_names
            else _name_from_filename(image.filename)
        )

        try:
            finish = await _persist_finish(
                db=db,
                category=category,
                name=finish_name,
                image_content=content,
                filename=image.filename,
                content_type=image.content_type,
            )
            created.append(serialize_finish(finish))
        except HTTPException as exc:
            errors.append(
                {
                    "filename": image.filename,
                    "name": finish_name,
                    "status_code": exc.status_code,
                    "detail": exc.detail,
                }
            )

    return {
        "category": category,
        "created": created,
        "created_count": len(created),
        "errors": errors,
        "error_count": len(errors),
    }


@router.get("")
def list_finishes(
    category: str | None = None,
    db: Session = Depends(get_db),
):
    query = db.query(Finish).filter(
        Finish.is_active.is_(True)
    )

    if category:
        query = query.filter(
            Finish.category == category.strip().lower()
        )

    finishes = query.order_by(
        Finish.category.asc(),
        Finish.name.asc(),
    ).all()

    return [
        serialize_finish(finish)
        for finish in finishes
    ]


@router.get("/{finish_id}")
def get_finish(
    finish_id: int,
    db: Session = Depends(get_db),
):
    finish = db.query(Finish).filter(
        Finish.id == finish_id
    ).first()

    if not finish:
        raise HTTPException(
            status_code=404,
            detail="Finish not found.",
        )

    return {
        **serialize_finish(finish),
        "master_key": finish.master_key,
        "master_url": generate_presigned_url(
            finish.master_key,
            expires_in=900,
        ),
    }


@router.delete("/{finish_id}")
def delete_finish(
    finish_id: int,
    db: Session = Depends(get_db),
):
    finish = db.query(Finish).filter(
        Finish.id == finish_id
    ).first()

    if not finish:
        raise HTTPException(
            status_code=404,
            detail="Finish not found.",
        )

    master_key = finish.master_key
    thumb_key = finish.thumb_key

    db.delete(finish)
    db.commit()

    try:
        delete_s3_file(master_key)
        delete_s3_file(thumb_key)
    except Exception as exc:
        # Database record has been deleted, but S3 cleanup failed.
        raise HTTPException(
            status_code=500,
            detail=(
                "Finish deleted from the database, "
                "but S3 cleanup failed."
            ),
        ) from exc

    return {
        "message": "Finish deleted successfully."
    }


@router.get("/connection/test")
def test_connection():
    return test_s3_connection()


@router.get("/connection/write-test")
def test_write_connection():
    return test_s3_write()