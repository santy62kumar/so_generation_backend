from typing import List, Literal, Optional
import asyncio
import logging

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
)
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..core.rate_limit import SlidingWindowLimiter, env_int
from .auth import get_current_user
from ..db.database import get_db
from ..db.finish_models import Finish
from ..core.input_validation import (
    InputValidationError,
    MAX_BULK_IMAGES,
    MAX_IMAGE_BYTES,
    bounded_text,
    read_upload_limited,
    validate_image,
)
from ..services.s3_service import (
    clear_finish_thumbnail_cache,
    delete_s3_file,
    generate_presigned_url,
    slugify,
    test_s3_connection,
    test_s3_write,
    upload_finish_to_s3,
)


logger = logging.getLogger(__name__)

# The only unauthenticated route in this router. Every row it returns costs a
# presigned-URL signature, so cap how often an anonymous caller can ask.
_list_limiter = SlidingWindowLimiter(
    "finishes/list",
    env_int("FINISH_LIST_RATE_LIMIT", 120),
    env_int("FINISH_LIST_RATE_WINDOW", 60),
)
_list_rate_limit = _list_limiter.dependency()

router = APIRouter(
    prefix="/api/finishes",
    tags=["Finishes"],
)

FinishCategory = Literal["cabinet", "glass", "skirting", "open-shelf", "handle-profile", "gola"]

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
    try:
        clean_name = bounded_text(name, "Finish name", 200, required=True)
        validate_image(image_content, content_type, "Finish image")
    except InputValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    clean_slug = slugify(clean_name)
    if not clean_slug:
        raise HTTPException(status_code=400, detail="Finish name is invalid.")

    # Serialize this category/slug across workers before touching deterministic
    # S3 keys. A duplicate is rejected before an existing object can be overwritten.
    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:finish_key))"),
        {"finish_key": f"{category}:{clean_slug}"},
    )
    if db.query(Finish).filter(Finish.category == category, Finish.slug == clean_slug).first():
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=f"A finish named '{clean_name}' already exists in this category.",
        )

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
        clear_finish_thumbnail_cache()
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
        db.rollback()
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
    user: str = Depends(get_current_user),
):
    try:
        image_content = await read_upload_limited(image, MAX_IMAGE_BYTES, "Finish image")
    except InputValidationError as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
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
    user: str = Depends(get_current_user),
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
    if len(images) > MAX_BULK_IMAGES:
        raise HTTPException(status_code=400, detail=f"Upload at most {MAX_BULK_IMAGES} images at once.")

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

    created = []
    errors = []

    for index, image in enumerate(images):
        finish_name = (
            parsed_names[index]
            if parsed_names
            else _name_from_filename(image.filename)
        )

        try:
            content = await read_upload_limited(image, MAX_IMAGE_BYTES, f"Image {index + 1}")
            finish = await _persist_finish(
                db=db,
                category=category,
                name=finish_name,
                image_content=content,
                filename=image.filename,
                content_type=image.content_type,
            )
            created.append(serialize_finish(finish))
        except (HTTPException, InputValidationError) as exc:
            errors.append(
                {
                    "filename": image.filename,
                    "name": finish_name,
                    "status_code": getattr(exc, "status_code", 413),
                    "detail": getattr(exc, "detail", str(exc)),
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
    category: FinishCategory | None = None,
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    rate_limit: None = Depends(_list_rate_limit),
):
    query = db.query(Finish).filter(
        Finish.is_active.is_(True)
    )

    if category:
        query = query.filter(
            Finish.category == category.strip().lower()
        )

    # Bounded by default: the catalog is small today, but an unbounded list
    # endpoint signs a presigned URL per row and grows with the catalog.
    finishes = (
        query.order_by(Finish.category.asc(), Finish.name.asc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    return [
        serialize_finish(finish)
        for finish in finishes
    ]


@router.get("/{finish_id}")
def get_finish(
    finish_id: int,
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user),
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
    user: str = Depends(get_current_user),
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
    clear_finish_thumbnail_cache()

    # The row is already gone, so a failed S3 delete must not surface as an
    # error: the client would retry and get a 404 while the objects still leak.
    # Report it instead, and let the operator sweep orphans.
    orphaned = []
    for key in (master_key, thumb_key):
        try:
            delete_s3_file(key)
        except Exception:
            logger.exception("Finish %s deleted, but S3 cleanup failed for %s", finish_id, key)
            orphaned.append(key)

    response = {"message": "Finish deleted successfully."}
    if orphaned:
        response["s3_cleanup_failed"] = orphaned
    return response


@router.get("/connection/test")
def test_connection(user: str = Depends(get_current_user)):
    return test_s3_connection()


@router.post("/connection/write-test")
def test_write_connection(user: str = Depends(get_current_user)):
    return test_s3_write()
