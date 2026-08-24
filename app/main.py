import asyncio
import io
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from .db.database import Base, engine, get_db
from .services.sogeneration import handle_process_xlsx
from .services.odoo import get_warranty_details_by_so
from .generators.pdfgenerator import generate_pdf_sync
from .generators.warrantygenerator import generate_warranty_pdf_sync
from .generators.installation_report_generator import generate_installation_report_sync
from .core.rate_limit import SlidingWindowLimiter, env_int
from .core.input_validation import (
    InputValidationError,
    MIB,
    MAX_IMAGE_BYTES,
    MAX_REPORT_IMAGE_BYTES,
    MAX_REPORT_PHOTOS,
    RequestSizeLimitMiddleware,
    bounded_text,
    parse_json_list,
    parse_json_object,
    read_upload_limited,
    validate_image,
    validate_nonnegative_number,
)

from .generators import web_fonts
from .api.auth import auth_router
from .api.db_admin import router as db_admin_router
from .api.finish_routes import router as finish_router

# Imported for their side effect of registering tables on Base before create_all.
from .db import finish_models, models  # noqa: F401

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

Base.metadata.create_all(bind=engine)

# create_all() skips tables that already exist, so the lookup indexes every SO
# row depends on never appear on an already-populated database. This DDL is
# idempotent and costs milliseconds on tables this size.
_BOOTSTRAP_DDL = (
    "CREATE INDEX IF NOT EXISTS ix_cabinets_cabinet_code ON cabinets (cabinet_code)",
    "CREATE INDEX IF NOT EXISTS ix_colorcode_colour_name ON colorcode (colour_name)",
)
if os.getenv("SKIP_DB_BOOTSTRAP", "").lower() not in {"1", "true", "yes"}:
    with engine.begin() as connection:
        for statement in _BOOTSTRAP_DDL:
            connection.execute(text(statement))

app = FastAPI(title="SO Generator API")

# Read the vendored font files once at boot rather than on the first render.
web_fonts.warm()

try:
    max_concurrent_generations = int(os.getenv("MAX_CONCURRENT_GENERATIONS", "2"))
except ValueError as exc:
    raise RuntimeError("MAX_CONCURRENT_GENERATIONS must be a whole number.") from exc
if not 1 <= max_concurrent_generations <= 16:
    raise RuntimeError("MAX_CONCURRENT_GENERATIONS must be between 1 and 16.")

_thread_pool = ThreadPoolExecutor(max_workers=max_concurrent_generations)
# ponytail: per-process render slots; use a shared queue only when generation moves across workers.
_generation_slots = asyncio.Semaphore(max_concurrent_generations)

# The generation routes are unauthenticated, and each request costs a Chromium
# render or a full workbook parse. The slot semaphore caps concurrency but not
# arrival rate, so without this a loop keeps both slots permanently busy and
# every real user sees 503.
_generation_limiter = SlidingWindowLimiter(
    "generation",
    env_int("GENERATION_RATE_LIMIT", 30),
    env_int("GENERATION_RATE_WINDOW", 60),
)
_generation_rate_limit = _generation_limiter.dependency()

origins = os.getenv("ALLOWED_ORIGINS", "*")
origins_list = [o.strip() for o in origins.split(",") if o.strip()]
if not origins_list:
    raise RuntimeError("ALLOWED_ORIGINS must contain at least one origin.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins_list,
    allow_credentials=origins_list != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=600,
)
app.add_middleware(
    RequestSizeLimitMiddleware,
    limits={
        "/process-xlsx": 21 * MIB,
        "/generate-pdf": 140 * MIB,
        "/generate-warranty": 64 * 1024,
        "/generate-installation-report": 202 * MIB,
        "/api/finishes/bulk": 305 * MIB,
        "/api/finishes": 16 * MIB,
        "/auth/login": 16 * 1024,
        "/db": 2 * MIB,
    },
    default_limit=2 * MIB,
)

app.include_router(auth_router)
app.include_router(db_admin_router)

app.include_router(finish_router)


async def _generation_slot():
    try:
        await asyncio.wait_for(_generation_slots.acquire(), timeout=2)
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=503, detail="The document generator is busy. Try again shortly.") from exc
    try:
        yield
    finally:
        _generation_slots.release()


async def _read_image(upload: UploadFile, field: str, max_bytes: int = MAX_IMAGE_BYTES) -> bytes:
    try:
        content = await read_upload_limited(upload, max_bytes, field)
        validate_image(content, upload.content_type, field)
        return content
    except InputValidationError as exc:
        status = 413 if "exceed" in str(exc) else 400
        raise HTTPException(status_code=status, detail=str(exc)) from exc


def _validate_work_rows(raw: str, field: str, extra_key: str) -> list[dict]:
    rows = parse_json_list(raw, field)
    allowed = {"actionItem", "date", extra_key}
    result = []
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict) or set(row) - allowed:
            raise InputValidationError(f"{field} item {index} has an invalid shape.")
        result.append({
            "actionItem": bounded_text(row.get("actionItem", ""), f"{field} item {index} action", 500, required=True),
            "date": bounded_text(row.get("date", ""), f"{field} item {index} date", 30),
            extra_key: bounded_text(row.get(extra_key, ""), f"{field} item {index} {extra_key}", 500),
        })
    return result


# ── Route 1: SO Generation ─────────────────────────────────────────────────────

@app.get("/health")
def health(db: Session = Depends(get_db)):
    """Liveness + database reachability, for the load balancer."""
    db.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.post("/process-xlsx")
async def process_xlsx(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    generation_slot: None = Depends(_generation_slot),
    rate_limit: None = Depends(_generation_rate_limit),
):
    return await handle_process_xlsx(file, db)


# ── Route 2: PDF Generation ────────────────────────────────────────────────────

@app.post("/generate-pdf")
async def generate_pdf_route(
    title: str         = Form(...),
    customerName: str  = Form(...),
    city: str          = Form(...),
    relationName: str  = Form(default=""),
    relationPhone: str = Form(default=""),
    relationEmail: str = Form(default=""),
    designerName: str  = Form(default=""),
    designerPhone: str = Form(default=""),
    designerEmail: str = Form(default=""),
    layoutImage: List[UploadFile]      = File(default=[]),
    renderImage0: Optional[UploadFile] = File(default=None),
    renderImage1: Optional[UploadFile] = File(default=None),
    renderImage2: Optional[UploadFile] = File(default=None),
    renderImage3: Optional[UploadFile] = File(default=None),
    kitchenFinishImage: UploadFile = File(...),
    kitchenFinishColors: str                 = Form(default="{}"),
    # Display names for whichever colors were selected, keyed the same as
    # kitchenFinishColors (e.g. {"upperCabinet": "Abyss Edge"}). Sent
    # alongside the composite "category:id" values so the PDF can print the
    # finish name under each swatch without a DB round-trip.
    kitchenFinishColorNames: str              = Form(default="{}"),
    generation_slot: None = Depends(_generation_slot),
    rate_limit: None = Depends(_generation_rate_limit),
):
    if len(layoutImage) > 4:
        raise HTTPException(status_code=400, detail="Upload at most 4 layout images.")

    try:
        title = bounded_text(title, "Title", 20, required=True)
        customerName = bounded_text(customerName, "Customer name", 200, required=True)
        city = bounded_text(city, "City", 50, required=True)
        if title not in {"MR.", "MRS."}:
            raise InputValidationError("Title must be MR. or MRS.")
        if not re.fullmatch(r"[A-Za-z ]+", city) or not (
            Path(__file__).resolve().parents[1] / "assets" / f"{city.upper()}.jpg"
        ).is_file():
            raise InputValidationError("City is not supported.")
        relationName = bounded_text(relationName, "Relations associate name", 200)
        relationPhone = bounded_text(relationPhone, "Relations associate phone", 50)
        relationEmail = bounded_text(relationEmail, "Relations associate email", 320)
        designerName = bounded_text(designerName, "Designer name", 200)
        designerPhone = bounded_text(designerPhone, "Designer phone", 50)
        designerEmail = bounded_text(designerEmail, "Designer email", 320)
        finish_colors = parse_json_object(kitchenFinishColors, "kitchenFinishColors")
        finish_color_names = parse_json_object(kitchenFinishColorNames, "kitchenFinishColorNames")
    except InputValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    layout_buffers = [await _read_image(f, f"Layout image {i + 1}") for i, f in enumerate(layoutImage)]

    # A reference image and 2-8 selected finish categories are mandatory.
    ALL_FINISH_KEYS = [
        "lowerCabinet", "upperCabinet", "loftUnit", "glassColor",
        "skirtingColor", "openShelf", "tallTower", "handleProfile",
    ]

    allowed_categories = {
        "lowerCabinet": {"cabinet"},
        "upperCabinet": {"cabinet", "glass"},
        "loftUnit": {"cabinet"},
        "glassColor": {"glass"},
        "skirtingColor": {"skirting"},
        "openShelf": {"cabinet"},
        "tallTower": {"cabinet", "glass"},
        "handleProfile": {"gola", "handle-profile"},
    }
    if set(finish_colors) - set(allowed_categories) or set(finish_color_names) - set(allowed_categories):
        raise HTTPException(status_code=400, detail="Kitchen finish payload contains unknown fields.")

    selected_keys = [k for k in ALL_FINISH_KEYS if finish_colors.get(k)]
    if not 2 <= len(selected_keys) <= 8:
        raise HTTPException(status_code=400, detail="Select between 2 and 8 kitchen finish colors.")
    if set(finish_color_names) != set(selected_keys):
        raise HTTPException(status_code=400, detail="Each selected finish must have exactly one display name.")

    for key in selected_keys:
        value = finish_colors[key]
        if not isinstance(value, str) or not re.fullmatch(
            r"[a-z]+(?:-[a-z]+)*:[a-z0-9]+(?:-[a-z0-9]+)*", value
        ):
            raise HTTPException(status_code=400, detail=f"Invalid finish value for {key}.")
        category, _ = value.split(":", 1)
        if category not in allowed_categories[key]:
            raise HTTPException(status_code=400, detail=f"Invalid finish category for {key}.")
        try:
            finish_color_names[key] = bounded_text(
                finish_color_names.get(key, ""), f"Finish name for {key}", 200, required=True
            )
        except InputValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    finish_colors = {k: finish_colors[k] for k in selected_keys}
    finish_color_names = {k: finish_color_names[k] for k in selected_keys}
    kitchen_finish_image_bytes = await _read_image(kitchenFinishImage, "Kitchen Finish image")
    render_uploads = [renderImage0, renderImage1, renderImage2, renderImage3]
    if not any(render_uploads):
        raise HTTPException(status_code=400, detail="Upload at least 1 render image.")
    render_buffers = [
        await _read_image(upload, f"Render image {i + 1}") if upload else None
        for i, upload in enumerate(render_uploads)
    ]

    dynamic_data = {
        "title":        title,
        "customerName": customerName,
        "city":         city,
        "layoutImage":  layout_buffers,
        "renderImage0": render_buffers[0],
        "renderImage1": render_buffers[1],
        "renderImage2": render_buffers[2],
        "renderImage3": render_buffers[3],
        "kitchenFinishImage":      kitchen_finish_image_bytes,
        "kitchenFinishColors":     finish_colors,
        "kitchenFinishColorNames": finish_color_names,
        "relationName":  relationName,
        "relationPhone": relationPhone,
        "relationEmail": relationEmail,
        "designerName":  designerName,
        "designerPhone": designerPhone,
        "designerEmail": designerEmail,
    }

    # Run Playwright in a background thread with its own ProactorEventLoop.
    # This lets uvicorn keep its SelectorEventLoop while Playwright gets
    # the ProactorEventLoop it needs to spawn Chromium on Windows.
    pdf_buffer = await asyncio.get_running_loop().run_in_executor(
        _thread_pool,
        generate_pdf_sync,   # sync function, runs in thread
        dynamic_data,
    )

    safe_name = re.sub(r"[^a-zA-Z0-9 ]", "", customerName).replace(" ", "_")

    return StreamingResponse(
        io.BytesIO(pdf_buffer),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="Modula_Kitchen_{safe_name}.pdf"'},
    )

@app.get("/api/warranty/so")
async def warranty_so_lookup(
    soNumber: str,
    rate_limit: None = Depends(_generation_rate_limit),
):
    try:
        so_number = bounded_text(soNumber, "SO number", 100, required=True)
    except InputValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        details = await asyncio.get_running_loop().run_in_executor(
            _thread_pool,
            get_warranty_details_by_so,
            so_number,
        )
    except Exception as exc:
        logger.exception("Odoo warranty lookup failed for SO %s", so_number)
        raise HTTPException(status_code=502, detail="Could not fetch this SO from Odoo.") from exc

    if details is None:
        raise HTTPException(status_code=404, detail="SO number not found.")
    return details


@app.post("/generate-warranty")
async def generate_warranty_route(
    customerName:    str = Form(...),
    contactNumber:   str = Form(...),
    invoiceNumber:   str = Form(...),
    distributorName: str = Form(default=""),
    address:         str = Form(default=""),
    pinCode:         str = Form(default=""),
    handoverDate:    str = Form(default=""),
    generation_slot: None = Depends(_generation_slot),
    rate_limit: None = Depends(_generation_rate_limit),
):
    """
    Generate a personalised Warranty Handbook PDF.
 
    Form fields
    ───────────
    customerName   – Customer's full name
    contactNumber  – Phone / mobile number
    invoiceNumber  – Customer invoice number
    distributorName – Distributor name
    address        – Installation address
    pinCode        – PIN / postal code
    handoverDate   – Date of handover  (e.g. "12 May 2026")
    """
    try:
        customerName = bounded_text(customerName, "Customer name", 200, required=True)
        contactNumber = bounded_text(contactNumber, "Contact number", 50, required=True)
        invoiceNumber = bounded_text(invoiceNumber, "Invoice number", 100, required=True)
        distributorName = bounded_text(distributorName, "Distributor name", 200)
        address = bounded_text(address, "Address", 1000)
        pinCode = bounded_text(pinCode, "PIN code", 20)
        handoverDate = bounded_text(handoverDate, "Handover date", 50)
    except InputValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    warranty_data = {
        "customerName":  customerName,
        "contactNumber": contactNumber,
        "invoiceNumber": invoiceNumber,
        "distributorName": distributorName,
        "address":       address,
        "pinCode":       pinCode,
        "handoverDate":  handoverDate,
    }
 
    pdf_buffer = await asyncio.get_running_loop().run_in_executor(
        _thread_pool,
        generate_warranty_pdf_sync,
        warranty_data,
    )
 
    safe_name = re.sub(r"[^a-zA-Z0-9 ]", "", customerName).replace(" ", "_") or "Customer"
 
    return StreamingResponse(
        io.BytesIO(pdf_buffer),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="Modula_Warranty_Handbook_{safe_name}.pdf"'
        },
    )



@app.post("/generate-installation-report")
async def generate_installation_report_route(
    # ── Project Information ────────────────────────────────────────────────────
    projectName:       str = Form(...),
    reportDate:        str = Form(default=""),   # "DD/MM/YYYY"; auto-fills today if blank
    projectSupervisor: str = Form(default=""),
    projectManager:    str = Form(default=""),
    projectDesigner:   str = Form(default=""),
 
    # ── Flexible fields ────────────────────────────────────────────────────────
    # accomplishments: plain string  OR  JSON array string — both accepted
    #   e.g.  "All work done"
    #   e.g.  '["Task A","Task B"]'
    accomplishments: str = Form(default=""),
 
    # JSON array; bad JSON is silently ignored → empty list
    completedWork: str = Form(default="[]"),
    upcomingWork:  str = Form(default="[]"),
 
    # ── Man Power ─────────────────────────────────────────────────────────────
    numIPs:        str = Form(default=""),
    numHelpers:    str = Form(default=""),
    numLabour:     str = Form(default=""),
    ipInTime:      str = Form(default=""),
    ipOutTime:     str = Form(default=""),
    helperInTime:  str = Form(default=""),
    helperOutTime: str = Form(default=""),
    labourInTime:  str = Form(default=""),
    labourOutTime: str = Form(default=""),
    mandays:       str = Form(default=""),
 
    # ── Photos — unlimited ─────────────────────────────────────────────────────
    photos: List[UploadFile] = File(default=[]),
    generation_slot: None = Depends(_generation_slot),
    rate_limit: None = Depends(_generation_rate_limit),
):
    """
    Generate a Daily Installation Report PDF.
 
    • Project name is required; other text fields are optional.
    • accomplishments accepts a plain string or a JSON list.
    • Invalid completed/upcoming work JSON is rejected.
    • Up to MAX_REPORT_PHOTOS photos are accepted; each gets its own page.
    """
    if len(photos) > MAX_REPORT_PHOTOS:
        raise HTTPException(status_code=400, detail=f"Upload at most {MAX_REPORT_PHOTOS} photos.")

    try:
        projectName = bounded_text(projectName, "Project name", 200, required=True)
        reportDate = bounded_text(reportDate, "Report date", 30)
        projectSupervisor = bounded_text(projectSupervisor, "Project supervisor", 200)
        projectManager = bounded_text(projectManager, "Project manager", 200)
        projectDesigner = bounded_text(projectDesigner, "Project designer", 200)
        accomplishments = bounded_text(accomplishments, "Accomplishments", 5000)
        if accomplishments.startswith("["):
            accomplishments = [
                bounded_text(item, f"Accomplishments item {index}", 500, required=True)
                for index, item in enumerate(
                    parse_json_list(accomplishments, "accomplishments"), 1
                )
            ]
        completedWork = _validate_work_rows(completedWork, "completedWork", "challengesFaced")
        upcomingWork = _validate_work_rows(upcomingWork, "upcomingWork", "potentialIssues")
        numIPs = validate_nonnegative_number(numIPs, "Number of IPs", integer=True)
        numHelpers = validate_nonnegative_number(numHelpers, "Number of helpers", integer=True)
        numLabour = validate_nonnegative_number(numLabour, "Number of labourers", integer=True)
        mandays = validate_nonnegative_number(mandays, "Mandays")
        ipInTime = bounded_text(ipInTime, "IP in time", 30)
        ipOutTime = bounded_text(ipOutTime, "IP out time", 30)
        helperInTime = bounded_text(helperInTime, "Helper in time", 30)
        helperOutTime = bounded_text(helperOutTime, "Helper out time", 30)
        labourInTime = bounded_text(labourInTime, "Labour in time", 30)
        labourOutTime = bounded_text(labourOutTime, "Labour out time", 30)
    except InputValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    photo_data = []
    for index, photo in enumerate(photos):
        buf = await _read_image(photo, f"Photo {index + 1}", MAX_REPORT_IMAGE_BYTES)
        try:
            filename = bounded_text(photo.filename or "photo.jpg", f"Photo {index + 1} filename", 255)
        except InputValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        photo_data.append({"bytes": buf, "filename": filename})
 
    data = {
        "projectName":       projectName,
        "reportDate":        reportDate,
        "projectSupervisor": projectSupervisor,
        "projectManager":    projectManager,
        "projectDesigner":   projectDesigner,
        # raw strings — _parse_list_field inside the generator handles both formats
        "accomplishments":   accomplishments,
        "completedWork":     completedWork,
        "manpower": {
            "numIPs":         numIPs,
            "numHelpers":     numHelpers,
            "numLabour":      numLabour,
            "ipInTime":       ipInTime,
            "ipOutTime":      ipOutTime,
            "helperInTime":   helperInTime,
            "helperOutTime":  helperOutTime,
            "labourInTime":   labourInTime,
            "labourOutTime":  labourOutTime,
            "mandays":        mandays,
        },
        "upcomingWork": upcomingWork,
        "photos":       photo_data,
    }
 
    pdf_buffer = await asyncio.get_running_loop().run_in_executor(
        _thread_pool, generate_installation_report_sync, data
    )
 
    safe_name = re.sub(r"[^a-zA-Z0-9 ]", "", projectName).replace(" ", "_") or "Report"
    return StreamingResponse(
        io.BytesIO(pdf_buffer),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="Installation_Report_{safe_name}.pdf"'},
    )
 
 
if __name__ == "__main__":
    import uvicorn

    # Each worker is a separate process with its own render slots, browsers,
    # and login-throttle bucket, so N workers means N x MAX_CONCURRENT_GENERATIONS
    # resident Chromiums. Raise this only with the RAM to back it.
    try:
        workers = int(os.getenv("WORKERS", "1"))
    except ValueError as exc:
        raise RuntimeError("WORKERS must be a whole number.") from exc
    if workers < 1:
        raise RuntimeError("WORKERS must be at least 1.")

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False, workers=workers)
