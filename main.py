import io
import json
import re
import asyncio
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from sqlalchemy.orm import Session
from finish_routes import router as finish_router





from database import get_db
from sogeneration import handle_process_xlsx
from pdfgenerator import generate_pdf_sync          # ← sync wrapper, not generate_pdf
from warrantygenerator import generate_warranty_pdf_sync 
from installation_report_generator import generate_installation_report_sync   # ← NEW

from auth import auth_router
from db_admin import router as db_admin_router

import os
from dotenv import load_dotenv

from database import Base, engine
from finish_models import Finish

Base.metadata.create_all(bind=engine)


load_dotenv()

app = FastAPI()

_thread_pool = ThreadPoolExecutor()                  # one shared pool for the app

origins = os.getenv("ALLOWED_ORIGINS", "*")
origins_list = [o.strip() for o in origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=600,
)

app.include_router(auth_router)
app.include_router(db_admin_router)

app.include_router(finish_router)


# ── Route 1: SO Generation ─────────────────────────────────────────────────────

@app.post("/process-xlsx")
async def process_xlsx(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    return await handle_process_xlsx(file, db)


# ── Route 2: PDF Generation ────────────────────────────────────────────────────

@app.post("/generate-pdf")
async def generate_pdf_route(
    title: str         = Form(default="MR/MRS."),
    customerName: str  = Form(default="CLIENT NAME"),
    city: str          = Form(default="CITY"),
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
    kitchenFinishImage: Optional[UploadFile] = File(default=None),
    kitchenFinishColors: str                 = Form(default="{}"),
    # Display names for whichever colors were selected, keyed the same as
    # kitchenFinishColors (e.g. {"upperCabinet": "Abyss Edge"}). Sent
    # alongside the composite "category:id" values so the PDF can print the
    # finish name under each swatch without a DB round-trip.
    kitchenFinishColorNames: str              = Form(default="{}"),
):
    layout_buffers = [await f.read() for f in layoutImage] if layoutImage else []

    async def read_optional(f):
        return await f.read() if f else None

    # ── Kitchen Finish is now OPTIONAL as a whole. The reference image is
    # what unlocks color selection: if no image was uploaded, we ignore any
    # color payload entirely and skip the slide. If an image *was* uploaded,
    # the user must have picked between 2 and 8 of the 8 possible finish
    # categories (any subset — none of the 8 keys are individually
    # required). Enforced here too since the frontend check can be
    # bypassed. ──
    ALL_FINISH_KEYS = [
        "lowerCabinet", "upperCabinet", "loftUnit", "glassColor",
        "golaColor", "skirtingColor", "openShelf", "tallTower",
    ]

    try:
        finish_colors = json.loads(kitchenFinishColors) or {}
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="kitchenFinishColors must be valid JSON.")

    try:
        finish_color_names = json.loads(kitchenFinishColorNames) or {}
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="kitchenFinishColorNames must be valid JSON.")

    kitchen_finish_image_bytes = None

    if kitchenFinishImage is not None:
        kitchen_finish_image_bytes = await kitchenFinishImage.read()
        if not kitchen_finish_image_bytes:
            raise HTTPException(status_code=400, detail="The uploaded kitchen finish image is empty.")

        # Only count real selections against the known 8 keys — stray keys
        # in the JSON payload are ignored rather than trusted.
        selected_keys = [k for k in ALL_FINISH_KEYS if finish_colors.get(k)]

        if len(selected_keys) < 2:
            raise HTTPException(
                status_code=400,
                detail="Select at least 2 kitchen finish colors when uploading a reference image.",
            )
        if len(selected_keys) > 8:
            raise HTTPException(
                status_code=400,
                detail="You can select at most 8 kitchen finish colors.",
            )

        # Keep only the selected keys (and their matching names) going
        # forward, so downstream code never has to re-check truthiness.
        finish_colors = {k: finish_colors[k] for k in selected_keys}
        finish_color_names = {k: finish_color_names.get(k, "") for k in selected_keys}
    else:
        # No reference image → no finish slide at all, regardless of what
        # (if anything) was sent in kitchenFinishColors.
        finish_colors = {}
        finish_color_names = {}

    dynamic_data = {
        "title":        title,
        "customerName": customerName,
        "city":         city,
        "layoutImage":  layout_buffers,
        "renderImage0": await read_optional(renderImage0),
        "renderImage1": await read_optional(renderImage1),
        "renderImage2": await read_optional(renderImage2),
        "renderImage3": await read_optional(renderImage3),
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
    loop = asyncio.get_event_loop()
    pdf_buffer = await loop.run_in_executor(
        _thread_pool,
        generate_pdf_sync,   # sync function, runs in thread
        dynamic_data
    )

    safe_name = re.sub(r"[^a-zA-Z0-9 ]", "", customerName).replace(" ", "_")

    return StreamingResponse(
        io.BytesIO(pdf_buffer),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="Modula_Kitchen_{safe_name}.pdf"'},
    )



@app.post("/generate-warranty")
async def generate_warranty_route(
    customerName:  str = Form(default=""),
    contactNumber: str = Form(default=""),
    orderId:       str = Form(default=""),
    address:       str = Form(default=""),
    pinCode:       str = Form(default=""),
    issuedBy:      str = Form(default=""),
    handoverDate:  str = Form(default=""),
):
    """
    Generate a Warranty Card PDF.
 
    Form fields
    ───────────
    customerName   – Customer's full name
    contactNumber  – Phone / mobile number
    orderId        – Sales / work-order ID
    address        – Installation address
    pinCode        – PIN / postal code
    issuedBy       – Staff member issuing the card
    handoverDate   – Date of handover  (e.g. "12 May 2026")
    """
    warranty_data = {
        "customerName":  customerName,
        "contactNumber": contactNumber,
        "orderId":       orderId,
        "address":       address,
        "pinCode":       pinCode,
        "issuedBy":      issuedBy,
        "handoverDate":  handoverDate,
    }
 
    loop = asyncio.get_event_loop()
    pdf_buffer = await loop.run_in_executor(
        _thread_pool,
        generate_warranty_pdf_sync,
        warranty_data,
    )
 
    safe_name = re.sub(r"[^a-zA-Z0-9 ]", "", customerName).replace(" ", "_") or "Customer"
 
    return StreamingResponse(
        io.BytesIO(pdf_buffer),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="Modula_Warranty_{safe_name}.pdf"'
        },
    )



@app.post("/generate-installation-report")
async def generate_installation_report_route(
    # ── Project Information ────────────────────────────────────────────────────
    projectName:       str = Form(default=""),
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
):
    """
    Generate a Daily Installation Report PDF.
 
    • All fields optional — only non-empty values appear.
    • accomplishments accepts a plain string or a JSON list.
    • upcomingWork with invalid JSON is treated as empty (no crash).
    • Photos unlimited; each gets its own page with the Modula icon top-right.
    """
    photo_data = []
    for f in photos:
        buf = await f.read()
        if buf:
            photo_data.append({"bytes": buf, "filename": f.filename or "photo.jpg"})
 
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
 
    loop = asyncio.get_event_loop()
    pdf_buffer = await loop.run_in_executor(
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
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)