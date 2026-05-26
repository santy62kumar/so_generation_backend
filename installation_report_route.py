"""
installation_report_route.py
-----------------------------
Drop this file into your project and add the route to main.py.

Add to main.py imports:
    from installation_report_route import router as installation_router

Add to main.py app setup (after app = FastAPI()):
    app.include_router(installation_router)
"""

import io
import json
import re
from typing import List, Optional

from fastapi import APIRouter, File, Form, UploadFile
from fastapi.responses import StreamingResponse

from installation_report_generator import generate_installation_report_sync

router = APIRouter()


@router.post("/generate-installation-report")
async def generate_installation_report_route(
    # ── Project Information ──────────────────────────────────────────────────
    projectName:       str = Form(default=""),
    reportDate:        str = Form(default=""),   # "DD/MM/YYYY"; auto-fills today if blank
    projectSupervisor: str = Form(default=""),
    projectManager:    str = Form(default=""),
    projectDesigner:   str = Form(default=""),

    # ── Key Accomplishments ──────────────────────────────────────────────────
    # Pass as JSON string: '["Done X", "Done Y"]'
    accomplishments: str = Form(default="[]"),

    # ── Completed Work ───────────────────────────────────────────────────────
    # Pass as JSON string:
    # '[{"actionItem":"X","date":"25/05/2026","challengesFaced":"None"}]'
    completedWork: str = Form(default="[]"),

    # ── Man Power ────────────────────────────────────────────────────────────
    numIPs:         str = Form(default=""),
    numHelpers:     str = Form(default=""),
    numLabour:      str = Form(default=""),
    ipInTime:       str = Form(default=""),
    ipOutTime:      str = Form(default=""),
    helperInTime:   str = Form(default=""),
    helperOutTime:  str = Form(default=""),
    labourInTime:   str = Form(default=""),
    labourOutTime:  str = Form(default=""),
    mandays:        str = Form(default=""),

    # ── Upcoming Work ────────────────────────────────────────────────────────
    # Pass as JSON string:
    # '[{"actionItem":"Y","date":"26/05/2026","potentialIssues":"Rain"}]'
    upcomingWork: str = Form(default="[]"),

    # ── Photos ───────────────────────────────────────────────────────────────
    # Any number of photo files; no upper limit.
    photos: List[UploadFile] = File(default=[]),
):
    """
    Generate a Daily Installation Report PDF.

    All text fields are optional — only filled values appear in the report.
    Upload any number of photos; they are laid out 3-per-page after the form.

    Returns the PDF as an inline/download stream.
    """

    # Parse JSON list fields (safe-fallback to empty list on bad JSON)
    def _parse_list(raw: str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    # Read all photo bytes in parallel
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
        "accomplishments":   _parse_list(accomplishments),
        "completedWork":     _parse_list(completedWork),
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
        "upcomingWork": _parse_list(upcomingWork),
        "photos":       photo_data,
    }

    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    loop = asyncio.get_event_loop()
    pdf_buffer = await loop.run_in_executor(
        None,
        generate_installation_report_sync,
        data,
    )

    safe_name = re.sub(r"[^a-zA-Z0-9 ]", "", projectName).replace(" ", "_") or "Report"

    return StreamingResponse(
        io.BytesIO(pdf_buffer),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="Installation_Report_{safe_name}.pdf"'
        },
    )