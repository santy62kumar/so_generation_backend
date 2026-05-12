"""
warrantygenerator.py
--------------------
Warranty Card PDF generator — mirrors the structure of pdfgenerator.py.

Assets are loaded from  Backend/warranty_assets/
Dynamic slide is 4.jpg  (Customer Name, Contact Number, Order ID,
                          Address, Pin Code, Issued By, Handover Date)

Dependencies (same as pdfgenerator.py):
    pip install playwright pypdf
    playwright install chromium
"""

import asyncio
import base64
import io
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright
from pypdf import PdfReader, PdfWriter

# ── Slide dimensions  (A4 portrait at 96 dpi) ─────────────────────────────────
SLIDE_W = 794
SLIDE_H = 1123

# ── Asset directory ────────────────────────────────────────────────────────────
WARRANTY_ASSETS = Path(__file__).parent / "warranty_assets"


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers  (same API as pdfgenerator.py)
# ─────────────────────────────────────────────────────────────────────────────

def to_data_uri(buffer: bytes, mime: str = "image/jpeg") -> str:
    """Encode raw bytes as a base-64 data URI."""
    b64 = base64.b64encode(buffer).decode("utf-8")
    return f"data:{mime};base64,{b64}"


def asset_uri(filename: str) -> str:
    """
    Load a file from warranty_assets and return a data URI.
    Returns an empty string if the file doesn't exist.
    """
    file_path = WARRANTY_ASSETS / filename
    if not file_path.exists():
        print(f"⚠️  Warranty asset not found: {filename} — slide rendered without background")
        return ""

    ext = file_path.suffix.lower()
    mime = (
        "image/svg+xml" if ext == ".svg"
        else "image/jpeg" if ext in (".jpg", ".jpeg")
        else "image/png"
    )
    return to_data_uri(file_path.read_bytes(), mime)


# ─────────────────────────────────────────────────────────────────────────────
#  Field overlay shared style
# ─────────────────────────────────────────────────────────────────────────────

_FIELD_STYLE = (
    "font-family: 'Nunito Sans', 'Montserrat', sans-serif;"
    "font-size: 16px;"
    "color: #5C3317;"           # warm brown matching the brand colour
    "white-space: nowrap;"
    "overflow: hidden;"
    "text-overflow: ellipsis;"
)

# Vertical positions (top, px) for each field value on 4.jpg.
# Each row is spaced ~60 px apart; left offset sits just after the label text.
# ── Tune these numbers if the real asset has different proportions. ──────────
_FIELDS: list[dict] = [
    {"key": "customerName",   "top": 544, "left": 256, "max_w": 490},
    {"key": "contactNumber",  "top": 610, "left": 260, "max_w": 480},
    {"key": "orderId",        "top": 676, "left": 187, "max_w": 545},
    {"key": "address",        "top": 740, "left": 187, "max_w": 548},
    {"key": "pinCode",        "top": 808, "left": 185, "max_w": 547},
    {"key": "issuedBy",       "top": 875, "left": 201, "max_w": 543},
    {"key": "handoverDate",   "top": 955, "left": 250, "max_w": 514},
]


# ─────────────────────────────────────────────────────────────────────────────
#  Slide builder for slide 4 (the only dynamic slide)
# ─────────────────────────────────────────────────────────────────────────────

def _build_slide_4(d: dict) -> str:
    """
    Overlay the seven form-field values on top of 4.jpg.
    Each value is absolutely positioned to sit on the printed underline.
    """
    html = ""
    for field in _FIELDS:
        value = d.get(field["key"], "")
        html += f"""
  <div style="
    position: absolute;
    top:  {field['top']}px;
    left: {field['left']}px;
    max-width: {field['max_w']}px;
    {_FIELD_STYLE}
  ">{value}</div>
"""
    return html


# ─────────────────────────────────────────────────────────────────────────────
#  Slide definitions  (mirrors the SLIDES list in pdfgenerator.py)
#
#  Each entry is a dict with:
#    "asset"  – filename string (or callable(data)->str|None)
#    "build"  – callable(data) -> HTML overlay string
#
#  Slides present in warranty_assets:
#    1.jpg  2.jpg  3.jpg  4.jpg  5.jpg  6.jpg
#    7.jpg  8.jpg  9.jpg  10.jpg  12.jpg
#  (11.jpg is absent — skipped automatically)
# ─────────────────────────────────────────────────────────────────────────────

SLIDES: list[dict] = [
    {"asset": "1.jpg",  "build": lambda d: ""},   # Cover / intro
    {"asset": "2.jpg",  "build": lambda d: ""},   # Static page
    {"asset": "3.jpg",  "build": lambda d: ""},   # Static page
    {"asset": "4.jpeg",  "build": _build_slide_4}, # ← Dynamic: form fields
    {"asset": "5.jpg",  "build": lambda d: ""},   # Static page
    {"asset": "6.jpg",  "build": lambda d: ""},   # Static page
    {"asset": "7.jpg",  "build": lambda d: ""},   # Static page
    {"asset": "8.jpg",  "build": lambda d: ""},   # Static page
    {"asset": "9.jpg",  "build": lambda d: ""},   # Static page
    {"asset": "10.jpg", "build": lambda d: ""},   # Static page
    # 11.jpg is missing — omitted intentionally
    {"asset": "12.jpg", "build": lambda d: ""},   # Back cover / terms
]


# ─────────────────────────────────────────────────────────────────────────────
#  Render a single slide → PDF bytes   (identical logic to pdfgenerator.py)
# ─────────────────────────────────────────────────────────────────────────────

async def _render_slide(page, slide: dict, data: dict) -> Optional[bytes]:
    asset_val = slide["asset"]
    asset_file: Optional[str] = asset_val(data) if callable(asset_val) else asset_val

    overlay_html: str = slide["build"](data)

    # Skip the slide if there is nothing to render
    if not asset_file and not overlay_html.strip():
        return None

    bg_uri = asset_uri(asset_file) if asset_file else None

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8"/>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Nunito+Sans:wght@400;700;900&family=Montserrat:wght@400;700;900&display=swap"
        rel="stylesheet">
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    html, body {{
      width:  {SLIDE_W}px;
      height: {SLIDE_H}px;
      overflow: hidden;
    }}
    .slide {{
      position: relative;
      width:  {SLIDE_W}px;
      height: {SLIDE_H}px;
    }}
    .bg {{
      position: absolute;
      top: 0; left: 0;
      width: 100%; height: 100%;
      {f"background-image: url('{bg_uri}');" if bg_uri else ""}
      background-size: cover;
      background-position: center;
    }}
  </style>
</head>
<body>
  <div class="slide">
    <div class="bg"></div>
    {overlay_html}
  </div>
</body>
</html>"""

    await page.set_content(html, wait_until="networkidle", timeout=60_000)

    pdf_bytes: bytes = await page.pdf(
        width=f"{SLIDE_W}px",
        height=f"{SLIDE_H}px",
        print_background=True,
        margin={"top": "0px", "right": "0px", "bottom": "0px", "left": "0px"},
    )
    return pdf_bytes


# ─────────────────────────────────────────────────────────────────────────────
#  Public async entry-point
# ─────────────────────────────────────────────────────────────────────────────

async def generate_warranty_pdf(data: dict) -> bytes:
    """
    Render all warranty-card slides and merge them into a single PDF.

    Parameters
    ----------
    data : dict
        Expected keys
        ─────────────
        customerName   : str   – printed name of the customer
        contactNumber  : str   – customer phone / mobile
        orderId        : str   – sales / work-order reference
        address        : str   – installation address
        pinCode        : str   – postal / PIN code
        issuedBy       : str   – staff name who issues the card
        handoverDate   : str   – date of handover (e.g. "12 May 2026")

    Returns
    -------
    bytes – complete merged PDF ready to stream or save.
    """
    print(f"🚀 Generating Warranty Card for: {data.get('customerName', 'UNKNOWN')}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        try:
            context = await browser.new_context(
                viewport={"width": SLIDE_W, "height": SLIDE_H},
                device_scale_factor=2,          # retina-quality render
            )
            page = await context.new_page()

            slide_buffers: list[bytes] = []
            for i, slide in enumerate(SLIDES):
                asset_val = slide["asset"]
                asset_file = asset_val(data) if callable(asset_val) else asset_val
                print(f"  Slide {i + 1}/{len(SLIDES)}: {asset_file or 'SKIPPED'}")

                buf = await _render_slide(page, slide, data)
                if buf is not None:
                    slide_buffers.append(buf)
        finally:
            await browser.close()

    # ── Merge individual slide PDFs ───────────────────────────────────────────
    writer = PdfWriter()
    for buf in slide_buffers:
        reader = PdfReader(io.BytesIO(buf))
        for pdf_page in reader.pages:
            writer.add_page(pdf_page)

    output = io.BytesIO()
    writer.write(output)
    final_pdf = output.getvalue()

    print(f"✅ Warranty PDF ready — {len(final_pdf) // 1024} KB, {len(slide_buffers)} pages")
    return final_pdf


# ─────────────────────────────────────────────────────────────────────────────
#  Sync wrapper  (called from FastAPI route via run_in_executor)
# ─────────────────────────────────────────────────────────────────────────────

def generate_warranty_pdf_sync(*args, **kwargs) -> bytes:
    """Thread-safe entry point — spins up a fresh event loop for Playwright."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(generate_warranty_pdf(*args, **kwargs))
    finally:
        loop.close()