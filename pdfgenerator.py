"""
pdfgenerator.py
---------------
Python equivalent of pdfGenerator.js.

Dependencies:
    pip install playwright pypdf
    playwright install chromium
"""
import asyncio
import concurrent.futures

import asyncio
import base64
import io
from pathlib import Path
from typing import Callable, Optional

from playwright.async_api import async_playwright
from pypdf import PdfReader, PdfWriter

# ── Slide dimensions (match design canvas exactly) ────────────────────────────
SLIDE_W = 1456  # px
SLIDE_H = 816   # px

# ── Asset directory ────────────────────────────────────────────────────────────
ASSETS = Path(__file__).parent / "assets"


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def to_data_uri(buffer: bytes, mime: str = "image/jpeg") -> str:
    """Encode raw bytes as a base-64 data URI."""
    b64 = base64.b64encode(buffer).decode("utf-8")
    return f"data:{mime};base64,{b64}"


def asset_uri(filename: str) -> str:
    """
    Load a file from the assets directory and return a data URI.
    Returns an empty string (no background) if the file doesn't exist.
    """
    file_path = ASSETS / filename
    if not file_path.exists():
        print(f"⚠️  Asset not found: {filename} — slide will render without background")
        return ""

    ext = file_path.suffix.lower()
    mime = (
        "image/svg+xml" if ext == ".svg"
        else "image/jpeg" if ext in (".jpg", ".jpeg")
        else "image/png"
    )
    return to_data_uri(file_path.read_bytes(), mime)


# ─────────────────────────────────────────────────────────────────────────────
#  Slide definitions  (mirrors the SLIDES array in pdfGenerator.js)
# ─────────────────────────────────────────────────────────────────────────────

def _build_slide_1(d: dict) -> str:
    """Slide 1 – Cover: customer name overlay."""
    return f"""
      <div style="
        position:absolute;
        top: 358px; left: 65px;
        font-family: 'Montserrat', Nunito, sans-serif;
        font-size: 50px;
        letter-spacing: 6px;
        color: #D4A96A;
        text-transform: uppercase;
      ">{d['title']} {d['customerName']}</div>
    """


def _build_slide_6(d: dict) -> str:
    """Slide 6 – Layout plan: 1-4 images in a responsive grid."""
    imgs: list[bytes] = d.get("layoutImage") or []
    count = min(len(imgs), 4)

    grid_top, grid_left, grid_w, grid_h = 125, 50, 1130, 640

    def get_positions(n: int) -> list[dict]:
        if n == 1:
            return [{"top": grid_top, "left": grid_left,
                     "w": grid_w, "h": grid_h}]
        if n == 2:
            return [
                {"top": grid_top, "left": grid_left,
                 "w": grid_w / 2, "h": grid_h},
                {"top": grid_top, "left": grid_left + grid_w / 2,
                 "w": grid_w / 2, "h": grid_h},
            ]
        if n == 3:
            return [
                {"top": grid_top,             "left": grid_left,
                 "w": grid_w / 2,             "h": grid_h},
                {"top": grid_top,             "left": grid_left + grid_w / 2,
                 "w": grid_w / 2,             "h": grid_h / 2},
                {"top": grid_top + grid_h / 2, "left": grid_left + grid_w / 2,
                 "w": grid_w / 2,              "h": grid_h / 2},
            ]
        # 4-image 2×2 grid
        return [
            {"top": grid_top,              "left": grid_left,
             "w": grid_w / 2,              "h": grid_h / 2},
            {"top": grid_top,              "left": grid_left + grid_w / 2,
             "w": grid_w / 2,              "h": grid_h / 2},
            {"top": grid_top + grid_h / 2, "left": grid_left,
             "w": grid_w / 2,              "h": grid_h / 2},
            {"top": grid_top + grid_h / 2, "left": grid_left + grid_w / 2,
             "w": grid_w / 2,              "h": grid_h / 2},
        ]

    positions = get_positions(count)
    html = ""
    for i, buf in enumerate(imgs[:4]):
        p = positions[i]
        html += f"""
  <img src="{to_data_uri(buf)}" style="
    position:absolute;
    top:{p['top']}px; left:{p['left']}px;
    width:{p['w']}px; height:{p['h']}px;
    object-fit:fill;
  "/>
"""
    return html


def _make_render_slide(key: str) -> dict:
    """Factory for renderImage0-3 slide definitions."""
    def asset_fn(d: dict) -> Optional[str]:
        return "7.jpg" if d.get(key) else None

    def build_fn(d: dict) -> str:
        buf: Optional[bytes] = d.get(key)
        if buf:
            return f"""
      <img src="{to_data_uri(buf)}" style="
        position: absolute;
        top:  125px;
        left:  55px;
        width: 1130px;
        height: 635px;
        object-fit: fill;
      "/>
    """
        return ""

    return {"asset": asset_fn, "build": build_fn}


def _build_contact(d: dict) -> str:
    """Last slide – contact info for Relations Associate & Designated Designer."""
    return f"""
    <style>
      @font-face {{
        font-family: 'Nunito Sans';
        font-weight: 900;
        src: url('https://fonts.gstatic.com/s/nunitosans/v15/pe0RMImSLYBIv1o4X1M8ccezI7s.ttf') format('truetype');
      }}
      @font-face {{
        font-family: 'Montserrat';
        font-weight: 900;
        src: url('https://fonts.gstatic.com/s/montserrat/v25/JTUHjIg1_i6t8kCHKm4532VJOt5-QNFgpCtr6Hw5aX8.ttf') format('truetype');
      }}
    </style>

    <!-- Relations Associate column -->
    <div style="position:absolute; top:680px; left:345px;
                font-family:'Nunito Sans','Montserrat',sans-serif;
                font-size:20px; color:#fff; line-height:1.3;">
      {d['relationName']}<br/>{d['relationPhone']}<br/>{d['relationEmail']}
    </div>

    <!-- Designated Designer column -->
    <div style="position:absolute; top:680px; left:700px;
                font-family:'Nunito Sans','Montserrat',sans-serif;
                font-size:20px; color:#fff; line-height:1.3;">
      {d['designerName']}<br/>{d['designerPhone']}<br/>{d['designerEmail']}
    </div>
  """


# Complete slide list — mirrors the SLIDES constant in pdfGenerator.js
SLIDES: list[dict] = [
    # Slide 1 – Cover
    {"asset": "2.jpg",  "build": _build_slide_1},

    # Slide 2 – About Us (fully static)
    {"asset": "3.jpg",  "build": lambda d: ""},

    # Slides 3-4 – Static
    {"asset": "4.jpeg",  "build": lambda d: ""},
    {"asset": "5.jpg",  "build": lambda d: ""},

    # Slide 5 – Layout plan (dynamic grid)
    {"asset": "6.jpg",  "build": _build_slide_6},

    # Slides 6-9 – Render images (1 per page, skipped if not uploaded)
    _make_render_slide("renderImage0"),
    _make_render_slide("renderImage1"),
    _make_render_slide("renderImage2"),
    _make_render_slide("renderImage3"),

    # Slide 10 – Static
    {"asset": "8.jpg",  "build": lambda d: ""},

    # Slide 11 – City (asset name derived from data)
    {"asset": lambda d: f"{d['city'].upper()}.jpg", "build": lambda d: ""},

    # Slide 12 – Contact
    {"asset": "10.jpg", "build": _build_contact},
]


# ─────────────────────────────────────────────────────────────────────────────
#  Render a single slide → PDF bytes
# ─────────────────────────────────────────────────────────────────────────────

async def render_slide(page, slide: dict, data: dict) -> Optional[bytes]:
    asset_val = slide["asset"]
    asset_file: Optional[str] = asset_val(data) if callable(asset_val) else asset_val

    overlay_html: str = slide["build"](data)

    # Skip slide if there is no asset AND no overlay content
    if not asset_file and not overlay_html.strip():
        return None

    bg_uri = asset_uri(asset_file) if asset_file else None

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8"/>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@400;600&display=swap" rel="stylesheet">
  <style>
    * {{ margin:0; padding:0; box-sizing:border-box; }}
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
      top:0; left:0;
      width:  100%;
      height: 100%;
      {f"background-image: url('{bg_uri}');" if bg_uri else ""}
      background-size: cover;
      background-position: center;
    }}
  </style>
</head>
<body>
  <div class="slide">
    <!-- ① Design asset — pixel-perfect background -->
    <div class="bg"></div>

    <!-- ② Dynamic overlays — text & images at exact coordinates -->
    {overlay_html}
  </div>
</body>
</html>"""

    await page.set_content(html, wait_until="networkidle", timeout=60000)

    pdf_bytes: bytes = await page.pdf(
        width=f"{SLIDE_W}px",
        height=f"{SLIDE_H}px",
        print_background=True,
        margin={"top": "0px", "right": "0px", "bottom": "0px", "left": "0px"},
    )

    return pdf_bytes


# ─────────────────────────────────────────────────────────────────────────────
#  Main export — generates the complete merged PDF
# ─────────────────────────────────────────────────────────────────────────────

async def generate_pdf(data: dict) -> bytes:
    """
    Render all slides and merge them into a single PDF.

    Parameters
    ----------
    data : dict
        Keys expected:
          title, customerName, city,
          layoutImage (list[bytes]),
          renderImage0..3 (bytes | None),
          relationName, relationPhone, relationEmail,
          designerName,  designerPhone,  designerEmail
    """
    print(f"🚀 Generating PDF for: {data['customerName']}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        try:
            # Use a browser context so we can set device_scale_factor (≡ deviceScaleFactor:2)
            context = await browser.new_context(
                viewport={"width": SLIDE_W, "height": SLIDE_H},
                device_scale_factor=2,
            )
            page = await context.new_page()

            slide_buffers: list[bytes] = []
            for i, slide in enumerate(SLIDES):
                asset_val = slide["asset"]
                asset_file = asset_val(data) if callable(asset_val) else asset_val
                print(f"  Rendering slide {i + 1}/{len(SLIDES)}: {asset_file or 'SKIPPED'}")

                buf = await render_slide(page, slide, data)
                if buf is not None:          # filter skipped slides (same as .filter(Boolean))
                    slide_buffers.append(buf)

        finally:
            await browser.close()

    # ── Merge all slide PDFs into one document ────────────────────────────────
    writer = PdfWriter()
    for buf in slide_buffers:
        reader = PdfReader(io.BytesIO(buf))
        for pdf_page in reader.pages:
            writer.add_page(pdf_page)

    output = io.BytesIO()
    writer.write(output)
    final_pdf = output.getvalue()

    print(f"✅ PDF generated — {len(final_pdf) // 1024} KB")
    return final_pdf



# def generate_pdf_sync(data: dict) -> bytes:
#     """
#     Thread-safe entry point called from uvicorn's route.
#     Spins up a brand-new ProactorEventLoop in a background thread
#     so Playwright can spawn Chromium regardless of uvicorn's own loop.
#     """
#     loop = asyncio.ProactorEventLoop()
#     try:
#         return loop.run_until_complete(generate_pdf(data))
#     finally:
#         loop.close()


def generate_pdf_sync(*args, **kwargs):
    # Works on both Linux and Windows
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(generate_pdf(*args, **kwargs))
    finally:
        loop.close()