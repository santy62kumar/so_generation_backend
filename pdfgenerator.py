"""
pdfgenerator.py
---------------
Python equivalent of pdfGenerator.js.

Dependencies:
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

from s3_service import get_finish_thumbnail

# ── Slide dimensions (match design canvas exactly) ────────────────────────────
SLIDE_W = 1456  # px
SLIDE_H = 816   # px

# ── Asset directory (static slide backgrounds only — 2.jpg, 3.jpg, etc.) ──────
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


def _fetch_finish_swatch_uri(composite_value: Optional[str]) -> str:
    """
    Fetch ONE color-finish thumbnail straight from AWS S3, given a
    composite value like "cabinet:abyss-edge" or "glass:clear-glass" (the
    same string the frontend's ColorSwatchSelect stores).

    Intentionally per-swatch and on demand — with 70-80 total catalog
    images, generating a PDF should only ever pull the ~7 objects a given
    order actually selected, never the whole catalog.
    """
    if not composite_value or ":" not in composite_value:
        return ""
    category, color_id = composite_value.split(":", 1)
    try:
        image_bytes = get_finish_thumbnail(category, color_id)
    except Exception as exc:
        print(f"⚠️  Could not fetch finish swatch from S3 for {composite_value}: {exc}")
        return ""
    return to_data_uri(image_bytes, "image/webp")


def prefetch_finish_swatches(kitchen_finish_colors: dict) -> dict:
    """
    Resolve every selected "category:id" value in kitchenFinishColors to
    its swatch image (as a data URI), fetching each one from S3 exactly
    once. Called before rendering so the slide-building step is a plain
    dict lookup — no network calls inside page-render code.
    """
    swatches: dict = {}
    for composite_value in (kitchen_finish_colors or {}).values():
        if composite_value and composite_value not in swatches:
            swatches[composite_value] = _fetch_finish_swatch_uri(composite_value)
    return swatches


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


# Full, fixed left-to-right order of the 8 possible finish categories. The
# user may select any subset of these (min 2, max 8) — whichever keys have
# a truthy value in kitchenFinishColors are the ones that get a slide.
KITCHEN_FINISH_ITEMS = [
    ("lowerCabinet",  "Lower Cabinet"),
    ("upperCabinet",  "Upper Cabinet"),
    ("loftUnit",      "Loft Unit"),
    ("glassColor",    "Glass Color"),
    ("golaColor",     "Gola Color"),
    ("skirtingColor", "Skirting Color"),
    ("openShelf",     "Open Shelf"),
    ("tallTower",     "Tall Tower"),
    ("handleProfile", "Handle Profile"),
]


def _selected_finish_items(d: dict) -> list[tuple[str, str]]:
    """
    Returns the (key, shortLabel) pairs, in fixed catalog order, for
    whichever finish categories the user actually selected a color for.
    Empty/missing keys are dropped rather than padded — the artwork asset
    itself changes to match this count (Finishes_image_<n>.jpg).
    """
    colors: dict = d.get("kitchenFinishColors") or {}
    return [(k, label) for k, label in KITCHEN_FINISH_ITEMS if colors.get(k)]


def kitchen_finish_asset(d: dict) -> Optional[str]:
    """
    Picks which background asset to use for the 'Kitchen Color Finishes
    Used' slide, based on how many finish colors were actually selected.
    Returns None (slide skipped entirely) if there's no reference image or
    fewer than 2 colors were selected — mirrors the 2-8 rule enforced by
    the API route.

    Assets live at assets/Finishes_image_<n>.jpg for n in 2..8 (same
    naming convention the frontend uses for its slide-backgrounds preview
    folder), one purpose-designed background per possible swatch count.
    """
    if not d.get("kitchenFinishImage"):
        return None
    n = len(_selected_finish_items(d))
    if n < 2 or n > 8:
        return None
    return f"Finishes_image_{n}.jpg"


# Reference-photo placeholder frame baked into every Finishes_image_<n>.jpg
# variant. Measured directly off all 7 real template assets (n=2..8) — the
# box is pixel-identical across every one of them (confirmed, not assumed):
# only the swatch row below it changes between variants.
REF_IMG_BOX = {"top": 93.7, "left": 297.3, "width": 860.7, "height": 419.3}

# Swatch-row geometry — measured directly off all 7 real Finishes_image_2.jpg
# through Finishes_image_8.jpg templates (connected-component analysis on
# each artwork's own untouched placeholder circles, not estimated).
#
# Turns out the row doesn't need per-count calibration at all: circle size,
# vertical position, and column spacing are IDENTICAL across every count —
# only the row's horizontal centering shifts, because the whole row of n
# circles stays centered on the slide as n changes. So a single formula
# covers all counts 2-8 exactly (max deviation from measured centers across
# all 7 templates was ~6px on a 1456px-wide slide).
CIRCLE_SIZE = 90.9
CIRCLE_CENTER_Y = 657.5
COL_SPACING = 177.7

# Dark border drawn around every finish swatch circle. Matches the deep-taupe
# brand color used elsewhere in the app (KitchenForm.jsx's rgba(58,26,26,...)).
# Tweak here to change color/thickness for every swatch at once.
CIRCLE_BORDER_COLOR = "#3A1A1A"
CIRCLE_BORDER_WIDTH = 3

# Some catalog images (especially Gola and Skirting) contain extra empty
# space around the actual finish sphere. Enlarge only those two swatches so
# the visible finish fills the placeholder circle properly.
SWATCH_BACKGROUND_SIZE = {
    "golaColor": "170%",
    "skirtingColor": "170%",
    "handleProfile": "170%",
}
DEFAULT_SWATCH_BACKGROUND_SIZE = "cover"


def _finish_row_geometry(n: int) -> dict:
    """Row of n circles, evenly spaced at COL_SPACING, centered on the slide."""
    first_cx = (SLIDE_W / 2 - COL_SPACING * (n - 1) / 2)+2
    return {
        "circle_size": CIRCLE_SIZE,
        "center_y": CIRCLE_CENTER_Y,
        "first_center_x": first_cx,
        "col_spacing": COL_SPACING,
    }


def _build_kitchen_finish_slide(d: dict) -> str:
    """
    'Kitchen Color Finishes Used' slide. Background is whichever
    Finishes_image_<n>.jpg asset matches the number of colors selected
    (logo, "RENDERS" header, reference-photo frame, and the
    "KITCHEN COLOR FINISHES USED" title are baked into every variant) —
    this function overlays the dynamic parts on top of it:
        1. the reference photo, into the empty photo frame
        2. one swatch image per selected finish, into the circle row
        3. the category name + selected finish name, printed under each swatch

    Only the categories the user actually picked a color for are rendered,
    in the artwork's fixed left-to-right catalog order (Lower Cabinet,
    Upper Cabinet, Loft Unit, Glass Color, Gola Color, Skirting Color,
    Open Shelf, Tall Tower) — never padded out to 8.

    Expects:
        d['kitchenFinishImage']       -> bytes
        d['kitchenFinishColors']      -> dict of composite "category:id"
                                          strings for whichever keys the
                                          user selected (2-8 of them)
        d['kitchenFinishColorNames']  -> dict, same keys, -> display name
                                          of the finish (e.g. "Abyss Edge")
        d['kitchenFinishSwatches']    -> dict of composite value -> swatch
                                          data URI, already fetched from S3
                                          by prefetch_finish_swatches()
    """
    swatches: dict = d.get("kitchenFinishSwatches") or {}
    colors: dict = d.get("kitchenFinishColors") or {}
    names: dict = d.get("kitchenFinishColorNames") or {}
    selected = _selected_finish_items(d)
    n = len(selected)

    if n < 2:
        return ""

    geo = _finish_row_geometry(n)
    circle_size = geo["circle_size"]
    center_y = geo["center_y"]
    first_cx = geo["first_center_x"]
    col_spacing = geo["col_spacing"]

    html = ""

    ref_img = d.get("kitchenFinishImage")
    if ref_img:
        b = REF_IMG_BOX
        html += f"""
    <img src="{to_data_uri(ref_img)}" style="
      position:absolute; top:{b['top']}px; left:{b['left']}px;
      width:{b['width']}px; height:{b['height']}px;
      object-fit:cover; border-radius:6px;
    "/>
    """

    for i, (key, short_label) in enumerate(selected):
        value = colors.get(key)
        swatch_uri = swatches.get(value, "")
        finish_name = names.get(key, "")
        cx = first_cx + col_spacing * i

        if swatch_uri:
            background_size = SWATCH_BACKGROUND_SIZE.get(
                key, DEFAULT_SWATCH_BACKGROUND_SIZE
            )
            bg = (
                f"background-image:url('{swatch_uri}'); "
                f"background-size:{background_size}; "
                "background-position:center; "
                "background-repeat:no-repeat;"
            )
        else:
            # Selected but the swatch failed to resolve from S3 — leave a
            # plain filled circle rather than a broken/blank one.
            bg = "background:#e5ddd3;"

        html += f"""
    <div style="position:absolute; top:{center_y - circle_size / 2}px; left:{cx - circle_size / 2}px;
      width:{circle_size}px; height:{circle_size}px; border-radius:50%; box-sizing:border-box;
      border:{CIRCLE_BORDER_WIDTH}px solid {CIRCLE_BORDER_COLOR}; {bg}"></div>

    <div style="position:absolute; top:{center_y + circle_size / 2 + 10}px;
      left:{cx - col_spacing / 2}px; width:{col_spacing}px;
      text-align:center; font-family:'Nunito Sans','Montserrat',sans-serif;
      color:#fff; line-height:1.35;">
      <div style="font-size:13px; font-weight:700; text-transform:uppercase; letter-spacing:0.3px;">{short_label}</div>
      <div style="font-size:13px; font-weight:400;">{finish_name}</div>
    </div>
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
    {"asset": "4.jpg",  "build": lambda d: ""},
    {"asset": "5.jpg",  "build": lambda d: ""},

    # Slide 5 – Layout plan (dynamic grid)
    {"asset": "6.jpg",  "build": _build_slide_6},

    # Slide 6 – Kitchen Color Finishes Used. Optional: only appears if a
    # reference image was uploaded and 2-8 finish colors were selected.
    # This slide now appears immediately after the Layout Plan and before
    # all render-image slides.
    # Background asset is picked dynamically — assets/Finishes_image_<n>.jpg
    # for n = number of colors selected (2..8), matching the frontend's
    # slide-backgrounds/Finishes_image_<n>.jpg preview files.
    {"asset": kitchen_finish_asset, "build": _build_kitchen_finish_slide},

    # Slides 7-10 – Render images (1 per page, skipped if not uploaded)
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
          renderImage0..3 (bytes | None — at least one is required),
          kitchenFinishImage (bytes — required),
          kitchenFinishColors (dict[str, str] — 2-8 of the 8
              possible keys, each a "category:id" composite string, e.g.
              {"upperCabinet": "cabinet:abyss-edge", ...}),
          kitchenFinishColorNames (dict[str, str], optional — same keys,
              -> display name of the finish, e.g. "Abyss Edge"),
          relationName, relationPhone, relationEmail,
          designerName,  designerPhone,  designerEmail

        NOTE: Kitchen Finish image is mandatory, 2-8 finish colors must be
        selected, and at least one of renderImage0..3 must be present.
        These rules are enforced here even if the frontend is bypassed.
    """
    print(f"🚀 Generating PDF for: {data['customerName']}")

    # ── Mandatory upload validation ───────────────────────────────────────────
    # Backend validation is required even though the frontend performs the
    # same checks, because API requests can bypass the browser form.
    if not data.get("kitchenFinishImage"):
        raise ValueError("Kitchen Finish image is required.")

    selected_finish_items = _selected_finish_items(data)
    selected_finish_count = len(selected_finish_items)

    if selected_finish_count < 2:
        raise ValueError("Select at least 2 kitchen finish colors.")

    if selected_finish_count > 8:
        raise ValueError("You can select at most 8 kitchen finish colors.")

    render_keys = (
        "renderImage0",
        "renderImage1",
        "renderImage2",
        "renderImage3",
    )
    if not any(data.get(key) for key in render_keys):
        raise ValueError("At least 1 render image is required.")

    # Pull the selected finish swatches from AWS S3 exactly once, up front.
    # boto3 is blocking, so this runs off the event loop; everything below
    # (slide building) then just reads the resulting dict.
    kitchen_finish_colors = data.get("kitchenFinishColors") or {}
    data["kitchenFinishSwatches"] = await asyncio.get_event_loop().run_in_executor(
        None, prefetch_finish_swatches, kitchen_finish_colors
    )

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
