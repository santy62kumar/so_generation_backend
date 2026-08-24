"""
installation_report_generator.py
---------------------------------
Generates a Daily Installation Report PDF using Playwright + pypdf.

Pages
-----
  Page 1  : Report form  (project info, status, manpower, upcoming work)
  Page 2+ : Pictorial attachments — ONE image per page, full-width

Dependencies:
    pip install playwright pypdf pillow
    playwright install chromium
"""

import asyncio
import base64
import io
import json
import logging
from datetime import date
from functools import lru_cache
from pathlib import Path

from PIL import Image
from pypdf import PdfReader, PdfWriter

from .browser_pool import run_render
from ..core.input_validation import escape_html
from .web_fonts import font_css, install_font_routes

logger = logging.getLogger(__name__)

# ── Canvas dimensions ────────────────────────────────────────────────────────
PAGE_W = 1080
PAGE_H = 1527

# ── Brand colours ────────────────────────────────────────────────────────────
BRAND_BROWN = "#7B3F1E"
BRAND_LIGHT = "#F5E6D8"
TEXT_DARK   = "#1A1A1A"


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _b64(buf: bytes, mime: str = "image/jpeg") -> str:
    return f"data:{mime};base64,{base64.b64encode(buf).decode()}"

def _img_mime(filename: str) -> str:
    ext = Path(filename).suffix.lower().lstrip(".")
    return {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "gif": "image/gif",
        "webp": "image/webp",
    }.get(ext, "image/jpeg")

def _esc(val) -> str:
    return escape_html(val)

def _parse_list_field(raw) -> list:
    """
    Accept either:
      - already a list  → return as-is
      - JSON string     → parse and return list
      - plain string    → wrap in single-element list
      - anything else   → return []
    """
    if isinstance(raw, list):
        return raw
    if not raw:
        return []
    if isinstance(raw, str):
        stripped = raw.strip()
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []          # bad JSON → ignore silently
        else:
            return [stripped]      # plain string → single-item list
    return []

def _compress_image(buf: bytes, max_width: int = 1200, quality: int = 75) -> bytes:
    """Resize and compress an image to reduce memory usage during PDF generation."""
    img = Image.open(io.BytesIO(buf))
    if img.width > max_width:
        ratio = max_width / img.width
        img = img.resize((max_width, int(img.height * ratio)), Image.LANCZOS)
    out = io.BytesIO()
    img.convert("RGB").save(out, format="JPEG", quality=quality)
    return out.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
#  Shared logo snippet — inserted into every page
# ─────────────────────────────────────────────────────────────────────────────

LOGO_FILE = Path(__file__).resolve().parents[2] / "assets" / "modula_logo.svg"
# Kept as a fallback only: fetching it per page needed egress from the render
# host and cost a round trip on every photo page.
MODULA_LOGO_URL = "https://www.modula.in/images/modula_jsw.svg"


@lru_cache(maxsize=1)
def _logo_src() -> str:
    """Data URI for the local logo, falling back to the live URL if it's gone."""
    try:
        encoded = base64.b64encode(LOGO_FILE.read_bytes()).decode()
    except OSError:
        logger.warning("Logo not found at %s — falling back to %s", LOGO_FILE, MODULA_LOGO_URL)
        return MODULA_LOGO_URL
    return f"data:image/svg+xml;base64,{encoded}"


def _logo_html(left_content: str) -> str:
    """Flex row: left_content on the left, the Modula logo on the right."""
    return f"""
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:28px;">
      <div>{left_content}</div>
      <img src="{_logo_src()}" style="height:48px; object-fit:contain;" />
    </div>"""


# ─────────────────────────────────────────────────────────────────────────────
#  Shared CSS
# ─────────────────────────────────────────────────────────────────────────────

_BASE_CSS = f"""
* {{ margin:0; padding:0; box-sizing:border-box; }}
html, body {{
  width: {PAGE_W}px;
  font-family: 'Montserrat', 'Segoe UI', Arial, sans-serif;
  color: {TEXT_DARK};
  background: #fff;
}}
.page {{
  width: {PAGE_W}px;
  padding: 48px 52px;
}}
.info-table {{
  width: 100%;
  border-collapse: collapse;
  margin-bottom: 18px;
}}
.info-table td, .info-table th {{
  border: 1px solid #ccc;
  padding: 9px 14px;
  font-size: 15px;
}}
.section-header {{
  background: {BRAND_BROWN};
  color: #fff;
  text-align: center;
  font-weight: 700;
  font-size: 16px;
  letter-spacing: 0.5px;
  padding: 8px;
}}
.sub-header {{
  background: {BRAND_LIGHT};
  font-weight: 600;
  text-align: center;
  font-size: 15px;
  color: {BRAND_BROWN};
}}
.label-cell {{
  width: 28%;
  font-weight: 600;
  background: #fafafa;
  color: #444;
}}
.value-cell {{ width: 72%; }}
.action-row td {{ min-height: 32px; height: 36px; }}
.mp-label   {{ width:22%; font-weight:600; background:#fafafa; }}
.mp-num     {{ width:9%;  text-align:center; }}
.mp-tag     {{ width:10%; text-align:center; background:{BRAND_LIGHT}; font-weight:600; font-size:13px; }}
.mp-time    {{ width:16%; text-align:center; }}
.mp-mandays {{ width:14%; text-align:center; background:{BRAND_LIGHT}; font-weight:600; font-size:13px; }}
"""


# ─────────────────────────────────────────────────────────────────────────────
#  Page builders
# ─────────────────────────────────────────────────────────────────────────────

def _build_report_page(d: dict) -> str:
    """Page 1 — the report form."""

    # Project info
    project_rows = ""
    for label, val in [
        ("Project Name",       d.get("projectName")),
        ("Report Date",        d.get("reportDate") or date.today().strftime("%d/%m/%Y")),
        ("Project Supervisor", d.get("projectSupervisor")),
        ("Project Manager",    d.get("projectManager")),
        ("Project Designer",   d.get("projectDesigner")),
    ]:
        if val:
            project_rows += (
                f'<tr><td class="label-cell">{_esc(label)}</td>'
                f'<td class="value-cell">{_esc(val)}</td></tr>'
            )

    # Accomplishments — accept plain string OR list
    accomplishments = _parse_list_field(d.get("accomplishments"))
    acc_rows = "".join(
        f'<tr class="action-row"><td>{_esc(a)}</td></tr>' for a in accomplishments
    )
    for _ in range(max(0, 3 - len(accomplishments))):
        acc_rows += '<tr class="action-row"><td>&nbsp;</td></tr>'

    # Completed work
    completed_work = _parse_list_field(d.get("completedWork"))
    cw_rows = ""
    for cw in completed_work:
        if isinstance(cw, dict):
            cw_rows += f"""<tr class="action-row">
              <td>{_esc(cw.get('actionItem'))}</td>
              <td style="width:120px;text-align:center">{_esc(cw.get('date'))}</td>
              <td>{_esc(cw.get('challengesFaced'))}</td>
            </tr>"""
        else:
            cw_rows += (
                f'<tr class="action-row"><td>{_esc(cw)}</td>'
                f'<td>&nbsp;</td><td>&nbsp;</td></tr>'
            )
    for _ in range(max(0, 3 - len(completed_work))):
        cw_rows += '<tr class="action-row"><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>'

    # Manpower
    mp          = d.get("manpower") or {}
    num_ips     = _esc(mp.get("numIPs", ""))
    num_helpers = _esc(mp.get("numHelpers", ""))
    num_labour  = _esc(mp.get("numLabour", ""))
    ip_in       = _esc(mp.get("ipInTime", ""))
    ip_out      = _esc(mp.get("ipOutTime", ""))
    h_in        = _esc(mp.get("helperInTime", ""))
    h_out       = _esc(mp.get("helperOutTime", ""))
    l_in        = _esc(mp.get("labourInTime", ""))
    l_out       = _esc(mp.get("labourOutTime", ""))
    mandays     = _esc(mp.get("mandays", ""))

    # Upcoming work
    upcoming = _parse_list_field(d.get("upcomingWork"))
    uw_rows = ""
    for uw in upcoming:
        if isinstance(uw, dict):
            uw_rows += f"""<tr class="action-row">
              <td>{_esc(uw.get('actionItem'))}</td>
              <td style="width:120px;text-align:center">{_esc(uw.get('date'))}</td>
              <td>{_esc(uw.get('potentialIssues'))}</td>
            </tr>"""
        else:
            uw_rows += (
                f'<tr class="action-row"><td>{_esc(uw)}</td>'
                f'<td>&nbsp;</td><td>&nbsp;</td></tr>'
            )
    for _ in range(max(0, 3 - len(upcoming))):
        uw_rows += '<tr class="action-row"><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>'

    logo = _logo_html(
        '<div style="font-size:38px;font-weight:700;letter-spacing:1px;">Daily Installation Report</div>'
    )

    return f"""<!DOCTYPE html>
<html><head>
  <meta charset="UTF-8"/>
  <style>{font_css()}{_BASE_CSS}</style>
</head><body><div class="page">

  {logo}

  <table class="info-table">
    <tr><td colspan="2" class="section-header">Project Information</td></tr>
    {project_rows or '<tr><td colspan="2">&nbsp;</td></tr>'}
  </table>

  <table class="info-table">
    <tr><td class="section-header">Project Status Summary</td></tr>
    <tr><td class="sub-header">Key Accomplishments</td></tr>
    {acc_rows}
  </table>

  <table class="info-table">
    <tr><td colspan="3" class="section-header">Progress Report of the day</td></tr>
    <tr><td colspan="3" class="sub-header">Completed Work</td></tr>
    <tr>
      <th>Action Item</th>
      <th style="width:120px">Date</th>
      <th>Challenges Faced</th>
    </tr>
    {cw_rows}
  </table>

  <table class="info-table">
    <tr>
      <td colspan="6" class="section-header" style="text-align:left;padding-left:16px">
        Man Power Available for the day
      </td>
      <td class="mp-mandays">Mandays</td>
    </tr>
    <tr>
      <td class="mp-label">No. of IPs Present</td>
      <td class="mp-num">{num_ips}</td>
      <td class="mp-tag">In Time</td><td class="mp-time">{ip_in}</td>
      <td class="mp-tag">Out Time</td><td class="mp-time">{ip_out}</td>
      <td rowspan="3" style="text-align:center;vertical-align:middle;font-size:22px;font-weight:700">{mandays}</td>
    </tr>
    <tr>
      <td class="mp-label">No. of Helpers</td>
      <td class="mp-num">{num_helpers}</td>
      <td class="mp-tag">In Time</td><td class="mp-time">{h_in}</td>
      <td class="mp-tag">Out Time</td><td class="mp-time">{h_out}</td>
    </tr>
    <tr>
      <td class="mp-label">No. of Labour</td>
      <td class="mp-num">{num_labour}</td>
      <td class="mp-tag">In Time</td><td class="mp-time">{l_in}</td>
      <td class="mp-tag">Out Time</td><td class="mp-time">{l_out}</td>
    </tr>
  </table>

  <table class="info-table">
    <tr><td colspan="3" class="section-header">Upcoming Work for next day</td></tr>
    <tr>
      <th>Action Item</th>
      <th style="width:120px">Date</th>
      <th>Potential Issues that can hinder</th>
    </tr>
    {uw_rows}
  </table>

</div></body></html>"""


def _build_photo_page(
    image_tuple: tuple[bytes, str],
    page_num: int,
    total_pages: int,
    global_idx: int,
) -> str:
    """
    Single photo page — one image, full width, with caption.
    """
    buf, fname = image_tuple
    mime = _img_mime(fname)
    uri  = _b64(buf, mime)

    logo = _logo_html(
        f'<div style="font-size:22px;font-weight:700;color:{BRAND_BROWN};">'
        f'Pictorial Attachment of Daily Progress Report</div>'
    )

    return f"""<!DOCTYPE html>
<html><head>
  <meta charset="UTF-8"/>
  <style>{font_css()}{_BASE_CSS}</style>
</head><body>
<div class="page">
  {logo}
  <div style="font-size:13px;color:#aaa;margin-bottom:20px;">
    Page {page_num} of {total_pages}
  </div>
  <div style="
    border: 1px solid #ddd;
    border-radius: 6px;
    overflow: hidden;
    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
  ">
    <img src="{uri}" style="
      display:block;
      width: 100%;
      max-height: 1200px;
      object-fit: contain;
      background: #f8f8f8;
    "/>
    <div style="
      padding: 6px 12px;
      font-size: 13px;
      color: #888;
      background: #fafafa;
      border-top: 1px solid #eee;
    ">Photo {global_idx} — {_esc(fname)}</div>
  </div>
</div>
</body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
#  Render helper
# ─────────────────────────────────────────────────────────────────────────────

async def _html_to_pdf(page, html: str) -> bytes:
    await page.set_content(html, wait_until="load", timeout=60_000)
    # The logo is a data URI and the fonts are served locally, so instead of
    # waiting on the network settling, wait on the only thing that matters:
    # the faces being ready, or text measures against a fallback font.
    await page.evaluate("() => document.fonts.ready")

    # Measure exact rendered height to avoid trailing blank pages
    content_h = await page.evaluate(
        "() => document.documentElement.scrollHeight"
    )
    render_h = max(content_h, 400)

    return await page.pdf(
        width=f"{PAGE_W}px",
        height=f"{render_h}px",
        print_background=True,
        margin={"top": "0px", "right": "0px", "bottom": "0px", "left": "0px"},
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Public async entry-point
# ─────────────────────────────────────────────────────────────────────────────

async def generate_installation_report(data: dict, browser) -> bytes:
    """
    Generate a Daily Installation Report PDF.

    data keys
    ---------
    projectName, reportDate, projectSupervisor, projectManager, projectDesigner
    accomplishments  : str  OR  list[str]
    completedWork    : list[{actionItem, date, challengesFaced}]
    manpower         : {numIPs, numHelpers, numLabour, ipInTime, ipOutTime,
                        helperInTime, helperOutTime, labourInTime, labourOutTime, mandays}
    upcomingWork     : list[{actionItem, date, potentialIssues}]  (bad JSON → [])
    photos           : list[{bytes, filename}]  — unlimited; 1 photo per page
    """
    project = data.get("projectName", "Report")
    logger.info("Generating Installation Report for: %s", project)

    photos: list[dict] = data.get("photos") or []
    total_pages = 1 + len(photos)  # page 1 = form, then 1 page per photo

    context = await browser.new_context(
        viewport={"width": PAGE_W, "height": PAGE_H},
    )
    try:
        await install_font_routes(context)
        pg = await context.new_page()

        slide_buffers: list[bytes] = []

        # Page 1 — report form
        logger.info("Rendering page 1: Report form")
        slide_buffers.append(await _html_to_pdf(pg, _build_report_page(data)))

        # Pages 2+ — one photo per page
        for idx, photo_dict in enumerate(photos):
            page_num   = idx + 2
            global_idx = idx + 1
            logger.info(
                "Rendering page %s: photo %s (%s)",
                page_num, global_idx, photo_dict.get("filename", "photo.jpg"),
            )

            # Pillow re-encode is CPU-bound; keep it off the render loop.
            compressed = await asyncio.to_thread(_compress_image, photo_dict["bytes"])
            image_tuple = (compressed, photo_dict.get("filename", "photo.jpg"))

            slide_buffers.append(
                await _html_to_pdf(
                    pg,
                    _build_photo_page(image_tuple, page_num, total_pages, global_idx),
                )
            )
    finally:
        await context.close()

    # Merge all pages into one PDF
    writer = PdfWriter()
    for buf in slide_buffers:
        reader = PdfReader(io.BytesIO(buf))
        for pdf_page in reader.pages:
            writer.add_page(pdf_page)

    out = io.BytesIO()
    writer.write(out)
    final_pdf = out.getvalue()
    logger.info("PDF ready — %s KB, %s page(s)", len(final_pdf) // 1024, len(slide_buffers))
    return final_pdf


# ─────────────────────────────────────────────────────────────────────────────
#  Sync wrapper
# ─────────────────────────────────────────────────────────────────────────────

def generate_installation_report_sync(data: dict) -> bytes:
    """Thread-safe entry point called from the route's thread pool."""
    return run_render(lambda browser: generate_installation_report(data, browser))
