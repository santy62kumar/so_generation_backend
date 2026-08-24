"""Generate a personalised copy of the approved Modula warranty handbook."""

import io
import logging
from pathlib import Path

from pypdf import PdfReader, PdfWriter

from ..core.input_validation import escape_html
from .browser_pool import run_render
from .web_fonts import font_css, install_font_routes

logger = logging.getLogger(__name__)

PAGE_W = 557
PAGE_H = 797
WARRANTY_TEMPLATE = (
    Path(__file__).resolve().parents[2] / "warranty_assets" / "warranty_handbook.pdf"
)

_FIELD_STYLE = (
    "font-family:'Nunito Sans','Montserrat',sans-serif;"
    "font-size:13px;"
    "font-weight:600;"
    "line-height:1;"
    "color:#4A231C;"
    "white-space:nowrap;"
    "overflow:hidden;"
    "text-overflow:ellipsis;"
)

# Positions follow the writing lines on page 4 of warranty_handbook.pdf.
_FIELDS = [
    {"key": "customerName",    "top": 372, "left": 184, "max_w": 300},
    {"key": "contactNumber",   "top": 419, "left": 184, "max_w": 300},
    {"key": "invoiceNumber",   "top": 465, "left": 184, "max_w": 300},
    {"key": "distributorName", "top": 510, "left": 196, "max_w": 288},
    {"key": "address",         "top": 554, "left": 130, "max_w": 354},
    {"key": "pinCode",         "top": 600, "left": 128, "max_w": 356},
    {"key": "handoverDate",    "top": 652, "left": 178, "max_w": 306},
]


def _build_slide_4(data: dict) -> str:
    return "".join(
        f'<div style="position:absolute;top:{field["top"]}px;left:{field["left"]}px;'
        f'max-width:{field["max_w"]}px;{_FIELD_STYLE}">'
        f'{escape_html(data.get(field["key"], ""))}</div>'
        for field in _FIELDS
    )


async def _render_details_overlay(browser, data: dict) -> bytes:
    context = await browser.new_context(
        viewport={"width": PAGE_W, "height": PAGE_H},
        java_script_enabled=False,
    )
    try:
        await install_font_routes(context)
        page = await context.new_page()
        await page.set_content(
            f"""<!doctype html>
<html><head><meta charset="utf-8"><style>
{font_css()}
* {{ margin:0; padding:0; box-sizing:border-box; }}
html, body {{ width:{PAGE_W}px; height:{PAGE_H}px; background:transparent; overflow:hidden; }}
</style></head><body>{_build_slide_4(data)}</body></html>""",
            wait_until="load",
            timeout=60_000,
        )
        return await page.pdf(
            width=f"{PAGE_W}px",
            height=f"{PAGE_H}px",
            print_background=True,
            margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
        )
    finally:
        await context.close()


async def generate_warranty_pdf(data: dict, browser) -> bytes:
    logger.info("Generating Warranty Handbook for: %s", data.get("customerName", "UNKNOWN"))

    template = PdfReader(WARRANTY_TEMPLATE)
    if len(template.pages) < 4:
        raise ValueError("Warranty handbook must contain at least four pages")

    overlay = PdfReader(io.BytesIO(await _render_details_overlay(browser, data)))
    template.pages[3].merge_page(overlay.pages[0])

    writer = PdfWriter()
    for page in template.pages:
        writer.add_page(page)

    output = io.BytesIO()
    writer.write(output)
    final_pdf = output.getvalue()
    logger.info("Warranty Handbook ready - %s KB, %s pages", len(final_pdf) // 1024, len(template.pages))
    return final_pdf


def generate_warranty_pdf_sync(data: dict) -> bytes:
    return run_render(lambda browser: generate_warranty_pdf(data, browser))
