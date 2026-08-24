"""
app/generators/web_fonts.py
─────────────────────────────────────────────────────────────────────────────
Serves the PDF fonts from disk instead of fonts.googleapis.com.

Every rendered page used to carry a `<link>` to Google Fonts, and two of the
three renderers then waited on `wait_until="networkidle"`. That cost a CSS
round trip plus the font files plus a 500 ms idle window *per page* — roughly
a second each, so ~12 s on a 12-slide warranty card — and stalled for the full
60 s timeout on a host without egress.

The faces live in assets/fonts/ (see manifest.json). `font_css()` emits
@font-face rules pointing at a host that does not exist, and
`install_font_routes()` intercepts those requests and fulfils them from
memory. Chromium honours `unicode-range`, so a page only ever asks for the
subsets its text actually needs, and nothing leaves the process.

If assets/fonts/ is missing, `font_css()` falls back to the Google stylesheet
so rendering still works — just slowly.
"""

import json
import logging
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

FONTS_DIR = Path(__file__).resolve().parents[2] / "assets" / "fonts"

# Any host that will never resolve: every request to it is intercepted below.
FONT_HOST = "https://fonts.modula.invalid"

_FALLBACK_CSS = (
    "@import url('https://fonts.googleapis.com/css2"
    "?family=Montserrat:wght@400;600;700;800;900"
    "&family=Nunito+Sans:wght@400;700;900"
    "&family=Cormorant+Garamond:wght@400;600&display=swap');"
)


@lru_cache(maxsize=1)
def _manifest() -> list[dict]:
    try:
        return json.loads((FONTS_DIR / "manifest.json").read_text())
    except (OSError, ValueError):
        logger.warning(
            "No font manifest at %s — falling back to Google Fonts over the network. "
            "Re-add assets/fonts/ to get local fonts back.",
            FONTS_DIR,
        )
        return []


@lru_cache(maxsize=1)
def font_css() -> str:
    """The @font-face block to drop into every rendered page's <style>."""
    entries = _manifest()
    if not entries:
        return _FALLBACK_CSS

    rules = []
    for entry in entries:
        declarations = [
            f"font-family: '{entry['family']}'",
            f"font-style: {entry.get('style') or 'normal'}",
            f"font-weight: {entry['weight']}",
            # swap, not block: if a wait ever fails we want the wrong face, not invisible text
            "font-display: swap",
            f"src: url('{FONT_HOST}/{entry['file']}') format('woff2')",
        ]
        if entry.get("stretch"):
            declarations.insert(3, f"font-stretch: {entry['stretch']}")
        if entry.get("unicode_range"):
            declarations.append(f"unicode-range: {entry['unicode_range']}")
        rules.append("@font-face{" + ";".join(declarations) + "}")
    return "".join(rules)


@lru_cache(maxsize=64)
def _font_bytes(filename: str) -> bytes:
    """Read one font file, staying inside FONTS_DIR."""
    path = (FONTS_DIR / filename).resolve()
    path.relative_to(FONTS_DIR.resolve())      # raises on traversal
    return path.read_bytes()


async def install_font_routes(context) -> None:
    """Fulfil this context's font requests from disk. Call once per context."""
    if not _manifest():
        return

    async def handler(route, request):
        filename = request.url.rsplit("/", 1)[-1]
        try:
            body = _font_bytes(filename)
        except (OSError, ValueError):
            logger.warning("Unknown font requested by the renderer: %s", filename)
            await route.abort()
            return
        await route.fulfill(
            status=200,
            body=body,
            headers={
                "content-type": "font/woff2",
                "cache-control": "public, max-age=31536000, immutable",
            },
        )

    await context.route(f"{FONT_HOST}/*", handler)


def warm() -> None:
    """Read every face into the cache up front, off the render path."""
    for entry in _manifest():
        try:
            _font_bytes(entry["file"])
        except (OSError, ValueError):
            logger.warning("Missing font file listed in the manifest: %s", entry["file"])


if __name__ == "__main__":
    # Self-check: the CSS must reference only files that exist on disk.
    import re

    css = font_css()
    assert css and "googleapis" not in css, "font manifest is missing or unreadable"
    referenced = re.findall(rf"{re.escape(FONT_HOST)}/([^']+)", css)
    assert referenced, css[:200]
    for name in referenced:
        assert _font_bytes(name)[:4] == b"wOF2", f"{name} is not a woff2 file"
    families = sorted({e["family"] for e in _manifest()})
    print(f"{len(referenced)} faces OK across {families}, {len(css) // 1024} KB of CSS")
