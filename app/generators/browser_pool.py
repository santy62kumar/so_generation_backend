"""
app/generators/browser_pool.py
─────────────────────────────────────────────────────────────────────────────
Keeps one Chromium alive per render thread instead of launching a fresh one
for every request.

`p.chromium.launch()` costs roughly half a second to a second and a half, which
on the warranty card (a handful of small pages) was most of the request. Each
worker thread in main.py's ThreadPoolExecutor keeps its own event loop,
Playwright driver and browser, so concurrency stays bounded by
MAX_CONCURRENT_GENERATIONS and no browser object is ever touched from two
threads.

Every render gets a brand-new browser *context*, so nothing leaks between
requests — only the process is shared.

Set REUSE_BROWSER=0 to fall back to launch-per-request.
"""

import asyncio
import logging
import os
import threading

from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

_local = threading.local()

REUSE_BROWSER = os.getenv("REUSE_BROWSER", "1").strip().lower() not in {"0", "false", "no"}


def _thread_loop() -> asyncio.AbstractEventLoop:
    """One long-lived event loop per render thread — Playwright objects are
    bound to the loop that created them, so it has to outlive the browser."""
    loop = getattr(_local, "loop", None)
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _local.loop = loop
    return loop


async def _acquire_browser():
    """Returns (browser, was_reused)."""
    browser = getattr(_local, "browser", None)
    if browser is not None and browser.is_connected():
        return browser, True

    playwright = getattr(_local, "playwright", None)
    if playwright is None:
        playwright = await async_playwright().start()
        _local.playwright = playwright

    browser = await playwright.chromium.launch()
    _local.browser = browser
    return browser, False


async def _discard_browser() -> None:
    """Tear the cached browser down so a wedged one can't poison the next call."""
    for attribute, method in (("browser", "close"), ("playwright", "stop")):
        obj = getattr(_local, attribute, None)
        setattr(_local, attribute, None)
        if obj is None:
            continue
        try:
            await getattr(obj, method)()
        except Exception:
            logger.debug("Could not shut down %s cleanly", attribute, exc_info=True)


async def _render(factory):
    for attempt in (1, 2):
        browser, was_reused = await _acquire_browser()
        try:
            return await factory(browser)
        except Exception as exc:
            await _discard_browser()
            # A ValueError is our own payload validation, not a sick browser,
            # and a freshly launched browser failing will fail again.
            if attempt == 2 or isinstance(exc, ValueError) or not was_reused:
                raise
            logger.warning(
                "Render failed on a reused browser; retrying with a fresh launch",
                exc_info=True,
            )


async def _render_once(factory):
    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.launch()
        try:
            return await factory(browser)
        finally:
            await browser.close()
    finally:
        await playwright.stop()


def run_render(factory):
    """Sync entry point for the FastAPI routes' thread pool.

    `factory` takes a Playwright `browser` and returns the coroutine that does
    the actual rendering.
    """
    if not REUSE_BROWSER:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_render_once(factory))
        finally:
            loop.close()

    return _thread_loop().run_until_complete(_render(factory))
