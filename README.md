# SO Generator — backend

FastAPI service behind the Modula kitchen tooling. Four jobs:

1. **SO generation** — an Infurnia XLSX export in, an Odoo-importable order-lines
   workbook out (`/process-xlsx`).
2. **Design-draft PDF** — customer/render/finish slides rendered with Playwright
   (`/generate-pdf`).
3. **Warranty handbook + daily installation report** PDFs
   (`/generate-warranty`, `/generate-installation-report`).
4. **Database Manager** — token-authenticated CRUD over the `cabinets`,
   `colorcode` and `code_raw` lookup tables (`/auth`, `/db`), plus the S3-backed
   finish catalog (`/api/finishes`).

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .env.example .env      # then fill it in
python -m app.main        # or: uvicorn app.main:app --port 8000
```

`GET /health` checks the process and the database connection.
Interactive API docs at `/docs`.

## Tests

```bash
python -m unittest discover -s tests -p 'test_*.py'
```

`tests/test_input_validation.py` and `tests/test_sogeneration.py` need no database.
`tests/test_security_regressions.py` asserts source-level invariants (HTML escaping,
Chromium sandbox left on, auth on finish mutations) so they can't silently
regress.

## Layout

| Path | Role |
|---|---|
| `app/main.py` | app wiring, XLSX/PDF routes, and request-size limits |
| `app/api/` | auth, database-admin, and finish-catalog routers |
| `app/core/` | shared validation and middleware |
| `app/db/` | engine, sessions, and ORM models |
| `app/generators/` | Playwright/PDF rendering, browser pooling, and local fonts |
| `app/services/` | SO transformation, Odoo lookup, and S3 storage |
| `tests/` | unit and source-level regression tests |
| `scripts/` | one-off maintenance utilities |
| `assets/` / `warranty_assets/` | static document assets and warranty handbook template |

## Notes for whoever is next

- Everything heavy (Playwright, pandas, the Odoo XML-RPC call) runs in a thread
  pool bounded by `MAX_CONCURRENT_GENERATIONS`. Requests past that get a 503
  after a 2-second wait rather than queueing without limit.
- **Rendered pages must not fetch anything over the network.** Fonts live in
  `assets/fonts/` and are served by `web_fonts.install_font_routes`; the logo and
  every image are data URIs. That is what lets the renderers wait on `load`
  instead of `networkidle`, which is worth ~530 ms *per page*. If you add an
  external `<link>` or `<img src="https://...">`, you hand that back and
  reintroduce a hard dependency on egress from the render host.
- To refresh or add a face: drop the woff2 into `assets/fonts/`, add an entry to
  `manifest.json`, then `python -m app.generators.web_fonts` to self-check it.
- `/process-xlsx` pre-loads the three lookup tables once per workbook, so row
  processing does no database round-trips. Do not reintroduce per-row queries.
- Per-sheet transform state lives on `SheetState`, never in module globals —
  two concurrent uploads would otherwise share `tt_color` and the deferred BOM
  rows.
- Table DDL comes from `Base.metadata.create_all` plus the idempotent index
  statements in `app/main.py`. There is no migration tool; schema changes to
  existing tables have to be applied by hand.
- `/process-xlsx` and the three PDF routes are **unauthenticated** — only `/db`,
  `/auth/me` and the finish mutations require a bearer token. Fine behind a
  private network, not on the open internet.
