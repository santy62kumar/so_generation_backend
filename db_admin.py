"""
db_admin.py
─────────────────────────────────────────────────────────────────────────────
CRUD API backing the "Database Manager" UI.

Exposes one generic set of routes that works across all three tables
(cabinets, colorcode, code_raw) driven by TABLE_CONFIG below, so adding a
4th table later is a config change, not new endpoints.

All routes require a valid Bearer token (see auth.py).
"""

import io
from typing import Optional, Dict, Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from database import get_db
from models import Cabinet, ColorCode, CodeRaw
from auth import get_current_user

router = APIRouter(prefix="/db", tags=["database-admin"])


# ── Table registry ─────────────────────────────────────────────────────────────
# search_field  → the column the "search" box filters on (partial match)
# pk            → primary key column, used to target update/delete
# columns       → columns returned to / editable from the frontend, in order
TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "cabinets": {
        "model": Cabinet,
        "pk": "id",
        "search_field": "cabinet_code",
        "columns": [
            "id", "cabinet_code", "description",
            "bom_line_1", "bom_line_2", "bom_line_3",
            "bom_line_4", "bom_line_5", "bom_line_6",
        ],
        "required": ["cabinet_code", "description"],
    },
    "colorcode": {
        "model": ColorCode,
        "pk": "id",
        "search_field": "colour_name",
        "columns": ["id", "colour_name", "colour_code"],
    },
    "code_raw": {
        "model": CodeRaw,
        "pk": "infurnia_code",
        "search_field": "infurnia_code",
        "columns": ["infurnia_code", "odoo_code"],
    },
}


def _get_config(table: str) -> Dict[str, Any]:
    cfg = TABLE_CONFIG.get(table)
    if not cfg:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown table '{table}'. Valid options: {', '.join(TABLE_CONFIG)}",
        )
    return cfg


def _row_to_dict(row, columns) -> Dict[str, Any]:
    return {col: getattr(row, col) for col in columns}


# ── List / Search ──────────────────────────────────────────────────────────────
@router.get("/{table}")
def list_or_search(
    table: str,
    q: Optional[str] = Query(
        None, description="Search value — matched against the table's configured search field"
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=500),
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user),
):
    cfg = _get_config(table)
    model = cfg["model"]

    # Single round-trip: a window-function count rides along with the page of
    # rows, instead of issuing a separate COUNT(*) query beforehand.
    query = db.query(model, func.count().over().label("total_count"))
    if q:
        search_col = getattr(model, cfg["search_field"])
        query = query.filter(search_col.ilike(f"%{q.strip()}%"))

    results = (
        query.order_by(getattr(model, cfg["pk"]))
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    rows = [r[0] for r in results]
    total = results[0][1] if results else 0

    # Empty page (e.g. filtered query with 0 matches) needs its own cheap
    # count fallback, since the window function only rides on returned rows.
    if not results and (q or page > 1):
        count_query = db.query(func.count(getattr(model, cfg["pk"])))
        if q:
            search_col = getattr(model, cfg["search_field"])
            count_query = count_query.filter(search_col.ilike(f"%{q.strip()}%"))
        total = count_query.scalar()

    return {
        "table": table,
        "search_field": cfg["search_field"],
        "pk": cfg["pk"],
        "columns": cfg["columns"],
        "total": total,
        "page": page,
        "page_size": page_size,
        "rows": [_row_to_dict(r, cfg["columns"]) for r in rows],
    }


# ── Create ─────────────────────────────────────────────────────────────────────
@router.post("/{table}")
def create_row(
    table: str,
    payload: Dict[str, Any],
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user),
):
    cfg = _get_config(table)
    model = cfg["model"]

    allowed_fields = {c: payload.get(c) for c in cfg["columns"] if c in payload}
    # Don't let the client set an autoincrement id explicitly on tables that have one
    if cfg["pk"] == "id":
        allowed_fields.pop("id", None)

    missing = [
        field for field in cfg.get("required", [])
        if not str(allowed_fields.get(field, "") or "").strip()
    ]
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing required field(s): {', '.join(missing)}")

    new_row = model(**allowed_fields)
    db.add(new_row)
    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Could not create record: {exc}")
    db.refresh(new_row)

    return {"message": "created", "row": _row_to_dict(new_row, cfg["columns"])}


# ── Update ─────────────────────────────────────────────────────────────────────
@router.put("/{table}/{pk_value}")
def update_row(
    table: str,
    pk_value: str,
    payload: Dict[str, Any],
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user),
):
    cfg = _get_config(table)
    model = cfg["model"]
    pk_col = getattr(model, cfg["pk"])

    row = db.query(model).filter(pk_col == pk_value).first()
    if not row:
        raise HTTPException(status_code=404, detail="Record not found")

    editable_columns = [c for c in cfg["columns"] if c != cfg["pk"]]
    for key, value in payload.items():
        if key in editable_columns:
            setattr(row, key, value)

    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Could not update record: {exc}")
    db.refresh(row)

    return {"message": "updated", "row": _row_to_dict(row, cfg["columns"])}


# ── Delete ─────────────────────────────────────────────────────────────────────
@router.delete("/{table}/{pk_value}")
def delete_row(
    table: str,
    pk_value: str,
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user),
):
    cfg = _get_config(table)
    model = cfg["model"]
    pk_col = getattr(model, cfg["pk"])

    row = db.query(model).filter(pk_col == pk_value).first()
    if not row:
        raise HTTPException(status_code=404, detail="Record not found")

    db.delete(row)
    db.commit()
    return {"message": "deleted", "pk": pk_value}


# ── Download whole table (.xlsx) ────────────────────────────────────────────────
@router.get("/{table}/download")
def download_table(
    table: str,
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user),
):
    cfg = _get_config(table)
    model = cfg["model"]

    rows = db.query(model).order_by(getattr(model, cfg["pk"])).all()
    data = [_row_to_dict(r, cfg["columns"]) for r in rows]
    df = pd.DataFrame(data, columns=cfg["columns"])

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=table[:31])  # Excel sheet name limit
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{table}.xlsx"'},
    )