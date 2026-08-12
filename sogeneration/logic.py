"""Sales-order generation: base processing logic.

All static lookup tables live in `constants/mapping.py`; this module only holds
behaviour (parsing, DB lookups, row processing, workbook building).
"""

import io
import math
import re

import pandas as pd
from fastapi import HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from database import get_db
from models import Cabinet, CodeRaw, ColorCode
from odoo import get_customer_poc

from .constants.mapping import (
    AUTO_ADD_SKIRTING,
    COLUMN_ORDER,
    DROP_GOLA_WITHOUT_SKIRTING,
    FLAG_MISSING_SKIRTING,
    FILLER_EXTRAS,
    FILLER_EXTRA_FINISH,
    FILLER_EXTRA_SUFFIX,
    GLASS_SHUTTER_MODELS,
    GLASS_SHUTTER_PROFILE_MAPPING,
    GOLA_LC_ACCESSORY,
    GOLA_LC_ACCESSORY_MULTIPLIER,
    GOLA_TO_SKIRTING,
    INFURNIA_TO_ODOO,
    LC_DESCRIPTION_PATTERN,
    LC_FILLERS,
    LIGHT_LENGTH_PRODUCT,
    LIGHT_METRES_PER_UNIT,
    LOFT_FILLERS,
    MK_FIL_MODELS,
    P_FIL_COMPANION_PRODUCT,
    P_FIL_MODELS,
    PRELAM_FINISHES,
    PSU_CABINETS_PER_UNIT,
    PSU_PRODUCT,
    REQUIRE_ALL_SKIRTINGS,
    SERVICE_CHARGE_PRODUCT,
    SHUTTER_FINISH_MAPPING,
    SKIP_DUPLICATE_SKIRTING,
    SKIRTING_ACCESSORIES,
    SKIRTING_ACCESSORY_MULTIPLIER,
    SKIRTING_QTY_RATIO,
    UC_FILLERS,
)

# ── Module-level state (reset once per sheet in handle_process_xlsx) ──────────
# tt_color and deferred_bom_rows must stay module-level globals; they are never
# reset inside process_mk_model.
tt_color = None
deferred_bom_rows = []
total_light_length = [0]
processed_categories = set()
lc_cabinet_count = [0]


# ── Utilities ─────────────────────────────────────────────────────────────────

def normalize_text(value):
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    return value if value else None


def _norm(code):
    """Normalise a product code: strip all whitespace, upper-case.

    Handles the inconsistent spacing in the source sheets, e.g. 'PR -119'
    and 'PR-119' both normalise to 'PR-119'.
    """
    if code is None:
        return ""
    return re.sub(r"\s+", "", str(code)).upper()


def _as_list(value):
    """Accept either a single code or a list of codes in the mappings."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


# Pre-normalised lookups, built once at import time from the static mappings.
# Values are always lists, so a gola may map to one skirting or several.
_GOLA_BY_MODEL = {_norm(k): _as_list(v) for k, v in GOLA_TO_SKIRTING.items()}
_ODOO_BY_INFURNIA = {_norm(k): v for k, v in INFURNIA_TO_ODOO.items()}

# Reverse safety net: if `model` arrives in some other form but its resolved
# odoo_code is a known gola odoo_code, we can still find the skirting.
_GOLA_BY_ODOO = {
    _norm(INFURNIA_TO_ODOO[gola]): _as_list(skirting)
    for gola, skirting in GOLA_TO_SKIRTING.items()
    if gola in INFURNIA_TO_ODOO
}

# Point 1 — gola code (infurnia or odoo form) → accessory product.
_GOLA_ACCESSORY_BY_CODE = {_norm(k): v for k, v in GOLA_LC_ACCESSORY.items()}
_GOLA_ACCESSORY_BY_CODE.update({
    _norm(INFURNIA_TO_ODOO[gola]): product
    for gola, product in GOLA_LC_ACCESSORY.items()
    if gola in INFURNIA_TO_ODOO
})

# Point 2 — skirting code (infurnia or odoo form) → accessory products.
_SKIRTING_ACCESSORIES_BY_CODE = {_norm(k): v for k, v in SKIRTING_ACCESSORIES.items()}
_SKIRTING_ACCESSORIES_BY_CODE.update({
    _norm(INFURNIA_TO_ODOO[skirting]): products
    for skirting, products in SKIRTING_ACCESSORIES.items()
    if skirting in INFURNIA_TO_ODOO
})

_LC_DESCRIPTION_RE = re.compile(LC_DESCRIPTION_PATTERN)


def is_lc_cabinet(description):
    """True when a cabinet description carries the LC token."""
    return bool(_LC_DESCRIPTION_RE.search(str(description or "")))


# ── Extractors ────────────────────────────────────────────────────────────────

def is_glass_shutter_model(model):
    return model in GLASS_SHUTTER_MODELS


def extract_model(text):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    m = re.search(r"Model:\s*(.+?)(?:\n|$)", str(text))
    return m.group(1).strip() if m else None


def extract_shutter_finish(text):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None

    s = str(text)

    m = re.search(r"Shutter.*?Finish\s*:\s*(.+?)(?:\n|$)", s, re.DOTALL)
    if m:
        finish = m.group(1).strip()
        return SHUTTER_FINISH_MAPPING.get(finish, finish)

    s_stripped = s.strip()
    if s_stripped in PRELAM_FINISHES:
        return s_stripped

    m2 = re.search(r"Finish\s*:\s*(.+?)(?:\n|$)", s, re.DOTALL)
    if m2:
        finish = m2.group(1).strip()
        return SHUTTER_FINISH_MAPPING.get(finish, finish)

    return None


def build_glass_shutter_description(mk_product, glass_model, prelam_finish):
    profile_label = GLASS_SHUTTER_PROFILE_MAPPING.get(glass_model, glass_model)
    return f"[{mk_product}]\n{profile_label}\nGLASS PROFILE: {prelam_finish}"


# ── Helpers ───────────────────────────────────────────────────────────────────

def compute_quantity(val):
    try:
        text = str(val).strip()

        if not text:
            return 1

        # Only a numerical value, e.g. 1, 2, 6.9, 16.37
        if re.fullmatch(r"\d+(?:\.\d+)?", text):
            number = float(text)

            # Return whole numbers as integers
            return int(number) if number.is_integer() else number

        # Numerical value with a unit, e.g. "6.9 metre"
        match = re.search(r"\d+(?:\.\d+)?", text)

        if not match:
            return 1

        quantity = float(match.group())

        # Convert running length into 3-metre quantities
        return math.ceil(quantity / 3)

    except (ValueError, TypeError):
        return 1


def get_pr_lines(cnt, total_light_length_mm):
    lines = []

    # ── PR-047: 1 unit per 5 light cabinets (rounded up) ──
    if cnt >= 1:
        lines.append({
            "Order Lines/Product": PSU_PRODUCT,
            "Order Lines/Description": PSU_PRODUCT,
            "Order Lines / Quantity": math.ceil(cnt / PSU_CABINETS_PER_UNIT),
        })

    # ── PR-048: total light length in meters, 1 unit per 5 m (rounded up) ──
    length_m = total_light_length_mm / 1000
    if length_m > 0:
        lines.append({
            "Order Lines/Product": LIGHT_LENGTH_PRODUCT,
            "Order Lines/Description": LIGHT_LENGTH_PRODUCT,
            "Order Lines / Quantity": math.ceil(length_m / LIGHT_METRES_PER_UNIT),
        })

    return lines


# ── DB lookups ────────────────────────────────────────────────────────────────

def get_colour_code(db, finish, model, index, reference, failed_rows):
    colour = db.query(ColorCode).filter(ColorCode.colour_name == finish).first()
    if not colour:
        failed_rows.append({
            "Row": index + 1,
            "Model": model,
            "Cabinet Position": reference,
            "Reason": f"Cabinet processed but could not find colour '{finish}'",
        })
        return None
    return colour.colour_code


def get_odoo_code(db, model, index, reference, failed_rows):
    mapping = db.query(CodeRaw).filter(CodeRaw.infurnia_code == model).first()
    if not mapping:
        failed_rows.append({
            "Row": index + 1,
            "Model": model,
            "Cabinet Position": reference,
            "Reason": f"No mapping found in code_raw for model '{model}'",
        })
        return None
    return mapping.odoo_code


# ── Deferred BOM handling ─────────────────────────────────────────────────────

def _flush_deferred_bom_rows(results):
    global deferred_bom_rows, tt_color

    for row in deferred_bom_rows:
        cabinet   = row["cabinet"]
        finish    = row["finish"]
        reference = row["reference"]
        quantity  = row["quantity"]

        for i in range(1, 7):
            bom = getattr(cabinet, f"bom_line_{i}")
            if bom:
                product = f"{bom}-{tt_color}"
                results.append({
                    "Order Lines/Product":     product,
                    "Order Lines/Description": f"[{product}] ({finish})",
                    "Cabinet Position":        reference,
                    "Order Lines / Quantity":  quantity,
                })

    deferred_bom_rows.clear()


def finalize_mk_processing(results, failed_rows):
    global deferred_bom_rows

    if not deferred_bom_rows:
        return

    if tt_color:
        # LC appeared late — flush whatever remains
        _flush_deferred_bom_rows(results)
    else:
        # LC never appeared in the entire sheet — log all deferred as failed
        for row in deferred_bom_rows:
            failed_rows.append({
                "Row":              row["index"] + 1,
                "Model":            row["model"],
                "Cabinet Position": row["reference"],
                "Reason":           "No colour code and no LC model found in sheet to derive tt_color",
            })
        deferred_bom_rows.clear()


# ── Condition processors ──────────────────────────────────────────────────────

def process_mw_model(db, model, finish, quantity, index, reference,
                     failed_rows, results, customer_meta=None,
                     glass_shutter_found=None):
    """Process an MW-* model.

    Differences from process_mk_model:
      - Description on the primary row is always "[MODEL]", never
        cabinet.description.
      - Non-prelam finish: only bom_line_i values starting with "P" get
        expanded (product = f"{bom}-{effective_colour}").
      - Prelam finish: only bom_line_i values starting with "G" get
        expanded, with no colour suffix, and a two-line glass description
        (GLASS PROFILE, then GLASS SHUTTER PROFILE).
    """
    cabinet = db.query(Cabinet).filter(Cabinet.cabinet_code == model).first()
    if not cabinet:
        failed_rows.append({
            "Row": index + 1, "Model": model,
            "Cabinet Position": reference,
            "Reason": "Cabinet not found in DB",
        })
        return False

    # ── MW primary row: description is always "[MODEL]" ──────────────────
    first_row = {
        "Order Lines/Product":     model,
        "Order Lines/Description": f"[{model}]",
        "Cabinet Position":        reference,
        "Order Lines / Quantity":  quantity,
    }
    if customer_meta:
        first_row.update(customer_meta)
    results.append(first_row)

    # ── Prelam finish → only "G"-prefixed BOM lines, no colour suffix ─────
    if finish in PRELAM_FINISHES:
        if not glass_shutter_found:
            failed_rows.append({
                "Row": index + 1, "Model": model,
                "Cabinet Position": reference,
                "Reason": "Prelam finish found but no glass-shutter profile model row exists in the sheet",
            })
            return True

        glass_model   = glass_shutter_found[0]
        profile_label = GLASS_SHUTTER_PROFILE_MAPPING.get(glass_model, glass_model)

        for i in range(1, 7):
            bom = getattr(cabinet, f"bom_line_{i}")
            if bom and bom.startswith("G"):
                product = bom  # no colour suffix
                results.append({
                    "Order Lines/Product":     product,
                    "Order Lines/Description": f"[{product}]\nGLASS PROFILE: {finish}\n{profile_label}",
                    "Cabinet Position":        reference,
                    "Order Lines / Quantity":  quantity,
                })
        return True

    # ── Non-prelam finish → only "P"-prefixed BOM lines get the colour code ─
    colour_code = get_colour_code(db, finish, model, index, reference, failed_rows)
    if not colour_code:
        # primary row already appended; BOM skipped since colour wasn't found
        return True

    effective_colour = colour_code

    for i in range(1, 7):
        bom = getattr(cabinet, f"bom_line_{i}")
        if bom and bom.startswith("P"):
            product = f"{bom}-{effective_colour}"
            results.append({
                "Order Lines/Product":     product,
                "Order Lines/Description": f"[{product}] ({finish})",
                "Cabinet Position":        reference,
                "Order Lines / Quantity":  quantity,
            })

    return True


def process_mk_model(db, model, finish, quantity, index, reference,
                     failed_rows, results, customer_meta=None,
                     light_cabinet_count=None):
    global tt_color, deferred_bom_rows, total_light_length, lc_cabinet_count

    cabinet = db.query(Cabinet).filter(Cabinet.cabinet_code == model).first()
    if not cabinet:
        failed_rows.append({
            "Row": index + 1, "Model": model,
            "Cabinet Position": reference,
            "Reason": "Cabinet not found in DB",
        })
        return False

    description = cabinet.description if cabinet.description else model

    # ── Point 1 support: tally LC cabinets for the PR-021 quantity ───────────
    # Counted here, before the prelam early-return, so prelam LC cabinets are
    # not missed.
    if is_lc_cabinet(description):
        lc_cabinet_count[0] += quantity

    if "LY" in description:
        if light_cabinet_count is not None:
            light_cabinet_count[0] += 1
        match = re.search(r'-(\d+)-', description)
        if match:
            total_light_length[0] += int(match.group(1)) * 2

    first_row = {
        "Order Lines/Product":     model,
        "Order Lines/Description": description,
        "Cabinet Position":        reference,
        "Order Lines / Quantity":  quantity,
    }
    if customer_meta:
        first_row.update(customer_meta)

    results.append(first_row)

    # ── If prelam, skip BOM here — post-loop will handle with glass description ──
    if finish in PRELAM_FINISHES:
        return True

    colour_code = get_colour_code(db, finish, model, index, reference, failed_rows)

    is_lc_model = "LC" in description
    if is_lc_model and colour_code and tt_color is None:
        tt_color = colour_code
        # ── Flush any rows that were deferred waiting for tt_color ────────
        _flush_deferred_bom_rows(results)

    # ── Use colour_code if available, fall back to tt_color ──────────────────
    effective_colour = colour_code or tt_color

    if not effective_colour:
        # Neither colour_code nor tt_color available yet — defer BOM lines.
        # 90% of the time an LC model will appear later and flush these.
        # The remaining 10% is handled by finalize_mk_processing() after the
        # main loop ends, which logs them to failed_rows.
        deferred_bom_rows.append({
            "cabinet":   cabinet,
            "finish":    finish,
            "reference": reference,
            "quantity":  quantity,
            "index":     index,
            "model":     model,
        })
        return True  # cabinet row already appended, BOM deferred

    for i in range(1, 7):
        bom = getattr(cabinet, f"bom_line_{i}")
        if bom:
            product = f"{bom}-{effective_colour}"
            results.append({
                "Order Lines/Product":     product,
                "Order Lines/Description": f"[{product}] ({finish})",
                "Cabinet Position":        reference,
                "Order Lines / Quantity":  quantity,
            })
    return True


def get_cabinet_category(model):
    """Return LC / UC / Loft for a filler model, or None if not recognised."""
    if model in LC_FILLERS:
        return "LC"
    if model in UC_FILLERS:
        return "UC"
    if model in LOFT_FILLERS:
        return "Loft"
    return None


def process_fil_model(db, model, finish, quantity, index, reference,
                      failed_rows, results, customer_meta=None):
    """Process a filler model and append extra fillers per unique category only once."""
    colour_code = get_colour_code(db, finish, model, index, reference, failed_rows)
    if not colour_code:
        return False

    # ── Primary line item ────────────────────────────────────────────
    product = f"{model}-{colour_code}"
    first_row = {
        "Order Lines/Product":     product,
        "Order Lines/Description": f"[{product}] ({finish})",
        "Cabinet Position":        reference,
        "Order Lines / Quantity":  quantity,
    }
    if customer_meta:
        first_row.update(customer_meta)
    results.append(first_row)

    # ── Extra filler line items based on cabinet category ────────────
    category = get_cabinet_category(model)
    if category and category not in processed_categories:
        for filler_code in FILLER_EXTRAS.get(category, []):
            extra_product = f"{filler_code}-{FILLER_EXTRA_SUFFIX}"
            extra_row = {
                "Order Lines/Product":     extra_product,
                "Order Lines/Description": f"[{extra_product}] ({FILLER_EXTRA_FINISH})",
                "Cabinet Position":        reference,
                "Order Lines / Quantity":  1,
            }
            results.append(extra_row)
        processed_categories.add(category)

    return True


# ── Gola → skirting resolution ────────────────────────────────────────────────

def get_skirting_models(model, odoo_code=None):
    """Return the list of skirting infurnia codes for a gola code (may be empty)."""
    skirtings = _GOLA_BY_MODEL.get(_norm(model))
    if not skirtings and odoo_code:
        skirtings = _GOLA_BY_ODOO.get(_norm(odoo_code))
    return list(skirtings or [])


# Backwards-compatible alias: returns only the first mapped skirting.
def get_skirting_model(model, odoo_code=None):
    skirtings = get_skirting_models(model, odoo_code)
    return skirtings[0] if skirtings else None


def check_gola_skirtings(df, failed_rows):
    """Report gola rows whose matching skirting seal is not punched in the sheet.

    This is a sheet-level check: a gola on row 12 may have its skirting punched
    on row 40, so it can only be answered after every model has been collected.
    Each offending gola code is reported once, against its first occurrence.

    Returns the set of normalised gola codes that failed the check. The gola is
    still punched into the output unless DROP_GOLA_WITHOUT_SKIRTING is on — the
    Failed entry is a warning for the design team, not a rejection.
    """
    present = set()
    for _, row in df.iterrows():
        m = normalize_text(row["Model"])
        if m:
            present.add(_norm(m))

    missing = {}      # normalised gola code -> (row number, reference, model, [skirtings])

    for index, row in df.iterrows():
        model = normalize_text(row["Model"])
        if not model:
            continue

        skirtings = _GOLA_BY_MODEL.get(_norm(model))
        if not skirtings:
            continue      # not a gola profile

        # A skirting counts as punched under its infurnia code or its odoo code.
        def _is_punched(skirting):
            forms = {_norm(skirting)}
            odoo = INFURNIA_TO_ODOO.get(skirting)
            if odoo:
                forms.add(_norm(odoo))
            return bool(forms & present)

        absent = [s for s in skirtings if not _is_punched(s)]

        if REQUIRE_ALL_SKIRTINGS:
            if not absent:
                continue          # every mapped skirting is punched
        else:
            if len(absent) < len(skirtings):
                continue          # at least one mapped skirting is punched
            # none punched — report the full list of acceptable codes
            absent = list(skirtings)

        # Record the first row that needs these skirtings.
        key = _norm(model)
        if key not in missing:
            missing[key] = (index + 1, row["Reference"], model, absent)

    if FLAG_MISSING_SKIRTING:
        joiner = " and " if REQUIRE_ALL_SKIRTINGS else " or "
        for row_no, reference, model, absent in missing.values():
            names = joiner.join(f"'{s}'" for s in absent)
            failed_rows.append({
                "Row":              row_no,
                "Model":            model,
                "Cabinet Position": reference,
                "Reason":           f"Gola '{model}' is punched: its skirting {names} is missing from the sheet",
            })

    return set(missing.keys())


def get_gola_accessory_lines(df, blocked_golas, lc_count, failed_rows):
    """Point 1: one accessory line per distinct gola accessory product.

    Quantity is the LC cabinet count x GOLA_LC_ACCESSORY_MULTIPLIER. Golas
    rejected by check_gola_skirtings are ignored, since they never reach the
    output either.
    """
    products = []
    first_seen = {}

    for index, row in df.iterrows():
        model = normalize_text(row["Model"])
        if not model:
            continue

        key = _norm(model)
        if key in blocked_golas:
            continue

        product = _GOLA_ACCESSORY_BY_CODE.get(key)
        if product and product not in products:
            products.append(product)
            first_seen[product] = (index + 1, model, row["Reference"])

    lines = []
    for product in products:
        quantity = lc_count * GOLA_LC_ACCESSORY_MULTIPLIER

        if quantity <= 0:
            row_no, model, reference = first_seen[product]
            failed_rows.append({
                "Row":              row_no,
                "Model":            model,
                "Cabinet Position": reference,
                "Reason":           f"Gola '{model}' punched but no LC cabinet found in the sheet, so {product} was skipped",
            })
            continue

        lines.append({
            "Order Lines/Product":     product,
            "Order Lines/Description": f"[{product}]",
            "Order Lines / Quantity":  quantity,
        })

    return lines


def resolve_skirting_odoo_code(db, skirting_model, index, reference):
    """Resolve the skirting odoo_code: DB first, static mapping as fallback."""
    probe_failures = []          # keep DB misses out of the real failed_rows
    odoo_code = get_odoo_code(db, skirting_model, index, reference, probe_failures)
    if odoo_code:
        return odoo_code
    return _ODOO_BY_INFURNIA.get(_norm(skirting_model))


def process_generic_model(db, model, quantity, index, reference,
                          failed_rows, results, customer_meta=None):
    odoo_code = get_odoo_code(db, model, index, reference, failed_rows)
    if not odoo_code:
        return False

    first_row = {
        "Order Lines/Product":     odoo_code,
        "Order Lines/Description": f"[{odoo_code}]",
        "Cabinet Position":        reference,
        "Order Lines / Quantity":  quantity,
    }
    if customer_meta:
        first_row.update(customer_meta)

    results.append(first_row)

    # ------------------------------------------------------------------
    # Point 2: a skirting seal ships with two accessory codes, each at
    # double the skirting line's own quantity.
    # ------------------------------------------------------------------
    accessories = (_SKIRTING_ACCESSORIES_BY_CODE.get(_norm(model))
                   or _SKIRTING_ACCESSORIES_BY_CODE.get(_norm(odoo_code)))
    if accessories:
        for accessory in accessories:
            results.append({
                "Order Lines/Product":     accessory,
                "Order Lines/Description": f"[{accessory}]",
                "Cabinet Position":        reference,
                "Order Lines / Quantity":  quantity * SKIRTING_ACCESSORY_MULTIPLIER,
            })

    # ------------------------------------------------------------------
    # Gola profiles always ship with their matching skirting seal.
    # ------------------------------------------------------------------
    skirting_models = get_skirting_models(model, odoo_code)
    if skirting_models and AUTO_ADD_SKIRTING:
        # With several acceptable skirtings and none punched, only the first is
        # auto-added unless every one is required.
        if not REQUIRE_ALL_SKIRTINGS:
            skirting_models = skirting_models[:1]

        for skirting_model in skirting_models:
            skirting_odoo = resolve_skirting_odoo_code(db, skirting_model, index, reference)

            if not skirting_odoo:
                failed_rows.append({
                    "Row":              index + 1,
                    "Model":            skirting_model,
                    "Cabinet Position": reference,
                    "Reason":           f"Skirting odoo_code not found for gola '{model}'",
                })
                continue      # the gola line itself is valid, keep it

            already_present = SKIP_DUPLICATE_SKIRTING and any(
                r.get("Order Lines/Product") == skirting_odoo
                and r.get("Cabinet Position") == reference
                for r in results
            )

            if not already_present:
                skirting_row = {
                    "Order Lines/Product":     skirting_odoo,
                    "Order Lines/Description": f"[{skirting_odoo}]",
                    "Cabinet Position":        reference,
                    "Order Lines / Quantity":  quantity * SKIRTING_QTY_RATIO,
                }
                if customer_meta:
                    skirting_row.update(customer_meta)

                results.append(skirting_row)

    return True


# ── Router ────────────────────────────────────────────────────────────────────

def process_row(db, model, finish, quantity, index, reference,
                failed_rows, results, customer_meta, light_cabinet_count,
                glass_shutter_found=None):
    if model.startswith(("MK-", "CK-2.0")):
        if model in MK_FIL_MODELS:
            return process_fil_model(db, model, finish, quantity, index, reference,
                                     failed_rows, results, customer_meta)
        return process_mk_model(db, model, finish, quantity, index, reference,
                                failed_rows, results, customer_meta,
                                light_cabinet_count=light_cabinet_count)

    elif model.startswith("MW"):
        return process_mw_model(db, model, finish, quantity, index, reference,
                                failed_rows, results, customer_meta,
                                glass_shutter_found=glass_shutter_found)

    elif model.startswith("FIL-"):
        return process_fil_model(db, model, finish, quantity, index, reference,
                                 failed_rows, results, customer_meta)

    elif model.startswith("EP-"):
        return process_fil_model(db, model, finish, quantity, index, reference,
                                 failed_rows, results, customer_meta)

    elif model in P_FIL_MODELS:
        success = process_fil_model(db, model, finish, quantity, index, reference,
                                    failed_rows, results, customer_meta)
        if success:
            results.append({
                "Order Lines/Product":     P_FIL_COMPANION_PRODUCT,
                "Order Lines/Description": f"[{P_FIL_COMPANION_PRODUCT}]",
                "Cabinet Position":        reference,
                "Order Lines / Quantity":  quantity,
            })
        return success

    else:
        return process_generic_model(db, model, quantity, index, reference,
                                     failed_rows, results, customer_meta)


# ── Main handler ──────────────────────────────────────────────────────────────

async def handle_process_xlsx(file: UploadFile, db: Session):
    # ── Reset per-sheet state, once per request ──────────────────────────────
    global processed_categories, tt_color, deferred_bom_rows, total_light_length
    global lc_cabinet_count
    processed_categories = set()
    tt_color = None
    deferred_bom_rows = []
    total_light_length = [0]
    lc_cabinet_count = [0]

    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Please upload a .xlsx file")

    contents = await file.read()

    try:
        excel_bytes = io.BytesIO(contents)
        raw_df = pd.read_excel(excel_bytes, sheet_name=0, header=None, engine="openpyxl")
        df     = pd.read_excel(excel_bytes, header=2, engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to read Excel file: {e}")

    # ── Column validation ─────────────────────────────────────────────────────
    df.columns = df.columns.astype(str).str.strip()
    required_cols = ["Reference", "Item", "Finishes"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing columns in sheet: {missing}")

    # ── Quantity column ───────────────────────────────────────────────────────
    try:
        finishes_col_idx = df.columns.get_loc("Finishes")
        quantity_col     = df.columns[finishes_col_idx + 1]
    except (KeyError, IndexError):
        raise HTTPException(
            status_code=400,
            detail="Could not find the Quantity column (expected right after 'Finishes')",
        )

    df["Quantity"] = df[quantity_col].apply(compute_quantity)

    try:
        cell_value = str(raw_df.iloc[1, 2]).strip()

        # Extract just the first line (the project ID)
        first_line = cell_value.splitlines()[0].strip()

        if not first_line:
            raise HTTPException(status_code=400, detail="Project ID not found in the file")

        project_id = first_line  # "25-E-26-0016"

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error extracting Project ID: {e}")

    crm_id = project_id
    project_name, customer, poc = get_customer_poc(crm_id)

    # ── Derive columns ────────────────────────────────────────────────────────
    df["Model"]          = df["Item"].apply(extract_model)
    df["Shutter_Finish"] = df["Finishes"].apply(extract_shutter_finish)
    df["Reference"]      = df["Reference"].apply(normalize_text)

    results     = []
    failed_rows = []

    # ── Gola / skirting completeness check ────────────────────────────────────
    # Sheet-level: every gola profile punched should have its matching skirting
    # seal punched somewhere in the same sheet. Offenders are reported in the
    # Failed sheet; the gola and its Point 1 accessory are still punched unless
    # DROP_GOLA_WITHOUT_SKIRTING is turned on.
    golas_missing_skirting = check_gola_skirtings(df, failed_rows)
    blocked_golas = golas_missing_skirting if DROP_GOLA_WITHOUT_SKIRTING else set()

    # ── Service Charges quantity ──────────────────────────────────────────────
    service_charge_qty = None
    try:
        for i, row in raw_df.iterrows():
            for j, cell in enumerate(row):
                if str(cell).strip() == "Service Charges":
                    header_row  = raw_df.iloc[i + 1]
                    qty_col_idx = None
                    for col_idx, col_val in enumerate(header_row):
                        if str(col_val).strip() == "Quantity":
                            qty_col_idx = col_idx
                            break
                    if qty_col_idx is not None:
                        data_row = raw_df.iloc[i + 2]
                        raw_qty  = data_row.iloc[qty_col_idx]
                        match    = re.search(r"[\d.]+", str(raw_qty))
                        if match:
                            service_charge_qty = float(match.group())
                    break
            if service_charge_qty is not None:
                break
    except Exception as e:
        print(f"Could not extract Service Charges quantity: {e}")

    # ── Pre-scan glass-shutter models ─────────────────────────────────────────
    glass_shutter_found = []
    for _, scan_row in df.iterrows():
        m = normalize_text(scan_row["Model"])
        if is_glass_shutter_model(m) and m not in glass_shutter_found:
            glass_shutter_found.append(m)

    print(f"Glass-shutter models found in sheet: {glass_shutter_found}")

    # ── Main loop ─────────────────────────────────────────────────────────────
    customer_written    = False
    prelam_pending      = []
    light_cabinet_count = [0]   # list, to allow mutation within process_mk_model

    for index, row in df.iterrows():
        model     = normalize_text(row["Model"])
        finish    = normalize_text(row["Shutter_Finish"])
        reference = row["Reference"]
        quantity  = row["Quantity"]

        if not model:
            continue

        if is_glass_shutter_model(model):
            continue

        # Gola with no skirting punched — only skipped when
        # DROP_GOLA_WITHOUT_SKIRTING is on; already logged either way.
        if _norm(model) in blocked_golas:
            continue

        if model.startswith(("MK-", "FIL-", "EP-")) and not finish:
            failed_rows.append({
                "Row": index + 1, "Model": model,
                "Cabinet Position": reference, "Reason": "Finish missing",
            })
            continue

        customer_meta = None
        if not customer_written:
            customer_meta = {
                "Customer":      customer or "Default Customer",
                "GST Treatment": "Consumer",
                "POC":           poc or "Default POC",
                "Tag":           "Product",
                "Project Name":  project_name or "Default Project Name",
            }

        before_idx = len(results)

        success = process_row(db, model, finish, quantity, index, reference,
                              failed_rows, results, customer_meta, light_cabinet_count,
                              glass_shutter_found=glass_shutter_found)

        if success and not customer_written:
            customer_written = True

        if success and model.startswith("MK-") and finish in PRELAM_FINISHES:
            prelam_pending.append({
                "result_idx": before_idx,
                "finish":     finish,
                "mk_product": model,
                "row":        index + 1,
                "reference":  reference,
                # Do NOT snapshot tt_color here — it may not be set yet (e.g.
                # C123 appears before any LC model in the sheet). colour_code is
                # resolved after the main loop, once tt_color is established.
            })

    # ── Finalize deferred BOM rows after main loop ────────────────────────────
    # Handles the 10% case where no LC model was ever encountered.
    finalize_mk_processing(results, failed_rows)

    # ── Post-loop: patch glass description onto MK-prelam rows ────────────────
    if prelam_pending:
        if not glass_shutter_found:
            for item in prelam_pending:
                failed_rows.append({
                    "Row":              item["row"],
                    "Model":            item["mk_product"],
                    "Cabinet Position": item["reference"],
                    "Reason":           "Prelam finish found but no glass-shutter profile model row exists in the sheet",
                })

        else:
            glass_model = glass_shutter_found[0]
            insert_offset = 0  # track how many rows we've inserted so far

            for item in prelam_pending:
                adjusted_idx = item["result_idx"] + insert_offset
                mk_product = item["mk_product"]

                results[adjusted_idx]["Order Lines/Description"] = (
                    build_glass_shutter_description(
                        mk_product, glass_model, item["finish"]
                    )
                )

                cabinet = db.query(Cabinet).filter(Cabinet.cabinet_code == mk_product).first()
                if cabinet:
                    insert_idx = adjusted_idx + 1
                    # Use tt_color resolved after the full loop, so every prelam
                    # item (including ones processed before the LC model
                    # appeared) gets the correct suffix.
                    colour_code = tt_color
                    for i in range(1, 7):
                        bom = getattr(cabinet, f"bom_line_{i}")
                        if bom:
                            product = f"{bom}-{colour_code}" if colour_code else bom
                            bom_row = {
                                "Order Lines/Product":     product,
                                "Order Lines/Description": f"[{product}]",
                                "Cabinet Position":        item["reference"],
                                "Order Lines / Quantity":  1,
                            }
                            results.insert(insert_idx, bom_row)
                            insert_idx += 1
                            insert_offset += 1

    # ── Service charge row ────────────────────────────────────────────────────
    if service_charge_qty is not None:
        results.append({
            "Cabinet Position":        "B2C Installation Service",
            "Order Lines/Product":     SERVICE_CHARGE_PRODUCT,
            "Order Lines/Description": f"[{SERVICE_CHARGE_PRODUCT}]",
            "Order Lines / Quantity":  service_charge_qty,
        })
    else:
        print(f"Service charge quantity not found; skipping {SERVICE_CHARGE_PRODUCT} row.")

    # ── PSU / light-length rows ───────────────────────────────────────────────
    cnt = light_cabinet_count[0]
    if cnt >= 1:
        results.extend(get_pr_lines(cnt, total_light_length[0]))

    # ── Point 1: gola accessory, qty = 2 x LC cabinet count ──────────────────
    results.extend(
        get_gola_accessory_lines(df, blocked_golas, lc_cabinet_count[0], failed_rows)
    )

    # ── Build output workbook ─────────────────────────────────────────────────
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if results:
            pd.DataFrame(results, columns=COLUMN_ORDER).to_excel(
                writer, sheet_name="Success", index=False
            )
        if failed_rows:
            pd.DataFrame(failed_rows).to_excel(
                writer, sheet_name="Failed", index=False
            )

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="processed_output.xlsx"'},
    )