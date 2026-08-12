from fastapi import UploadFile, HTTPException, Depends
from fastapi.responses import StreamingResponse
import pandas as pd
import re
import io
from models import Cabinet, CodeRaw, ColorCode
from sqlalchemy.orm import Session
from database import get_db
from models import Cabinet, ColorCode
from odoo import get_customer_poc
import math


# ── Utilities ─────────────────────────────────────────────────────────────────
# processed_categories = set()

def normalize_text(value):
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    return value if value else None


# ── Mappings ──────────────────────────────────────────────────────────────────

SHUTTER_FINISH_MAPPING = {
    "Sandalwood":      "Courtyard Clay Gloss",
    "Soundcloud":      "Mistfield Gloss",
    "Washed Earth":    "Canyon Ridge Gloss",
    "Starlight White": "Glacier Veil Gloss",
    "Asteroid Belt":   "Industrial Bay Matte",
}

PRELAM_FINISHES = {
    "Back Painted Fluted Glass Ivory Matt (Prelam)",
    "Back Painted Fluted Glass Ash Matt (Prelam)",
    "Back Painted Fluted Glass Biscuit Matt (Prelam)",
    "Back Painted Fluted Glass Maple Bronze Gloss (Prelam)",
    "Back Painted Frosted Glass Beige Matt (Prelam)",
    "Back Painted Frosted Glass Graphite Matt (Prelam)",
    "Back Painted Sandstone Gloss (Prelam)",
    "Back Painted Pebble Gloss (Prelam)",
    "Fluted Glass Vanilla Matt (Prelam)",
    "Fluted Glass Coffee Matt (Prelam)",
    "Fluted Glass Onyx Matt (Prelam)",
    "Fluted Glass Snow Gloss (Prelam)",
    "Fluted Glass Caramel Gloss (Prelam)",
    "Fluted Glass Black Gloss (Prelam)",
    "Sandwich Glass Bronze Veil (Prelam)",
    "Sandwich Glass Bronze Grid (Prelam)",
    "Frosted Glass Mist (Prelam)",
    "Fluted Glass Ridge (Prelam)",
    "Fluted Glass Fine Ridge (Prelam)",
    "Textured Glass Glacier (Prelam)",
    "Clear Glass (Prelam)",
    "Black Tinted Glass (Prelam)",
    "Clear Fluted Glass (Prelam)",
    "Brown Tinted Glass (Prelam)",
    "Brown Fluted Glass (Prelam)",
}

GLASS_SHUTTER_PROFILE_MAPPING = {
    "KAPS-59 MB": "GLASS SHUTTER PROFILE: Matt Black ( KAPS-59 MB )",
    "KAPS-59 MG": "GLASS SHUTTER PROFILE: Matt Gold ( KAPS-59 MG )",
    "KAPS-59 SS": "GLASS SHUTTER PROFILE: Silver ( KAPS-59 SS )",
    "SCP-06 MB":  "GLASS SHUTTER PROFILE: Matt Black ( SCP-06 MB )",
    "SCP-06 MG":  "GLASS SHUTTER PROFILE: Matt Gold ( SCP-06 MG )",
    "SCP-06 SS":  "GLASS SHUTTER PROFILE: Silver ( SCP-06 SS )",
    "KSP-01 MB":  "GLASS SHUTTER PROFILE: Matt Black ( KSP-01 MB )",
    "KSP-01 MG":  "GLASS SHUTTER PROFILE: Matt Gold ( KSP-01 MG )",
    "KSP-01 SS":  "GLASS SHUTTER PROFILE: Silver ( KSP-01 SS )",
    "BGK-01":     "GLASS SHUTTER PROFILE: Rose Gold 45 mm Profile",
    "BGK-04":     "GLASS SHUTTER PROFILE: Gold 45 mm Profile",
    "BGK-05":     "GLASS SHUTTER PROFILE: Black 45 mm Profile",
    "BGK-07":     "GLASS SHUTTER PROFILE: Champagne 45 mm Profile",
    "BGK-06":     "GLASS SHUTTER PROFILE: Silver 45 mm Profile",
    
}

GLASS_SHUTTER_MODELS = set(GLASS_SHUTTER_PROFILE_MAPPING.keys())

LIGHT_CABINET = [
    "MK-0619", "MK-0946", "MK-0469", "MK-0940", "MK-0620", "MK-0947",
    "MK-0614", "MK-0941", "MK-0621", "MK-0948", "MK-0470", "MK-0942",
    "MK-0697", "MK-0969", "MK-0626", "MK-0965", "MK-0465", "MK-0972",
    "MK-0714", "MK-0971", "MK-0616", "MK-0954", "MK-0615", "MK-0943",
    "MK-0462", "MK-0968", "MK-0625", "MK-0966", "MK-1071", "MK-1070",
    "MK-1068", "MK-1069", "MK-0617", "MK-0955", "MK-1072", "MK-0979",
    "MK-0810", "MK-0975", "MK-0455", "MK-0944", "MK-0458", "MK-0970",
    "MK-0457", "MK-0974", "MK-0522", "MK-0973", "MK-0523", "MK-0967",
]

_MK_FIL_MODELS = {
    "MK-0777", "MK-1145", "MK-0766", "MK-0834",
    "MK-0775", "MK-0835", "MK-0725", "MK-0836", "MK-1263",
}

_P_FIL_MODELS = {"P1725-AA", "P1724-AA", "P1723-AA", "P1722-AA"}

COLUMN_ORDER = [
    "Customer",
    "GST Treatment",
    "POC",
    "Cabinet Position",
    "Tag",
    "Project Name",
    "Order Lines/Product",
    "Order Lines/Description",
    "Order Lines / Quantity",
]


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

# def compute_quantity(val):
#     try:
#         match = re.search(r"[\d.]+", str(val))
#         if not match:
#             return 1
#         q = float(match.group())
#         if q == 1:
#             return 1
#         return math.ceil(q / 3)
#     except (ValueError, TypeError):
#         return 1




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
            "Order Lines/Product": "PR-047",
            "Order Lines/Description": "PR-047",
            "Order Lines / Quantity": math.ceil(cnt / 5),
        })

    # ── PR-048: total light length in meters, 1 unit per 5 m (rounded up) ──
    length_m = total_light_length_mm / 1000
    if length_m > 0:
        lines.append({
            "Order Lines/Product": "PR-048",
            "Order Lines/Description": "PR-048",
            "Order Lines / Quantity": math.ceil(length_m / 5),
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


# ── Condition Processors ──────────────────────────────────────────────────────

# ── FIX: tt_color and deferred_bom_rows must be module-level globals,
#         never reset inside process_mk_model, only reset per sheet in
#         handle_process_xlsx ─────────────────────────────────────────────────
tt_color = None
deferred_bom_rows = []


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
                     failed_rows, results, customer_meta=None, light_cabinet_count=None):

    # ── FIX: removed `tt_color = None` from here — resetting it per row
    #         was wiping out the value set by a previous LC model ────────────
    global tt_color, deferred_bom_rows, total_light_length

    # if model in LIGHT_CABINET:
    #     if light_cabinet_count is not None:
    #         light_cabinet_count[0] += 1

    cabinet = db.query(Cabinet).filter(Cabinet.cabinet_code == model).first()
    if not cabinet:
        failed_rows.append({
            "Row": index + 1, "Model": model,
            "Cabinet Position": reference,
            "Reason": "Cabinet not found in DB",
        })
        return False

    description = cabinet.description if cabinet.description else model

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


# ── Filler model → category lookup (from filler reference sheet) ─────────────
LC_FILLERS = {
    "FIL-0001", "FIL-0002", "FIL-0003",
    "FIL-0043", "FIL-0044", "FIL-0045",
}

UC_FILLERS = {
    "FIL-0004", "FIL-0005", "FIL-0006", "FIL-0007",
    "FIL-0008", "FIL-0009", "FIL-0058", "FIL-0059",
    "FIL-0060", "FIL-0061",
}

LOFT_FILLERS = {
    "FIL-0010", "FIL-0011", "FIL-0012", "FIL-0013", "FIL-0014",
    "FIL-0015", "FIL-0016", "FIL-0017", "FIL-0018", "FIL-0019",
    "FIL-0020", "FIL-0021", "FIL-0022", "FIL-0023", "FIL-0024",
    "FIL-0025", "FIL-0026", "FIL-0027", "FIL-0028", "FIL-0029",
    "FIL-0030", "FIL-0037", "FIL-0038", "FIL-0039", "FIL-0040",
    "FIL-0041", "FIL-0042", "FIL-0046", "FIL-0047", "FIL-0048",
    "FIL-0049", "FIL-0050", "FIL-0051", "FIL-0052", "FIL-0053",
    "FIL-0054", "FIL-0055", "FIL-0056", "FIL-0057",
}

# Extra filler codes injected per category (qty 1 each)
FILLER_EXTRAS = {
    "LC":   ["FIL-0001"],
    "UC":   ["FIL-0005"],
    "Loft": ["FIL-0056"],
}

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
            extra_product = f"{filler_code}-AD"
            extra_row = {
                "Order Lines/Product":     extra_product,
                "Order Lines/Description": f"[{extra_product}] (Glacier Veil Matte)",
                "Cabinet Position":        reference,
                "Order Lines / Quantity":  1,
            }
            # if customer_meta:
            #     extra_row.update(customer_meta)
            results.append(extra_row)
        processed_categories.add(category)

    return True



# ---------------------------------------------------------------------------
# Gola  ->  skirting mapping  (Sheet1)
# ---------------------------------------------------------------------------
GOLA_TO_SKIRTING = {
    "HG3L-AT": "PVCSE-10-AT-30",
    "HG3L-BG": "PVCSE-10-BG-30",
    "HG3L-SF": "PVCSE-10-BF-30",
    "9299225": "PVCSE-10-BL-30",
    "9345616": "PVCSE-10-SL-30",
    "9345615": "PVCSE-10-BF-30",
}

# ---------------------------------------------------------------------------
# Infurnia code -> odoo_code  (Sheet2)
# Used only as a fallback when the DB lookup does not resolve the code.
# ---------------------------------------------------------------------------
INFURNIA_TO_ODOO = {
    # gola profiles
    "HG3L-AT": "HW-0981",
    "HG3L-BG": "HW-0272",
    "HG3L-SF": "PR-119",
    "9299225": "PR-028",
    "9345616": "PR-105",
    "9345615": "PR-104",
    # skirting seals
    "PVCSE-10-AT-30": "HW-0861",
    "PVCSE-10-BG-30": "HW-0880",
    "PVCSE-10-BF-30": "HW-0872",
    "PVCSE-10-BL-30": "HW-0846",
    "PVCSE-10-SL-30": "HW-0857",
}

# 1 skirting line per 1 gola line. Change if the ratio is not 1:1.
SKIRTING_QTY_RATIO = 1

# Do not add a skirting line if an identical one already exists for the
# same cabinet position (e.g. the input already carried it explicitly).
SKIP_DUPLICATE_SKIRTING = True


def _norm(code):
    """Normalise a product code: strip all whitespace, upper-case.

    Handles the inconsistent spacing in the source sheets, e.g. 'PR -119'
    and 'PR-119' both normalise to 'PR-119'.
    """
    if code is None:
        return ""
    return re.sub(r"\s+", "", str(code)).upper()


# Pre-normalised lookups, built once at import time.
_GOLA_BY_MODEL = {_norm(k): v for k, v in GOLA_TO_SKIRTING.items()}
_ODOO_BY_INFURNIA = {_norm(k): v for k, v in INFURNIA_TO_ODOO.items()}

# Reverse safety net: if `model` arrives in some other form but its resolved
# odoo_code is a known gola odoo_code, we can still find the skirting.
_GOLA_BY_ODOO = {
    _norm(INFURNIA_TO_ODOO[gola]): skirting
    for gola, skirting in GOLA_TO_SKIRTING.items()
    if gola in INFURNIA_TO_ODOO
}


def get_skirting_model(model, odoo_code=None):
    """Return the skirting infurnia code for a gola code, else None."""
    skirting = _GOLA_BY_MODEL.get(_norm(model))
    if not skirting and odoo_code:
        skirting = _GOLA_BY_ODOO.get(_norm(odoo_code))
    return skirting


def resolve_skirting_odoo_code(db, skirting_model, index, reference):
    """Resolve the skirting odoo_code: DB first, static sheet as fallback."""
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
    # Gola profiles always ship with their matching skirting seal.
    # ------------------------------------------------------------------
    skirting_model = get_skirting_model(model, odoo_code)
    if skirting_model:
        skirting_odoo = resolve_skirting_odoo_code(db, skirting_model, index, reference)

        if not skirting_odoo:
            failed_rows.append({
                "index":     index,
                "reference": reference,
                "model":     skirting_model,
                "reason":    f"Skirting odoo_code not found for gola '{model}'",
            })
            return True  # the gola line itself is valid, keep it

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
    return True


def process_row(db, model, finish, quantity, index, reference,
                failed_rows, results, customer_meta, light_cabinet_count,glass_shutter_found=None):
    if model.startswith(("MK-", "CK-2.0")):
        if model in _MK_FIL_MODELS:
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

    elif model in _P_FIL_MODELS:
        success = process_fil_model(db, model, finish, quantity, index, reference,
                                    failed_rows, results, customer_meta)
        if success:
            results.append({
                "Order Lines/Product":     "M-CF-217",
                "Order Lines/Description": "[M-CF-217]",
                "Cabinet Position":        reference,
                "Order Lines / Quantity":  quantity,
            })
        return success

    else:
        return process_generic_model(db, model, quantity, index, reference,
                                     failed_rows, results, customer_meta)


# ── Main handler ──────────────────────────────────────────────────────────────

async def handle_process_xlsx(file: UploadFile, db: Session):
    # ── FIX: reset both tt_color and deferred_bom_rows here, once per sheet ──
    global processed_categories, tt_color, deferred_bom_rows, total_light_length
    processed_categories = set()
    tt_color = None
    deferred_bom_rows = []
    total_light_length = [0]

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
    light_cabinet_count = [0]
     # Use a list to allow mutation within process_mk_model

    for index, row in df.iterrows():
        model     = normalize_text(row["Model"])
        finish    = normalize_text(row["Shutter_Finish"])
        reference = row["Reference"]
        quantity  = row["Quantity"]

        if not model:
            continue

        if is_glass_shutter_model(model):
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
        # success = process_row(db, model, finish, quantity, index, reference,
        #                       failed_rows, results, customer_meta, light_cabinet_count)

        if success and not customer_written:
            customer_written = True

        if success and model.startswith("MK-") and finish in PRELAM_FINISHES:
            prelam_pending.append({
                "result_idx": before_idx,
                "finish":     finish,
                "mk_product": model,
                "row":        index + 1,
                "reference":  reference,
                # ── FIX: do NOT snapshot tt_color here — it may not be set yet
                #         (e.g. C123 appears before any LC model in the sheet).
                #         colour_code is resolved after the main loop, once
                #         tt_color is fully established, in the post-loop block.
            })

    # ── NEW: finalize deferred BOM rows after main loop ───────────────────────
    # Handles the 10% case where no LC model was ever encountered.
    # If tt_color was set by the end of the loop, flushes remaining deferred rows.
    # If not, logs them all to failed_rows.
    finalize_mk_processing(results, failed_rows)

    # ── Post-loop: patch glass description onto MK-prelam rows ───────────────
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
            insert_offset = 0  # ← track how many rows we've inserted so far

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
                    # ── FIX: use tt_color resolved after the full loop, so every
                    #         prelam item (including ones processed before the LC
                    #         model appeared, e.g. C123) gets the correct suffix ──
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
            "Order Lines/Product":     "SR-0001",
            "Order Lines/Description": "[SR-0001]",
            "Order Lines / Quantity":  service_charge_qty,
        })
    else:
        print("Service charge quantity not found; skipping SR-0001 row.")

    # ── PSU row ───────────────────────────────────────────────────────────────
    cnt = light_cabinet_count[0]
    if cnt >= 1:
        # results.extend(get_psu_line(cnt))
        results.extend(get_pr_lines(cnt, total_light_length[0]))

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