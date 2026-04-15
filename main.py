# from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
# from fastapi.responses import StreamingResponse
# from fastapi.middleware.cors import CORSMiddleware
# import pandas as pd
# import re
# import io
# from models import Cabinet, CodeRaw, ColorCode
# from sqlalchemy.orm import Session
# from database import get_db
# from models import Cabinet, ColorCode
# from odoo import get_customer_poc
# import os
# import math
# from dotenv import load_dotenv

# load_dotenv()


# app = FastAPI()

# origins = os.getenv("ALLOWED_ORIGINS", "*")
# origins_list = [origin.strip() for origin in origins.split(",") if origin]

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins_list,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# # ── Utilities ─────────────────────────────────────────────────────────────────

# def normalize_text(value):
#     if value is None or pd.isna(value):
#         return None
#     value = str(value).strip()
#     return value if value else None


# # ── Mappings ──────────────────────────────────────────────────────────────────

# SHUTTER_FINISH_MAPPING = {
#     "Sandalwood":      "Courtyard Clay Gloss",
#     "Soundcloud":      "Mistfield Gloss",
#     "Washed Earth":    "Canyon Ridge Gloss",
#     "Starlight White": "Glacier Veil Gloss",
#     "Asteroid Belt":   "Industrial Bay Matte",
# }

# PRELAM_FINISHES = {
#     "Back Painted Fluted Glass Ivory Matt (Prelam)",
#     "Back Painted Fluted Glass Ash Matt (Prelam)",
#     "Back Painted Fluted Glass Biscuit Matt (Prelam)",
#     "Back Painted Fluted Glass Maple Bronze Gloss (Prelam)",
#     "Back Painted Frosted Glass Beige Matt (Prelam)",
#     "Back Painted Frosted Glass Graphite Matt (Prelam)",
#     "Back Painted Sandstone Gloss (Prelam)",
#     "Back Painted Pebble Gloss (Prelam)",
#     "Fluted Glass Vanilla Matt (Prelam)",
#     "Fluted Glass Coffee Matt (Prelam)",
#     "Fluted Glass Onyx Matt (Prelam)",
#     "Fluted Glass Snow Gloss (Prelam)",
#     "Fluted Glass Caramel Gloss (Prelam)",
#     "Fluted Glass Black Gloss (Prelam)",
#     "Sandwich Glass Bronze Veil (Prelam)",
#     "Frosted Glass Mist (Prelam)",
# }

# # Maps glass-shutter model codes -> human-readable profile description
# GLASS_SHUTTER_PROFILE_MAPPING = {
#     "KAPS-59 MB": "GLASS SHUTTER PROFILE: Matt Black ( KAPS-59 MB )",
#     "KAPS-59 MG": "GLASS SHUTTER PROFILE: Matt Gold ( KAPS-59 MG )",
#     "KAPS-59 SS": "GLASS SHUTTER PROFILE: Silver ( KAPS-59 SS )",
#     "SCP-06 MB":  "GLASS SHUTTER PROFILE: Matt Black ( SCP-06 MB )",
#     "SCP-06 MG":  "GLASS SHUTTER PROFILE: Matt Gold ( SCP-06 MG )",
#     "SCP-06 SS":  "GLASS SHUTTER PROFILE: Silver ( SCP-06 SS )",
#     "KSP-01 MB":  "GLASS SHUTTER PROFILE: Matt Black ( KSP-01 MB )",
#     "KSP-01 MG":  "GLASS SHUTTER PROFILE: Matt Gold ( KSP-01 MG )",
#     "KSP-01 SS":  "GLASS SHUTTER PROFILE: Silver ( KSP-01 SS )",
#     "BGK-01":     "GLASS SHUTTER PROFILE: Rose Gold 20 mm Profile",
# }

# GLASS_SHUTTER_MODELS = set(GLASS_SHUTTER_PROFILE_MAPPING.keys())


# # ── Extractors ────────────────────────────────────────────────────────────────

# def is_glass_shutter_model(model: str | None) -> bool:
#     return model in GLASS_SHUTTER_MODELS


# def extract_model(text: str | None):
#     if text is None or (isinstance(text, float) and pd.isna(text)):
#         return None
#     # Capture full model name until newline — handles "MK-0458" and "KSP-01 MG"
#     m = re.search(r"Model:\s*(.+?)(?:\n|$)", str(text))
#     return m.group(1).strip() if m else None


# def extract_shutter_finish(text: str | None):
#     if text is None or (isinstance(text, float) and pd.isna(text)):
#         return None

#     s = str(text)

#     # Primary: "Shutter Finish : <value>"
#     m = re.search(r"Shutter.*?Finish\s*:\s*(.+?)(?:\n|$)", s, re.DOTALL)
#     if m:
#         finish = m.group(1).strip()
#         return SHUTTER_FINISH_MAPPING.get(finish, finish)

#     # Fallback 1: cell directly contains a known Prelam finish string
#     s_stripped = s.strip()
#     if s_stripped in PRELAM_FINISHES:
#         return s_stripped

#     # Fallback 2: generic "Finish : <value>" without "Shutter" prefix
#     m2 = re.search(r"Finish\s*:\s*(.+?)(?:\n|$)", s, re.DOTALL)
#     if m2:
#         finish = m2.group(1).strip()
#         return SHUTTER_FINISH_MAPPING.get(finish, finish)

#     return None


# def build_glass_shutter_description(mk_product: str, glass_model: str, prelam_finish: str) -> str:
#     """
#     Builds the 3-line Order Lines/Description for a glass-shutter pair:
#         [MK-XXXX]
#         GLASS SHUTTER PROFILE: <profile label>
#         GLASS PROFILE: <prelam finish>
#     """
#     profile_label = GLASS_SHUTTER_PROFILE_MAPPING.get(glass_model, glass_model)
#     return f"[{mk_product}]\n{profile_label}\nGLASS PROFILE: {prelam_finish}"


# # ── FastAPI endpoint ───────────────────────────────────────────────────────────

# @app.post("/process-xlsx")
# async def process_xlsx(
#     file: UploadFile = File(...),
#     db: Session = Depends(get_db)
# ):
#     if not file.filename.lower().endswith(".xlsx"):
#         raise HTTPException(status_code=400, detail="Please upload a .xlsx file")

#     contents = await file.read()

#     try:
#         excel_bytes = io.BytesIO(contents)
#         raw_df = pd.read_excel(excel_bytes, sheet_name=0, header=None, engine="openpyxl")
#         df     = pd.read_excel(excel_bytes, header=2, engine="openpyxl")
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Unable to read Excel file: {e}")

#     # ── Column validation ─────────────────────────────────────────────────────
#     df.columns = df.columns.astype(str).str.strip()
#     required_cols = ["Reference", "Item", "Finishes"]
#     missing = [c for c in required_cols if c not in df.columns]
#     if missing:
#         raise HTTPException(status_code=400, detail=f"Missing columns in sheet: {missing}")

#     # ── Quantity column (immediately right of "Finishes") ─────────────────────
#     try:
#         finishes_col_idx = df.columns.get_loc("Finishes")
#         quantity_col     = df.columns[finishes_col_idx + 1]
#     except (KeyError, IndexError):
#         raise HTTPException(
#             status_code=400,
#             detail="Could not find the Quantity column (expected right after 'Finishes')"
#         )

#     def compute_quantity(val):
#         try:
#             match = re.search(r"[\d.]+", str(val))
#             if not match:
#                 return 1
#             q = float(match.group())
#             if q == 1:
#                 return 1
#             return math.ceil(q / 3) + 1
#         except (ValueError, TypeError):
#             return 1

#     df["Quantity"] = df[quantity_col].apply(compute_quantity)

#     # ── Project ID ────────────────────────────────────────────────────────────
#     try:
#         cell_value       = str(raw_df.iloc[1, 2])
#         project_id_match = re.search(r"^\s*(\d+)", cell_value)
#         if not project_id_match:
#             raise HTTPException(status_code=400, detail="Project ID not found in the file")
#         project_id = project_id_match.group(1)
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Error extracting Project ID: {e}")

#     crm_id = project_id
#     print(f"Fetching customer and POC details for CRM ID: {crm_id}")
#     project_name, customer, poc = get_customer_poc(crm_id)
#     if project_name is None:
#         print(f"No CRM lead found for ID: {crm_id}, skipping...")

#     # ── Derive model / finish / reference columns ─────────────────────────────
#     df["Model"]          = df["Item"].apply(extract_model)
#     df["Shutter_Finish"] = df["Finishes"].apply(extract_shutter_finish)
#     df["Reference"]      = df["Reference"].apply(normalize_text)

#     results     = []
#     failed_rows = []

#     # ── Service Charges quantity ──────────────────────────────────────────────
#     service_charge_qty = None
#     try:
#         for i, row in raw_df.iterrows():
#             for j, cell in enumerate(row):
#                 if str(cell).strip() == "Service Charges":
#                     header_row  = raw_df.iloc[i + 1]
#                     qty_col_idx = None
#                     for col_idx, col_val in enumerate(header_row):
#                         if str(col_val).strip() == "Quantity":
#                             qty_col_idx = col_idx
#                             break
#                     if qty_col_idx is not None:
#                         data_row = raw_df.iloc[i + 2]
#                         raw_qty  = data_row.iloc[qty_col_idx]
#                         match    = re.search(r"[\d.]+", str(raw_qty))
#                         if match:
#                             service_charge_qty = float(match.group())
#                     break
#             if service_charge_qty is not None:
#                 break
#     except Exception as e:
#         print(f"Could not extract Service Charges quantity: {e}")

#     # ── PRE-SCAN: collect all glass-shutter model rows present in this sheet ──
#     # Glass-shutter rows (KSP-01 MG, KAPS-59 MB, etc.) can appear ANYWHERE —
#     # before, between, or after the MK-prelam rows they describe.
#     # We scan once upfront so the main loop never needs fragile sequential pairing.
#     glass_shutter_found = []   # ordered list of model strings as they appear
#     for _, scan_row in df.iterrows():
#         m = normalize_text(scan_row["Model"])
#         if is_glass_shutter_model(m) and m not in glass_shutter_found:
#             glass_shutter_found.append(m)

#     print(f"Glass-shutter models found in sheet: {glass_shutter_found}")

#     # ── Helpers ───────────────────────────────────────────────────────────────

#     def get_colour_code(db, finish, model, index, reference, failed_rows):
#         colour = db.query(ColorCode).filter(ColorCode.colour_name == finish).first()
#         if not colour:
#             failed_rows.append({
#                 "Row": index + 1,
#                 "Model": model,
#                 "Cabinet Position": reference,
#                 "Reason": f"Cabinet processed but could not find colour '{finish}'"
#             })
#             return None
#         return colour.colour_code

#     def get_odoo_code(db, model, index, reference, failed_rows):
#         mapping = db.query(CodeRaw).filter(CodeRaw.infurnia_code == model).first()
#         if not mapping:
#             failed_rows.append({
#                 "Row": index + 1,
#                 "Model": model,
#                 "Cabinet Position": reference,
#                 "Reason": f"No mapping found in code_raw for model '{model}'"
#             })
#             return None
#         return mapping.odoo_code

#     # ── Condition Processors ──────────────────────────────────────────────────

#     def process_mk_model(db, model, finish, quantity, index, reference,
#                          failed_rows, results, customer_meta=None):
#         cabinet = db.query(Cabinet).filter(Cabinet.cabinet_code == model).first()
#         if not cabinet:
#             failed_rows.append({
#                 "Row": index + 1, "Model": model,
#                 "Cabinet Position": reference,
#                 "Reason": "Cabinet not found in DB"
#             })
#             return False

#         first_row = {
#             "Order Lines/Product":    model,
#             "Cabinet Position":       reference,
#             "Order Lines / Quantity": quantity,
#         }
#         if customer_meta:
#             first_row.update(customer_meta)

#         results.append(first_row)

#         # Prelam finishes are glass profiles — skip BOM colour lookup entirely
#         if finish in PRELAM_FINISHES:
#             return True

#         colour_code = get_colour_code(db, finish, model, index, reference, failed_rows)
#         if not colour_code:
#             return True

#         for bom in [cabinet.bom_line_1, cabinet.bom_line_2,
#                     cabinet.bom_line_3, cabinet.bom_line_4]:
#             if bom:
#                 results.append({
#                     "Order Lines/Product":    f"{bom}-{colour_code}",
#                     "Cabinet Position":       reference,
#                     "Order Lines / Quantity": quantity,
#                 })
#         return True

#     def process_fil_model(db, model, finish, quantity, index, reference, failed_rows, results):
#         colour_code = get_colour_code(db, finish, model, index, reference, failed_rows)
#         if not colour_code:
#             return False

#         results.append({
#             "Order Lines/Product":    f"{model}-{colour_code}",
#             "Cabinet Position":       reference,
#             "Order Lines / Quantity": quantity,
#         })
#         return True

#     def process_generic_model(db, model, quantity, index, reference, failed_rows, results):
#         odoo_code = get_odoo_code(db, model, index, reference, failed_rows)
#         if not odoo_code:
#             return False

#         results.append({
#             "Order Lines/Product":    odoo_code,
#             "Cabinet Position":       reference,
#             "Order Lines / Quantity": quantity,
#         })
#         return True

#     def process_row(db, model, finish, quantity, index, reference,
#                     failed_rows, results, customer_meta):
#         """Route to the correct handler based on model prefix."""
#         if model.startswith("MK-"):
#             return process_mk_model(db, model, finish, quantity, index, reference,
#                                     failed_rows, results, customer_meta)
#         elif model.startswith("FIL-"):
#             return process_fil_model(db, model, finish, quantity, index, reference,
#                                      failed_rows, results)
#         elif model.startswith("EP-"):
#             return process_fil_model(db, model, finish, quantity, index, reference,
#                                      failed_rows, results)
#         else:
#             return process_generic_model(db, model, quantity, index, reference,
#                                          failed_rows, results)

#     # ── Main loop ─────────────────────────────────────────────────────────────
#     # Glass-shutter model rows are SKIPPED here — they produce no output of
#     # their own. Their model code was already captured in the pre-scan above.
#     # After the loop, all collected MK-prelam result rows are patched with the
#     # 3-line description using the pre-scanned glass-shutter model.
#     customer_written = False
#     prelam_pending   = []  # [{result_idx, finish, mk_product, row, reference}]

#     for index, row in df.iterrows():
#         model     = normalize_text(row["Model"])
#         finish    = normalize_text(row["Shutter_Finish"])
#         reference = row["Reference"]
#         quantity  = row["Quantity"]

#         if not model:
#             continue

#         # Glass-shutter rows: skip — handled entirely via pre-scan + post-loop patch
#         if is_glass_shutter_model(model):
#             continue

#         # Standard finish-missing validation
#         if model.startswith(("MK-", "FIL-", "EP-")) and not finish:
#             failed_rows.append({
#                 "Row": index + 1, "Model": model,
#                 "Cabinet Position": reference, "Reason": "Finish missing"
#             })
#             continue

#         customer_meta = None
#         if not customer_written:
#             customer_meta = {
#                 "Customer":      customer or "Default Customer",
#                 "GST Treatment": "Consumer",
#                 "POC":           poc or "Default POC",
#                 "Tag":           "Product",
#                 "Project Name":  project_name or "Default Project Name",
#             }

#         before_idx = len(results)
#         success = process_row(db, model, finish, quantity, index, reference,
#                               failed_rows, results, customer_meta)

#         if success and not customer_written:
#             customer_written = True

#         # Track MK-prelam rows so we can patch their description after the loop
#         if success and model.startswith("MK-") and finish in PRELAM_FINISHES:
#             prelam_pending.append({
#                 "result_idx": before_idx,
#                 "finish":     finish,
#                 "mk_product": results[before_idx]["Order Lines/Product"],
#                 "row":        index + 1,
#                 "reference":  reference,
#             })

#     # ── POST-LOOP: patch descriptions onto every MK-prelam row ────────────────
#     # All prelam rows in the same sheet share the same glass-shutter model
#     # (there is typically only one per order). If the sheet ever contains
#     # multiple glass-shutter models, extend this logic as needed.
#     if prelam_pending:
#         if not glass_shutter_found:
#             for item in prelam_pending:
#                 failed_rows.append({
#                     "Row":              item["row"],
#                     "Model":            item["mk_product"],
#                     "Cabinet Position": item["reference"],
#                     "Reason":           "Prelam finish found but no glass-shutter profile model row exists in the sheet",
#                 })
#         else:
#             glass_model = glass_shutter_found[0]  # use first found (typically only one)
#             for item in prelam_pending:
#                 description = build_glass_shutter_description(
#                     item["mk_product"], glass_model, item["finish"]
#                 )
#                 results[item["result_idx"]]["Order Lines/Description"] = description

#     # ── Service charge row ────────────────────────────────────────────────────
#     if service_charge_qty is not None:
#         results.append({
#             "Cabinet Position":       "B2C Installation Service",
#             "Order Lines/Product":    "SR-0001",
#             "Order Lines / Quantity": service_charge_qty,
#         })
#     else:
#         print("Service charge quantity not found; skipping SR-0001 row.")

#     # ── Build output workbook ─────────────────────────────────────────────────
#     output = io.BytesIO()

#     COLUMN_ORDER = [
#         "Customer",
#         "GST Treatment",
#         "POC",
#         "Cabinet Position",
#         "Tag",
#         "Project Name",
#         "Order Lines/Product",
#         "Order Lines/Description",
#         "Order Lines / Quantity",
#     ]

#     with pd.ExcelWriter(output, engine="openpyxl") as writer:
#         if results:
#             pd.DataFrame(results, columns=COLUMN_ORDER).to_excel(
#                 writer, sheet_name="Success", index=False
#             )
#         if failed_rows:
#             pd.DataFrame(failed_rows).to_excel(
#                 writer, sheet_name="Failed", index=False
#             )

#     output.seek(0)

#     return StreamingResponse(
#         output,
#         media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#         headers={"Content-Disposition": 'attachment; filename="processed_output.xlsx"'}
#     )


from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import re
import io
from models import Cabinet, CodeRaw, ColorCode
from sqlalchemy.orm import Session
from database import get_db
from models import Cabinet, ColorCode
from odoo import get_customer_poc
import os
import math
from dotenv import load_dotenv

load_dotenv()


app = FastAPI()

origins = os.getenv("ALLOWED_ORIGINS", "*")
origins_list = [origin.strip() for origin in origins.split(",") if origin]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Utilities ─────────────────────────────────────────────────────────────────

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
    "Frosted Glass Mist (Prelam)",
}

# Maps glass-shutter model codes -> human-readable profile description
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
    "BGK-01":     "GLASS SHUTTER PROFILE: Rose Gold 20 mm Profile",
}

GLASS_SHUTTER_MODELS = set(GLASS_SHUTTER_PROFILE_MAPPING.keys())


# ── Extractors ────────────────────────────────────────────────────────────────

def is_glass_shutter_model(model: str | None) -> bool:
    return model in GLASS_SHUTTER_MODELS


def extract_model(text: str | None):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    # Captures full model name until newline — handles "MK-0458" and "KSP-01 MG"
    m = re.search(r"Model:\s*(.+?)(?:\n|$)", str(text))
    return m.group(1).strip() if m else None


def extract_shutter_finish(text: str | None):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None

    s = str(text)

    # Primary: "Shutter Finish : <value>"
    m = re.search(r"Shutter.*?Finish\s*:\s*(.+?)(?:\n|$)", s, re.DOTALL)
    if m:
        finish = m.group(1).strip()
        return SHUTTER_FINISH_MAPPING.get(finish, finish)

    # Fallback 1: cell directly contains a known Prelam finish string
    s_stripped = s.strip()
    if s_stripped in PRELAM_FINISHES:
        return s_stripped

    # Fallback 2: generic "Finish : <value>" without "Shutter" prefix
    m2 = re.search(r"Finish\s*:\s*(.+?)(?:\n|$)", s, re.DOTALL)
    if m2:
        finish = m2.group(1).strip()
        return SHUTTER_FINISH_MAPPING.get(finish, finish)

    return None


def build_glass_shutter_description(mk_product: str, glass_model: str, prelam_finish: str) -> str:
    """
    3-line description for a glass-shutter MK-prelam row:
        [MK-XXXX]
        GLASS SHUTTER PROFILE: <profile label>
        GLASS PROFILE: <prelam finish>
    """
    profile_label = GLASS_SHUTTER_PROFILE_MAPPING.get(glass_model, glass_model)
    return f"[{mk_product}]\n{profile_label}\nGLASS PROFILE: {prelam_finish}"


# ── FastAPI endpoint ───────────────────────────────────────────────────────────

@app.post("/process-xlsx")
async def process_xlsx(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
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

    # ── Quantity column (immediately right of "Finishes") ─────────────────────
    try:
        finishes_col_idx = df.columns.get_loc("Finishes")
        quantity_col     = df.columns[finishes_col_idx + 1]
    except (KeyError, IndexError):
        raise HTTPException(
            status_code=400,
            detail="Could not find the Quantity column (expected right after 'Finishes')"
        )

    def compute_quantity(val):
        try:
            match = re.search(r"[\d.]+", str(val))
            if not match:
                return 1
            q = float(match.group())
            if q == 1:
                return 1
            return math.ceil(q / 3) + 1
        except (ValueError, TypeError):
            return 1

    df["Quantity"] = df[quantity_col].apply(compute_quantity)

    # ── Project ID ────────────────────────────────────────────────────────────
    try:
        cell_value       = str(raw_df.iloc[1, 2])
        project_id_match = re.search(r"^\s*(\d+)", cell_value)
        if not project_id_match:
            raise HTTPException(status_code=400, detail="Project ID not found in the file")
        project_id = project_id_match.group(1)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error extracting Project ID: {e}")

    crm_id = project_id
    print(f"Fetching customer and POC details for CRM ID: {crm_id}")
    project_name, customer, poc = get_customer_poc(crm_id)
    if project_name is None:
        print(f"No CRM lead found for ID: {crm_id}, skipping...")

    # ── Derive model / finish / reference columns ─────────────────────────────
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

    # ── PRE-SCAN: collect all glass-shutter model rows present in this sheet ──
    # Glass-shutter rows can appear ANYWHERE — before, between, or after the
    # MK-prelam rows they describe. Scan once upfront; main loop skips them.
    glass_shutter_found = []
    for _, scan_row in df.iterrows():
        m = normalize_text(scan_row["Model"])
        if is_glass_shutter_model(m) and m not in glass_shutter_found:
            glass_shutter_found.append(m)

    print(f"Glass-shutter models found in sheet: {glass_shutter_found}")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def get_colour_code(db, finish, model, index, reference, failed_rows):
        colour = db.query(ColorCode).filter(ColorCode.colour_name == finish).first()
        if not colour:
            failed_rows.append({
                "Row": index + 1,
                "Model": model,
                "Cabinet Position": reference,
                "Reason": f"Cabinet processed but could not find colour '{finish}'"
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
                "Reason": f"No mapping found in code_raw for model '{model}'"
            })
            return None
        return mapping.odoo_code

    # ── Condition Processors ──────────────────────────────────────────────────

    def process_mk_model(db, model, finish, quantity, index, reference,
                         failed_rows, results, customer_meta=None):
        cabinet = db.query(Cabinet).filter(Cabinet.cabinet_code == model).first()
        if not cabinet:
            failed_rows.append({
                "Row": index + 1, "Model": model,
                "Cabinet Position": reference,
                "Reason": "Cabinet not found in DB"
            })
            return False

        first_row = {
            "Order Lines/Product":      model,
            # MK cabinet row: description is just the model code (no brackets).
            # Prelam rows will have this overwritten in the post-loop patch.
            "Order Lines/Description":  model,
            "Cabinet Position":         reference,
            "Order Lines / Quantity":   quantity,
        }
        if customer_meta:
            first_row.update(customer_meta)

        results.append(first_row)

        # Prelam finishes are glass profiles — skip BOM colour lookup entirely.
        # The description will be patched after the main loop.
        if finish in PRELAM_FINISHES:
            return True

        colour_code = get_colour_code(db, finish, model, index, reference, failed_rows)
        if not colour_code:
            return True

        for bom in [cabinet.bom_line_1, cabinet.bom_line_2,
                    cabinet.bom_line_3, cabinet.bom_line_4]:
            if bom:
                product = f"{bom}-{colour_code}"
                results.append({
                    "Order Lines/Product":      product,
                    # BOM line: [product_code] (finish_name)
                    "Order Lines/Description":  f"[{product}] ({finish})",
                    "Cabinet Position":         reference,
                    "Order Lines / Quantity":   quantity,
                })
        return True

    def process_fil_model(db, model, finish, quantity, index, reference, failed_rows, results):
        colour_code = get_colour_code(db, finish, model, index, reference, failed_rows)
        if not colour_code:
            return False

        product = f"{model}-{colour_code}"
        results.append({
            "Order Lines/Product":      product,
            # FIL / EP line: [product_code] (finish_name)
            "Order Lines/Description":  f"[{product}] ({finish})",
            "Cabinet Position":         reference,
            "Order Lines / Quantity":   quantity,
        })
        return True

    def process_generic_model(db, model, quantity, index, reference, failed_rows, results):
        odoo_code = get_odoo_code(db, model, index, reference, failed_rows)
        if not odoo_code:
            return False

        results.append({
            "Order Lines/Product":      odoo_code,
            # Generic row: [product_code] — no finish
            "Order Lines/Description":  f"[{odoo_code}]",
            "Cabinet Position":         reference,
            "Order Lines / Quantity":   quantity,
        })
        return True

    def process_row(db, model, finish, quantity, index, reference,
                    failed_rows, results, customer_meta):
        """Route to the correct handler based on model prefix."""
        if model.startswith("MK-"):
            return process_mk_model(db, model, finish, quantity, index, reference,
                                    failed_rows, results, customer_meta)
        elif model.startswith("FIL-"):
            return process_fil_model(db, model, finish, quantity, index, reference,
                                     failed_rows, results)
        elif model.startswith("EP-"):
            return process_fil_model(db, model, finish, quantity, index, reference,
                                     failed_rows, results)
        else:
            return process_generic_model(db, model, quantity, index, reference,
                                         failed_rows, results)

    # ── Main loop ─────────────────────────────────────────────────────────────
    customer_written = False
    prelam_pending   = []  # [{result_idx, finish, mk_product, row, reference}]

    for index, row in df.iterrows():
        model     = normalize_text(row["Model"])
        finish    = normalize_text(row["Shutter_Finish"])
        reference = row["Reference"]
        quantity  = row["Quantity"]

        if not model:
            continue

        # Glass-shutter rows produce no output — handled via pre-scan + post-loop
        if is_glass_shutter_model(model):
            continue

        # Standard finish-missing validation
        if model.startswith(("MK-", "FIL-", "EP-")) and not finish:
            failed_rows.append({
                "Row": index + 1, "Model": model,
                "Cabinet Position": reference, "Reason": "Finish missing"
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
                              failed_rows, results, customer_meta)

        if success and not customer_written:
            customer_written = True

        # Track MK-prelam rows for post-loop description patch
        if success and model.startswith("MK-") and finish in PRELAM_FINISHES:
            prelam_pending.append({
                "result_idx": before_idx,
                "finish":     finish,
                "mk_product": results[before_idx]["Order Lines/Product"],
                "row":        index + 1,
                "reference":  reference,
            })

    # ── POST-LOOP: patch 3-line glass description onto every MK-prelam row ────
    # All prelam rows in the sheet share the same single glass-shutter model.
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
            glass_model = glass_shutter_found[0]  # one model shared by all prelam rows
            for item in prelam_pending:
                results[item["result_idx"]]["Order Lines/Description"] = (
                    build_glass_shutter_description(
                        item["mk_product"], glass_model, item["finish"]
                    )
                )

    # ── Service charge row ────────────────────────────────────────────────────
    if service_charge_qty is not None:
        results.append({
            "Cabinet Position":         "B2C Installation Service",
            "Order Lines/Product":      "SR-0001",
            "Order Lines/Description":  "[SR-0001]",
            "Order Lines / Quantity":   service_charge_qty,
        })
    else:
        print("Service charge quantity not found; skipping SR-0001 row.")

    # ── Build output workbook ─────────────────────────────────────────────────
    output = io.BytesIO()

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
        headers={"Content-Disposition": 'attachment; filename="processed_output.xlsx"'}
    )