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
    "Fluted Glass Ridge (Prelam)",
    "Fluted Glass Fine Ridge (Prelam)",
    "Textured Glass Glacier (Prelam)",
    "Clear Glass (Prelam)",
    "Black Tinted Glass (Prelam)",
    "Clear Fluted Glass (Prelam)"




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
    "BGK-04":     "GLASS SHUTTER PROFILE: Gold Profile",
    "BGK-05":     "GLASS SHUTTER PROFILE: Black Profile",
    "BGK-07":     "GLASS SHUTTER PROFILE: Champagne Profile",
    "BGK-06":     "GLASS SHUTTER PROFILE: Silver Profile",
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

    light_cabinet = [
    "MK-0619", "MK-0946", "MK-0469", "MK-0940", "MK-0620", "MK-0947",
    "MK-0614", "MK-0941", "MK-0621", "MK-0948", "MK-0470", "MK-0942",
    "MK-0697", "MK-0969", "MK-0626", "MK-0965", "MK-0465", "MK-0972",
    "MK-0714", "MK-0971", "MK-0616", "MK-0954", "MK-0615", "MK-0943",
    "MK-0462", "MK-0968", "MK-0625", "MK-0966", "MK-1071", "MK-1070",
    "MK-1068", "MK-1069", "MK-0617", "MK-0955", "MK-1072", "MK-0979",
    "MK-0810", "MK-0975", "MK-0455", "MK-0944", "MK-0458", "MK-0970",
    "MK-0457", "MK-0974", "MK-0522", "MK-0973", "MK-0523", "MK-0967"
    ]



    def get_psu_line(cnt):
        """Return the PSU product line dict based on light cabinet count."""
        if 1 <= cnt <= 4:
            product = "PSU2-2460"
            qty = 1
        elif 5 <= cnt <= 6:
            product = "PSU3-2465"
            qty = 1
        elif 7 <= cnt <= 8:
            product = "PSU2-2460"
            qty = 2
        else:  # cnt >= 9
            product = "PSU3-2465"
            qty = 2

        return {
            "Order Lines/Product":     product,
            "Order Lines/Description": product,
            # "Cabinet Position":        "",
            "Order Lines / Quantity":  qty,
        }
    

    def process_mk_model(db, model, finish, quantity, index, reference,
                         failed_rows, results, customer_meta=None, light_cabinet_count=None):
        
        if model in light_cabinet:
            if light_cabinet_count is not None:
                light_cabinet_count[0] += 1

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

        # for bom in [cabinet.bom_line_1, cabinet.bom_line_2,
        #             cabinet.bom_line_3, cabinet.bom_line_4]:
        #     if bom:
        #         product = f"{bom}-{colour_code}"
        #         results.append({
        #             "Order Lines/Product":      product,
        #             # BOM line: [product_code] (finish_name)
        #             "Order Lines/Description":  f"[{product}] ({finish})",
        #             "Cabinet Position":         reference,
        #             "Order Lines / Quantity":   quantity,
        #         })

        for i in range(1, 14):
            bom = getattr(cabinet, f"bom_line_{i}")

            if bom:
                product = f"{bom}-{colour_code}"

                results.append({
                    "Order Lines/Product":      product,
                    "Order Lines/Description":  f"[{product}] ({finish})",
                    "Cabinet Position":         reference,
                    "Order Lines / Quantity":   quantity,
                })
        return True
    

    _P_FIL_MODELS = {"P1725-AA", "P1724-AA", "P1723-AA", "P1722-AA"}

    def process_fil_model(db, model, finish, quantity, index, reference, failed_rows, results, customer_meta=None):
        colour_code = get_colour_code(db, finish, model, index, reference, failed_rows)
        if not colour_code:
            return False

        product = f"{model}-{colour_code}"
        first_row = {
            "Order Lines/Product":      product,
            # FIL / EP line: [product_code] (finish_name)
            "Order Lines/Description":  f"[{product}] ({finish})",
            "Cabinet Position":         reference,
            "Order Lines / Quantity":   quantity,
        }


        if customer_meta:
            first_row.update(customer_meta)

        results.append(first_row)
        return True

    def process_generic_model(db, model, quantity, index, reference, failed_rows, results, customer_meta=None):
        odoo_code = get_odoo_code(db, model, index, reference, failed_rows)
        if not odoo_code:
            return False

        first_row = {
            "Order Lines/Product":      odoo_code,
            # Generic row: [product_code] — no finish
            "Order Lines/Description":  f"[{odoo_code}]",
            "Cabinet Position":         reference,
            "Order Lines / Quantity":   quantity,
        }

        if customer_meta:
            first_row.update(customer_meta)

        results.append(first_row)
        return True
    

    light_cabinet_count = [0]

    _MK_FIL_MODELS = {
    "MK-0777", "MK-1145", "MK-0766", "MK-0834",
    "MK-0775", "MK-0835", "MK-0725", "MK-0836"
    }

    def process_row(db, model, finish, quantity, index, reference,
                    failed_rows, results,customer_meta,  light_cabinet_count,):
        """Route to the correct handler based on model prefix."""


        if model.startswith("MK-"):
            if model in _MK_FIL_MODELS:
                return process_fil_model(db, model, finish, quantity, index, reference,
                                        failed_rows, results, customer_meta)
            return process_mk_model(db, model, finish, quantity, index, reference,
                        failed_rows, results, customer_meta, light_cabinet_count=light_cabinet_count)

        elif model.startswith("FIL-"):
            return process_fil_model(db, model, finish, quantity, index, reference,
                                     failed_rows, results, customer_meta)
        elif model.startswith("EP-"):
            return process_fil_model(db, model, finish, quantity, index, reference,
                                     failed_rows, results, customer_meta)
        

        elif model in _P_FIL_MODELS:                          # ← new branch
            success = process_fil_model(db, model, finish, quantity, index, reference,
                                    failed_rows, results, customer_meta)
            if success:                                        # only append if FIL succeeded
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
                              failed_rows, results,customer_meta,  light_cabinet_count)

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


    cnt = light_cabinet_count[0]
    if cnt >= 1:
        psu_row = get_psu_line(cnt)
        results.append(psu_row)

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