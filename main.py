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

app = FastAPI()

# ✅ Adjust origins to your frontend URL
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    # allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def normalize_text(value):
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    return value if value else None



def extract_model(text: str | None):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    m = re.search(r"Model:\s*([A-Za-z0-9\-]+)", str(text))
    return m.group(1) if m else None


def extract_shutter_finish(text: str | None):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None

    s = str(text)
    m = re.search(r"Shutter.*?Finish\s*:\s*(.+?)(?:\n|$)", s, re.DOTALL)
    return m.group(1).strip() if m else None


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
        df = pd.read_excel(excel_bytes, header=2, engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to read Excel file: {e}")

    # Validate required columns
    df.columns = df.columns.astype(str).str.strip()
    required_cols = ["Reference", "Item", "Finishes"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing columns in sheet: {missing}")

    # Extract Project ID from column C (index 2), row 2 (index 1)
    try:
        cell_value = str(raw_df.iloc[1, 2])
        project_id_match = re.search(r"^\s*(\d+)", cell_value)
        if not project_id_match:
            raise HTTPException(status_code=400, detail="Project ID not found in the file")
        project_id = project_id_match.group(1)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error extracting Project ID: {e}")

    crm_id = project_id

    # Fetch customer and POC details using crm_id
    customer, poc = get_customer_poc(crm_id)

    # Apply extraction and normalization functions
    df["Model"] = df["Item"].apply(extract_model)
    df["Shutter_Finish"] = df["Finishes"].apply(extract_shutter_finish)
    df["Reference"] = df["Reference"].apply(normalize_text)

    results = []
    failed_rows = []


    # ── Helpers ───────────────────────────────────────────────────────────────────

    def get_colour_code(db, finish, model, index, reference, failed_rows):
        """Fetch colour code for a given finish name."""
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
        """Fetch mapped odoo_code from code_raw table using infurnia_code."""
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


    # ── Condition Processors ──────────────────────────────────────────────────────

    def process_mk_model(db, model, finish, index, reference, failed_rows, results):
        """
        Condition 1: Model starts with 'MK-'
        Original logic — look up cabinet, get BOM lines, append model + bom-colour_code rows.
        """
        cabinet = db.query(Cabinet).filter(Cabinet.cabinet_code == model).first()
        if not cabinet:
            failed_rows.append({
                "Row": index + 1,
                "Model": model,
                "Cabinet Position": reference,
                "Reason": "Cabinet not found in DB"
            })
            return False
        

        results.append({
            "Order Lines/Product": model,
            "Cabinet Position": reference,
        })

        colour_code = get_colour_code(db, finish, model, index, reference, failed_rows)

        if not colour_code:
            return False

        bom_lines = [cabinet.bom_line_1, cabinet.bom_line_2, cabinet.bom_line_3]

        # if not any(bom_lines):
        #     failed_rows.append({
        #         "Row": index + 1,
        #         "Model": model,
        #         "Cabinet Position": reference,
        #         "Reason": "No BOM lines found"
        #     })
        #     return False

        # Keep model as its own row
        # results.append({
        #     "Order Lines/Product": model,
        #     "Cabinet Position": reference,
        # })

        for bom in bom_lines:
            if bom:
                results.append({
                    "Order Lines/Product": f"{bom}-{colour_code}",
                    "Cabinet Position": reference,
                })
        return True


    def process_fil_model(db, model, finish, index, reference, failed_rows, results):
        """
        Condition 2: Model starts with 'FIL-'
        Concatenate model + colour_code suffix and push both the model row and the final code.
        e.g. FIL-0043 + finish 'Industrial Bay Matte' (AB) → model row + 'FIL-0043-AB'
        """
        colour_code = get_colour_code(db, finish, model, index, reference, failed_rows)
        if not colour_code:
            return False

        final_code = f"{model}-{colour_code}"

        # Push the concatenated final code
        results.append({
            "Order Lines/Product": final_code,
            "Cabinet Position": reference,
        })
        return True


    def process_generic_model(db, model, index, reference, failed_rows, results):
        """
        Condition 3: All other models.
        Look up infurnia_code → odoo_code in code_raw and push both the model row and the mapped code.
        """
        odoo_code = get_odoo_code(db, model, index, reference, failed_rows)
        if not odoo_code:
            return False

        # Push the mapped odoo code
        results.append({
            "Order Lines/Product": odoo_code,
            "Cabinet Position": reference,
        })
        return True


    def process_row(db, model, finish, index, reference, failed_rows, results):
        """Route to the correct handler based on model prefix."""
        if model.startswith("MK-"):
            return process_mk_model(db, model, finish, index, reference, failed_rows, results)
        elif model.startswith("FIL-"):
            return process_fil_model(db, model, finish, index, reference, failed_rows, results)
        else:
            return process_generic_model(db, model, index, reference, failed_rows, results)


    # ── Main Loop ─────────────────────────────────────────────────────────────────

    customer_written = False
    for index, row in df.iterrows():
        model     = normalize_text(row["Model"])
        finish    = normalize_text(row["Shutter_Finish"])
        reference = row["Reference"]

        # ── Validation ────────────────────────────────────────────────────────────
        if not model:
            failed_rows.append({
                "Row": index + 1,
                "Model": None,
                # "Reference": reference,
                "Cabinet Position": reference,
                "Reason": "Model missing"
            })
            continue

        # Finish is only required for MK- and FIL- models (generic models don't use it)
        if model.startswith(("MK-", "FIL-")) and not finish:
            failed_rows.append({
                "Row": index + 1,
                "Model": model,
                "Cabinet Position": reference,
                "Reason": "Finish missing"
            })
            continue

        # ── Write customer header once ─────────────────────────────────────────────
        if not customer_written:
            results.append({
                "Customer":            customer or "Default Customer",
                "GST Treatment":       "Consumer",
                "POC":                 poc or "Default POC",
                "Cabinet Position":           reference,
                "Order Lines/Product": model,
            })
            customer_written = True
        else:
            # model row written inside each processor below
            pass

        # ── Delegate to the right processor ───────────────────────────────────────
        process_row(db, model, finish, index, reference, failed_rows, results)

    pass
   

    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:

        if results:
            pd.DataFrame(results).to_excel(
                writer,
                sheet_name="Success",
                index=False
            )

        if failed_rows:
            pd.DataFrame(failed_rows).to_excel(
                writer,
                sheet_name="Failed",
                index=False
            )

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="processed_output.xlsx"'}
    )
