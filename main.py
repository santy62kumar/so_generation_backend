from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import re
import io
from models import Cabinet, ColorCode
from sqlalchemy.orm import Session
from database import get_db
from models import Cabinet, ColorCode
from odoo import get_customer_poc

app = FastAPI()

# ✅ Adjust origins to your frontend URL
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# crm_id = 16534  # Example crm_id

# # Call the function and print the results
# customer, poc = get_customer_poc(crm_id)


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
        df = pd.read_excel(excel_bytes, header=2, engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Unable to read Excel file: {e}")

    df.columns = df.columns.astype(str).str.strip()

    required_cols = ["Reference", "Item", "Finishes"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing columns in sheet: {missing}"
        )
    

    try:
        # Read the first sheet and get the value from cell A2 (Project ID : <value>)
        project_id_text = pd.read_excel(excel_bytes, sheet_name=0, header=None).iloc[1, 0]
        # Extract the numeric Project ID using regular expression
        project_id_match = re.search(r"Project ID\s*:\s*(\d+)", project_id_text)
        if project_id_match:
            project_id = project_id_match.group(1)  # This will be the Project ID (e.g., "23444")
        else:
            raise HTTPException(status_code=400, detail="Project ID not found in the file")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error extracting Project ID: {e}")

    crm_id = project_id  # Use this Project ID as crm_id (adjust if needed)

    # Fetch customer and POC details using crm_id
    customer, poc = get_customer_poc(crm_id)

    df["Model"] = df["Item"].apply(extract_model)
    df["Shutter_Finish"] = df["Finishes"].apply(extract_shutter_finish)

    
    required_cols = ["Reference", "Item", "Finishes"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing columns in sheet: {missing}"
        )

    try:
        # Read the first sheet and get the value from cell A2 (Project ID : <value>)
        project_id_text = pd.read_excel(excel_bytes, sheet_name=0, header=None).iloc[1, 0]
        # Extract the numeric Project ID using regular expression
        project_id_match = re.search(r"Project ID\s*:\s*(\d+)", project_id_text)
        if project_id_match:
            project_id = project_id_match.group(1)  # This will be the Project ID (e.g., "23444")
        else:
            raise HTTPException(status_code=400, detail="Project ID not found in the file")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error extracting Project ID: {e}")

    crm_id = project_id  # Use this Project ID as crm_id (adjust if needed)

    # Fetch customer and POC details using crm_id
    customer, poc = get_customer_poc(crm_id)

    # Applying the extraction functions for "Model" and "Shutter_Finish"
    df["Model"] = df["Item"].apply(extract_model)
    df["Shutter_Finish"] = df["Finishes"].apply(extract_shutter_finish)

    # Fetch the "Reference" column as well
    df["Reference"] = df["Reference"].apply(normalize_text)  # Assuming you want to normalize the text for "Reference"

    results = []
    failed_rows = []


    # Add customer, GST Treatment, and POC details only once
   

    customer_written = False
    for index, row in df.iterrows():
        model = normalize_text(row["Model"])
        finish = normalize_text(row["Shutter_Finish"])
        reference = row["Reference"]  # Capture the Reference data

        if not model:
            failed_rows.append({
                "Row": index + 1,
                "Model": None,
                "Reference": reference,  # Include reference in failure log
                "Reason": "Model missing"
            })
            continue

        if not finish:
            failed_rows.append({
                "Row": index + 1,
                "Model": model,
                "Reference": reference,  # Include reference in failure log
                "Reason": "Finish missing"
            })
            continue

        cabinet = db.query(Cabinet).filter(
            Cabinet.cabinet_code == model
        ).first()

        if not cabinet:
            failed_rows.append({
                "Row": index + 1,
                "Model": model,
                "Reference": reference,  # Include reference in failure log
                "Reason": "Cabinet not found in DB"
            })
            continue

        colour = db.query(ColorCode).filter(
            ColorCode.colour_name == finish
        ).first()

        if not colour:
            failed_rows.append({
                "Row": index + 1,
                "Model": model,
                "Reference": reference,  # Include reference in failure log
                "Reason": f"Colour '{finish}' not found"
            })
            continue

        colour_code = colour.colour_code

        bom_lines = [
            cabinet.bom_line_1,
            cabinet.bom_line_2,
            cabinet.bom_line_3
        ]

        valid_bom = False
        results.append({
        "Customer": (customer or "Default Customer") if not customer_written else "",
        "GST Treatment": "Customer" if not customer_written else "",
        "POC": (poc or "Default POC") if not customer_written else "",
        "Reference": reference,
        "Order Lines/Product": model,
        })
        customer_written = True

        for bom in bom_lines:
            if bom:
                valid_bom = True
                final_code = f"{bom}-{colour_code}"
                results.append({
                    "Order Lines/Product": final_code,
                    "Reference": reference,
                })

        if not valid_bom:
            failed_rows.append({
                "Row": index + 1,
                "Model": model,
                "Reference": reference,  # Include reference in failure log
                "Reason": "No BOM lines found"
            })

    # results = []
    # failed_rows = []

    # for index, row in df.iterrows():

    #     model = normalize_text(row["Model"])
    #     finish = normalize_text(row["Shutter_Finish"])

    #     if not model:
    #         failed_rows.append({
    #             "Row": index + 1,
    #             "Model": None,
    #             "Reason": "Model missing"
    #         })
    #         continue

    #     if not finish:
    #         failed_rows.append({
    #             "Row": index + 1,
    #             "Model": model,
    #             "Reason": "Finish missing"
    #         })
    #         continue

    #     cabinet = db.query(Cabinet).filter(
    #         Cabinet.cabinet_code == model
    #     ).first()

    #     if not cabinet:
    #         failed_rows.append({
    #             "Row": index + 1,
    #             "Model": model,
    #             "Reason": "Cabinet not found in DB"
    #         })
    #         continue

    #     colour = db.query(ColorCode).filter(
    #         ColorCode.colour_name == finish
    #     ).first()

    #     if not colour:
    #         failed_rows.append({
    #             "Row": index + 1,
    #             "Model": model,
    #             "Reason": f"Colour '{finish}' not found"
    #         })
    #         continue

    #     colour_code = colour.colour_code

    #     bom_lines = [
    #         cabinet.bom_line_1,
    #         cabinet.bom_line_2,
    #         cabinet.bom_line_3
    #     ]

    #     valid_bom = False
    #     results.append({
    #         "Customer": customer or "Default Customer",
    #         "GST Treatment": "Customer",
    #         "POC": poc or "Default POC",
    #         "Order Lines/Product": model,
    #     })

    #     for bom in bom_lines:
    #         if bom:
    #             valid_bom = True
    #             final_code = f"{bom}-{colour_code}"
    #             results.append({
    #                 "Order Lines/Product": final_code
    #             })


    #     if not valid_bom:
    #         failed_rows.append({
    #             "Row": index + 1,
    #             "Model": model,
    #             "Reason": "No BOM lines found"
    #         })


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
