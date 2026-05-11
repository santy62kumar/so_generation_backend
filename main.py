# # # # from fastapi import FastAPI, UploadFile, File, Depends
# # # # from fastapi.middleware.cors import CORSMiddleware
# # # # from sqlalchemy.orm import Session
# # # # from database import get_db
# # # # from sogeneration import handle_process_xlsx
# # # # import os
# # # # from dotenv import load_dotenv

# # # # load_dotenv()

# # # # app = FastAPI()

# # # # origins = os.getenv("ALLOWED_ORIGINS", "*")
# # # # origins_list = [origin.strip() for origin in origins.split(",") if origin]

# # # # app.add_middleware(
# # # #     CORSMiddleware,
# # # #     allow_origins=origins_list,
# # # #     allow_credentials=True,
# # # #     allow_methods=["*"],
# # # #     allow_headers=["*"],
# # # # )


# # # # @app.post("/process-xlsx")
# # # # async def process_xlsx(
# # # #     file: UploadFile = File(...),
# # # #     db: Session = Depends(get_db),
# # # # ):
# # # #     return await handle_process_xlsx(file, db)

# # # import sys

# # # if sys.platform == "win32":
# # #     import asyncio
# # #     asyncio.DefaultEventLoopPolicy = asyncio.WindowsProactorEventLoopPolicy


# # # import io
# # # import re
# # # from typing import List, Optional

# # # from fastapi import FastAPI, UploadFile, File, Form, Depends
# # # from fastapi.middleware.cors import CORSMiddleware
# # # from fastapi.responses import StreamingResponse
# # # from sqlalchemy.orm import Session

# # # from database import get_db
# # # from sogeneration import handle_process_xlsx
# # # from pdfgenerator import generate_pdf

# # # import os
# # # from dotenv import load_dotenv

# # # load_dotenv()

# # # app = FastAPI()

# # # origins = os.getenv("ALLOWED_ORIGINS", "*")
# # # origins_list = [origin.strip() for origin in origins.split(",") if origin.strip()]

# # # app.add_middleware(
# # #     CORSMiddleware,
# # #     allow_origins=origins_list,
# # #     allow_credentials=True,
# # #     allow_methods=["*"],
# # #     allow_headers=["*"],
# # # )


# # # # ── Route 1: SO Generation (existing) ─────────────────────────────────────────

# # # @app.post("/process-xlsx")
# # # async def process_xlsx(
# # #     file: UploadFile = File(...),
# # #     db: Session = Depends(get_db),
# # # ):
# # #     return await handle_process_xlsx(file, db)


# # # # ── Route 2: PDF Generation (converted from server.js) ───────────────────────

# # # @app.post("/api/generate-pdf")
# # # async def generate_pdf_route(
# # #     # ── Text fields (mirror req.body in server.js) ────────────────────────────
# # #     title: str          = Form(default="MR/MRS."),
# # #     customerName: str   = Form(default="CLIENT NAME"),
# # #     city: str           = Form(default="CITY"),
# # #     relationName: str   = Form(default=""),
# # #     relationPhone: str  = Form(default=""),
# # #     relationEmail: str  = Form(default=""),
# # #     designerName: str   = Form(default=""),
# # #     designerPhone: str  = Form(default=""),
# # #     designerEmail: str  = Form(default=""),

# # #     # ── File fields (mirror upload.fields in server.js) ───────────────────────
# # #     # layoutImage: up to 2 files
# # #     layoutImage: List[UploadFile] = File(default=[]),

# # #     # renderImage0-3: up to 1 file each
# # #     renderImage0: Optional[UploadFile] = File(default=None),
# # #     renderImage1: Optional[UploadFile] = File(default=None),
# # #     renderImage2: Optional[UploadFile] = File(default=None),
# # #     renderImage3: Optional[UploadFile] = File(default=None),
# # # ):
# # #     # Read all uploaded file bytes into memory (equivalent to multer memoryStorage)
# # #     layout_buffers: list[bytes] = [await f.read() for f in layoutImage] if layoutImage else []

# # #     async def read_optional(f: Optional[UploadFile]) -> Optional[bytes]:
# # #         return await f.read() if f else None

# # #     dynamic_data = {
# # #         # Slide 1 – Cover
# # #         "title":        title,
# # #         "customerName": customerName,
# # #         "city":         city,

# # #         # Slide 5 – Layout plan (list of buffers, up to 4 used)
# # #         "layoutImage":  layout_buffers,

# # #         # Slides 6-9 – Render images
# # #         "renderImage0": await read_optional(renderImage0),
# # #         "renderImage1": await read_optional(renderImage1),
# # #         "renderImage2": await read_optional(renderImage2),
# # #         "renderImage3": await read_optional(renderImage3),

# # #         # Slide 12 – Contact
# # #         "relationName":  relationName,
# # #         "relationPhone": relationPhone,
# # #         "relationEmail": relationEmail,
# # #         "designerName":  designerName,
# # #         "designerPhone": designerPhone,
# # #         "designerEmail": designerEmail,
# # #     }

# # #     pdf_buffer: bytes = await generate_pdf(dynamic_data)

# # #     # Sanitise filename (mirrors the safeName logic in server.js)
# # #     safe_name = re.sub(r"[^a-zA-Z0-9 ]", "", customerName).replace(" ", "_")

# # #     return StreamingResponse(
# # #         io.BytesIO(pdf_buffer),
# # #         media_type="application/pdf",
# # #         headers={
# # #             "Content-Disposition": f'inline; filename="Modula_Kitchen_{safe_name}.pdf"'
# # #         },
# # #     )

# # # if __name__ == "__main__":
# # #     import uvicorn
# # #     uvicorn.run("main:app", host="0.0.0.0", port=8000, loop="asyncio")


# # import sys
# # import io
# # import re
# # import asyncio
# # from typing import List, Optional

# # from fastapi import FastAPI, UploadFile, File, Form, Depends
# # from fastapi.middleware.cors import CORSMiddleware
# # from fastapi.responses import StreamingResponse
# # from sqlalchemy.orm import Session

# # from database import get_db
# # from sogeneration import handle_process_xlsx
# # from pdfgenerator import generate_pdf

# # import os
# # from dotenv import load_dotenv

# # load_dotenv()

# # app = FastAPI()

# # origins = os.getenv("ALLOWED_ORIGINS", "*")
# # origins_list = [origin.strip() for origin in origins.split(",") if origin.strip()]

# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=origins_list,
# #     allow_credentials=True,
# #     allow_methods=["*"],
# #     allow_headers=["*"],
# # )


# # # ── Route 1: SO Generation ─────────────────────────────────────────────────────

# # @app.post("/process-xlsx")
# # async def process_xlsx(
# #     file: UploadFile = File(...),
# #     db: Session = Depends(get_db),
# # ):
# #     return await handle_process_xlsx(file, db)


# # # ── Route 2: PDF Generation ────────────────────────────────────────────────────

# # @app.post("/api/generate-pdf")
# # async def generate_pdf_route(
# #     title: str         = Form(default="MR/MRS."),
# #     customerName: str  = Form(default="CLIENT NAME"),
# #     city: str          = Form(default="CITY"),
# #     relationName: str  = Form(default=""),
# #     relationPhone: str = Form(default=""),
# #     relationEmail: str = Form(default=""),
# #     designerName: str  = Form(default=""),
# #     designerPhone: str = Form(default=""),
# #     designerEmail: str = Form(default=""),
# #     layoutImage: List[UploadFile]    = File(default=[]),
# #     renderImage0: Optional[UploadFile] = File(default=None),
# #     renderImage1: Optional[UploadFile] = File(default=None),
# #     renderImage2: Optional[UploadFile] = File(default=None),
# #     renderImage3: Optional[UploadFile] = File(default=None),
# # ):
# #     layout_buffers = [await f.read() for f in layoutImage] if layoutImage else []

# #     async def read_optional(f):
# #         return await f.read() if f else None

# #     dynamic_data = {
# #         "title":        title,
# #         "customerName": customerName,
# #         "city":         city,
# #         "layoutImage":  layout_buffers,
# #         "renderImage0": await read_optional(renderImage0),
# #         "renderImage1": await read_optional(renderImage1),
# #         "renderImage2": await read_optional(renderImage2),
# #         "renderImage3": await read_optional(renderImage3),
# #         "relationName":  relationName,
# #         "relationPhone": relationPhone,
# #         "relationEmail": relationEmail,
# #         "designerName":  designerName,
# #         "designerPhone": designerPhone,
# #         "designerEmail": designerEmail,
# #     }

# #     pdf_buffer = await generate_pdf(dynamic_data)
# #     safe_name  = re.sub(r"[^a-zA-Z0-9 ]", "", customerName).replace(" ", "_")

# #     return StreamingResponse(
# #         io.BytesIO(pdf_buffer),
# #         media_type="application/pdf",
# #         headers={"Content-Disposition": f'attachment; filename="Modula_Kitchen_{safe_name}.pdf"'},
# #     )


# # # ── Entry point ────────────────────────────────────────────────────────────────

# # if __name__ == "__main__":
# #     import uvicorn

# #     config = uvicorn.Config(app, host="0.0.0.0", port=8000, reload=False)
# #     server = uvicorn.Server(config)

# #     if sys.platform == "win32":
# #         # Python 3.14+: set_event_loop_policy is deprecated.
# #         # Manually run uvicorn inside a ProactorEventLoop — the only loop
# #         # on Windows that supports subprocess creation (required by Playwright).
# #         loop = asyncio.ProactorEventLoop()
# #         loop.run_until_complete(server.serve())
# #     else:
# #         asyncio.run(server.serve())


# import sys
# import asyncio
# import warnings

# # ── Windows: force ProactorEventLoop so Playwright can spawn Chromium ─────────
# # Must be at module level — runs when uvicorn imports this file.
# # set_event_loop_policy is deprecated in 3.14 but still works; removed in 3.16.
# if sys.platform == "win32":
#     with warnings.catch_warnings():
#         warnings.simplefilter("ignore", DeprecationWarning)
#         asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# # ── Everything else below (unchanged) ─────────────────────────────────────────
# import io
# import re
# from typing import List, Optional

# from fastapi import FastAPI, UploadFile, File, Form, Depends
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import StreamingResponse
# from sqlalchemy.orm import Session

# from database import get_db
# from sogeneration import handle_process_xlsx
# from pdfgenerator import generate_pdf

# import os
# from dotenv import load_dotenv

# load_dotenv()

# app = FastAPI()

# origins = os.getenv("ALLOWED_ORIGINS", "*")
# origins_list = [origin.strip() for origin in origins.split(",") if origin.strip()]

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins_list,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# @app.post("/process-xlsx")
# async def process_xlsx(
#     file: UploadFile = File(...),
#     db: Session = Depends(get_db),
# ):
#     return await handle_process_xlsx(file, db)


# @app.post("/api/generate-pdf")
# async def generate_pdf_route(
#     title: str         = Form(default="MR/MRS."),
#     customerName: str  = Form(default="CLIENT NAME"),
#     city: str          = Form(default="CITY"),
#     relationName: str  = Form(default=""),
#     relationPhone: str = Form(default=""),
#     relationEmail: str = Form(default=""),
#     designerName: str  = Form(default=""),
#     designerPhone: str = Form(default=""),
#     designerEmail: str = Form(default=""),
#     layoutImage: List[UploadFile]      = File(default=[]),
#     renderImage0: Optional[UploadFile] = File(default=None),
#     renderImage1: Optional[UploadFile] = File(default=None),
#     renderImage2: Optional[UploadFile] = File(default=None),
#     renderImage3: Optional[UploadFile] = File(default=None),
# ):
#     layout_buffers = [await f.read() for f in layoutImage] if layoutImage else []

#     async def read_optional(f):
#         return await f.read() if f else None

#     dynamic_data = {
#         "title":        title,
#         "customerName": customerName,
#         "city":         city,
#         "layoutImage":  layout_buffers,
#         "renderImage0": await read_optional(renderImage0),
#         "renderImage1": await read_optional(renderImage1),
#         "renderImage2": await read_optional(renderImage2),
#         "renderImage3": await read_optional(renderImage3),
#         "relationName":  relationName,
#         "relationPhone": relationPhone,
#         "relationEmail": relationEmail,
#         "designerName":  designerName,
#         "designerPhone": designerPhone,
#         "designerEmail": designerEmail,
#     }

#     pdf_buffer = await generate_pdf(dynamic_data)
#     safe_name  = re.sub(r"[^a-zA-Z0-9 ]", "", customerName).replace(" ", "_")

#     return StreamingResponse(
#         io.BytesIO(pdf_buffer),
#         media_type="application/pdf",
#         headers={"Content-Disposition": f'attachment; filename="Modula_Kitchen_{safe_name}.pdf"'},
#     )


import io
import re
import asyncio
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, UploadFile, File, Form, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session


from database import get_db
from sogeneration import handle_process_xlsx
from pdfgenerator import generate_pdf_sync          # ← sync wrapper, not generate_pdf

import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

_thread_pool = ThreadPoolExecutor()                  # one shared pool for the app

origins = os.getenv("ALLOWED_ORIGINS", "*")
origins_list = [o.strip() for o in origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Route 1: SO Generation ─────────────────────────────────────────────────────

@app.post("/process-xlsx")
async def process_xlsx(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    return await handle_process_xlsx(file, db)


# ── Route 2: PDF Generation ────────────────────────────────────────────────────

@app.post("/generate-pdf")
async def generate_pdf_route(
    title: str         = Form(default="MR/MRS."),
    customerName: str  = Form(default="CLIENT NAME"),
    city: str          = Form(default="CITY"),
    relationName: str  = Form(default=""),
    relationPhone: str = Form(default=""),
    relationEmail: str = Form(default=""),
    designerName: str  = Form(default=""),
    designerPhone: str = Form(default=""),
    designerEmail: str = Form(default=""),
    layoutImage: List[UploadFile]      = File(default=[]),
    renderImage0: Optional[UploadFile] = File(default=None),
    renderImage1: Optional[UploadFile] = File(default=None),
    renderImage2: Optional[UploadFile] = File(default=None),
    renderImage3: Optional[UploadFile] = File(default=None),
):
    layout_buffers = [await f.read() for f in layoutImage] if layoutImage else []

    async def read_optional(f):
        return await f.read() if f else None

    dynamic_data = {
        "title":        title,
        "customerName": customerName,
        "city":         city,
        "layoutImage":  layout_buffers,
        "renderImage0": await read_optional(renderImage0),
        "renderImage1": await read_optional(renderImage1),
        "renderImage2": await read_optional(renderImage2),
        "renderImage3": await read_optional(renderImage3),
        "relationName":  relationName,
        "relationPhone": relationPhone,
        "relationEmail": relationEmail,
        "designerName":  designerName,
        "designerPhone": designerPhone,
        "designerEmail": designerEmail,
    }

    # Run Playwright in a background thread with its own ProactorEventLoop.
    # This lets uvicorn keep its SelectorEventLoop while Playwright gets
    # the ProactorEventLoop it needs to spawn Chromium on Windows.
    loop = asyncio.get_event_loop()
    pdf_buffer = await loop.run_in_executor(
        _thread_pool,
        generate_pdf_sync,   # sync function, runs in thread
        dynamic_data
    )

    safe_name = re.sub(r"[^a-zA-Z0-9 ]", "", customerName).replace(" ", "_")

    return StreamingResponse(
        io.BytesIO(pdf_buffer),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="Modula_Kitchen_{safe_name}.pdf"'},
    )