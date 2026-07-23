# """
# Fetches cabinet details from Odoo instead of the local database.

# `model` is treated as the Odoo `default_code` (internal reference) —
# same convention used in sogeneration.py's get_product_details().

# The old local-DB Cabinet model had 6 separate bom_line_1..6 columns.
# There is no equivalent flat field in Odoo. Instead, that data is
# derived from x_studio_panel_code, which looks like:

#     [P1201-AA] C3-XTP-UC-BF-BOTTOM-B-G-332X898, [P1174-AA] C3-XTP-UC-BF-TOP-B-G-332X898

# We extract the bracketed codes (P1201-AA, P1174-AA, ...) in order and
# map them to bom_line_1, bom_line_2, ... bom_line_6 (up to 6; missing
# ones are None), exactly the way clean_panel_code() in sogeneration.py
# already parses this field, just kept as a list instead of a joined
# string.
# """

# import re
# from types import SimpleNamespace
# from odoo_connection import get_connection, db, password

# CABINET_FIELDS = [
#     'id',
#     'default_code',
#     'name',
#     'x_studio_product_description',
#     'x_studio_panel_code',
# ]

# MAX_BOM_LINES = 6

# # Simple in-memory cache so repeated rows with the same model
# # don't trigger a fresh XML-RPC call each time.
# _cabinet_cache = {}


# def _extract_panel_codes(raw_panel_code):
#     """
#     Given a raw panel_code string like:
#         "[P1201-AA] C3-XTP-UC-BF-BOTTOM-B-G-332X898, [P1174-AA] C3-XTP-UC-BF-TOP-B-G-332X898"
#     returns an ordered, de-duplicated list of the bracketed codes:
#         ["P1201-AA", "P1174-AA"]
#     """
#     if not raw_panel_code:
#         return []

#     codes = re.findall(r'\[([^\]]+)\]', str(raw_panel_code))

#     unique_codes = []
#     for code in codes:
#         code = code.strip()
#         if code and code not in unique_codes:
#             unique_codes.append(code)

#     return unique_codes


# def get_cabinet(model):
#     """
#     Fetches a cabinet (product.template) from Odoo by internal reference
#     (default_code).

#     Returns a SimpleNamespace with the same attributes the old Cabinet
#     DB model had:
#         .cabinet_code, .description, .bom_line_1 ... .bom_line_6

#     bom_line_1..6 are derived from parsing x_studio_panel_code, in order.
#     If fewer than 6 codes are present, the remaining bom_line_N are None.

#     Returns None if no matching product is found.
#     """
#     model = str(model).strip()

#     if model in _cabinet_cache:
#         return _cabinet_cache[model]

#     uid, models = get_connection()

#     records = models.execute_kw(
#         db, uid, password,
#         'product.template', 'search_read',
#         [[('default_code', '=', model)]],
#         {'fields': CABINET_FIELDS, 'limit': 1}
#     )

#     if not records:
#         _cabinet_cache[model] = None
#         return None

#     rec = records[0]

#     panel_codes = _extract_panel_codes(rec.get('x_studio_panel_code'))
#     # pad out to MAX_BOM_LINES with None so bom_line_1..6 always exist
#     padded_codes = panel_codes[:MAX_BOM_LINES] + [None] * (MAX_BOM_LINES - len(panel_codes))

#     bom_line_kwargs = {
#         f"bom_line_{i + 1}": padded_codes[i] for i in range(MAX_BOM_LINES)
#     }

#     cabinet = SimpleNamespace(
#         cabinet_code=rec.get('default_code') or model,
#         description=rec.get('x_studio_product_description') or rec.get('name') or model,
#         **bom_line_kwargs,
#     )

#     _cabinet_cache[model] = cabinet
#     return cabinet