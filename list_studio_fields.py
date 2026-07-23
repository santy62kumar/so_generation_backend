# """
# One-off diagnostic script — run this once to see the real field names
# available on product.template, so cabinet_odoo.py can be corrected.

# Usage:
#     python list_studio_fields.py
# """

# from odoo_connection import get_connection, db, password


# def main():
#     uid, models = get_connection()

#     fields = models.execute_kw(
#         db, uid, password,
#         'product.template', 'fields_get',
#         [],
#         {'attributes': ['string', 'type']}
#     )

#     print("\n============================================================")
#     print("x_studio_* FIELDS ON product.template")
#     print("============================================================\n")

#     for name, meta in sorted(fields.items()):
#         if name.startswith('x_studio'):
#             print(f"{name:45s} ({meta['type']:10s})  label: {meta['string']}")


# if __name__ == "__main__":
#     main()