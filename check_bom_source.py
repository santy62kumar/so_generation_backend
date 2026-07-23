# """
# One-off diagnostic script — checks whether Odoo's Manufacturing app
# (mrp.bom / mrp.bom.line) has Bill of Materials data for a given
# cabinet's internal reference (default_code).

# Usage:
#     python check_bom_source.py MK-CABINET-CODE
# """

# import sys
# from odoo_connection import get_connection, db, password


# def main():
#     if len(sys.argv) < 2:
#         print("Usage: python check_bom_source.py <internal_reference>")
#         return

#     internal_ref = sys.argv[1].strip()
#     uid, models = get_connection()

#     # STEP 1: find the product.template for this internal reference
#     templates = models.execute_kw(
#         db, uid, password,
#         'product.template', 'search_read',
#         [[('default_code', '=', internal_ref)]],
#         {'fields': ['id', 'name', 'default_code'], 'limit': 1}
#     )

#     if not templates:
#         print(f"❌ No product.template found for default_code={internal_ref!r}")
#         return

#     template = templates[0]
#     template_id = template['id']
#     print(f"✅ Found product.template: id={template_id}, name={template['name']!r}\n")

#     # STEP 2: check if mrp.bom exists for this template
#     try:
#         boms = models.execute_kw(
#             db, uid, password,
#             'mrp.bom', 'search_read',
#             [[('product_tmpl_id', '=', template_id)]],
#             {'fields': ['id', 'code', 'product_qty', 'bom_line_ids']}
#         )
#     except Exception as e:
#         print(f"❌ mrp.bom lookup failed (Manufacturing app may not be installed): {e}")
#         return

#     if not boms:
#         print("⚠️  No mrp.bom record found for this product.")
#         print("    BOM data is likely NOT stored in Odoo's Manufacturing app for this product.")
#         return

#     print(f"✅ Found {len(boms)} BoM(s) for this product:\n")

#     for bom in boms:
#         print(f"  BoM id={bom['id']}  code={bom.get('code')}  qty={bom.get('product_qty')}")

#         line_ids = bom.get('bom_line_ids') or []
#         if not line_ids:
#             print("    (no BOM lines)")
#             continue

#         lines = models.execute_kw(
#             db, uid, password,
#             'mrp.bom.line', 'read',
#             [line_ids],
#             {'fields': ['id', 'product_id', 'product_qty']}
#         )

#         for line in lines:
#             product_field = line.get('product_id')
#             product_label = product_field[1] if product_field else 'Unknown'
#             print(f"    - component: {product_label}  qty={line.get('product_qty')}")

#         print()


# if __name__ == "__main__":
#     main()