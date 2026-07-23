# import xmlrpc.client
# from dotenv import load_dotenv
# import os

# # ------------------- CONFIG -------------------
# load_dotenv()

# url      = os.getenv('ODOO_URL')
# db       = os.getenv('ODOO_DB')
# username = os.getenv('ODOO_USERNAME')
# password = os.getenv('ODOO_PASSWORD')

# # Cached connection so we don't re-authenticate on every call
# _uid = None
# _models = None


# def get_connection():
#     """
#     Returns (uid, models) - authenticates once and reuses the connection
#     for the lifetime of the process.
#     """
#     global _uid, _models

#     if _uid and _models:
#         return _uid, _models

#     common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
#     uid = common.authenticate(db, username, password, {})

#     if not uid:
#         raise Exception("❌ Odoo Authentication failed. Check credentials.")

#     print("✅ Odoo Authentication Success. UID:", uid)

#     models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

#     _uid, _models = uid, models
#     return uid, models