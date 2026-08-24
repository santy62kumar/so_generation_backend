"""Odoo customer, point-of-contact, and warranty lookup service."""

import logging
import os
import time
import xmlrpc.client
from threading import Lock

from dotenv import load_dotenv

# Odoo connection details
load_dotenv()

logger = logging.getLogger(__name__)

url      = os.getenv('ODOO_URL')
db       = os.getenv('ODOO_DB')
username = os.getenv('ODOO_USERNAME')
password = os.getenv('ODOO_PASSWORD')

_uid = None
_models = None
_connection_lock = Lock()

# Two blocking XML-RPC calls per lookup, up to ODOO_TIMEOUT_SECONDS each, and
# the same crm_id gets re-uploaded repeatedly while a sheet is being corrected.
# A short TTL keeps repeats free without pinning a record that changed in Odoo.
try:
    CACHE_TTL_SECONDS = float(os.getenv("ODOO_CACHE_TTL_SECONDS", "300"))
except ValueError as exc:
    raise RuntimeError("ODOO_CACHE_TTL_SECONDS must be a number.") from exc

_CACHE_MAX_ENTRIES = 256
_poc_cache: dict[str, tuple[float, tuple]] = {}
_poc_cache_lock = Lock()


def _cache_get(key: str):
    if CACHE_TTL_SECONDS <= 0:
        return None
    with _poc_cache_lock:
        entry = _poc_cache.get(key)
        if entry is None:
            return None
        stored_at, value = entry
        if time.monotonic() - stored_at >= CACHE_TTL_SECONDS:
            del _poc_cache[key]
            return None
        return value


def _cache_put(key: str, value: tuple) -> None:
    if CACHE_TTL_SECONDS <= 0:
        return
    now = time.monotonic()
    with _poc_cache_lock:
        for stale in [k for k, (at, _) in _poc_cache.items() if now - at >= CACHE_TTL_SECONDS]:
            del _poc_cache[stale]
        if len(_poc_cache) >= _CACHE_MAX_ENTRIES:
            _poc_cache.pop(next(iter(_poc_cache)), None)
        _poc_cache[key] = (now, value)


def clear_customer_poc_cache() -> None:
    with _poc_cache_lock:
        _poc_cache.clear()


class _TimeoutTransport(xmlrpc.client.Transport):
    def __init__(self, timeout: float):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        connection = super().make_connection(host)
        connection.timeout = self.timeout
        return connection


class _TimeoutSafeTransport(xmlrpc.client.SafeTransport):
    def __init__(self, timeout: float):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        connection = super().make_connection(host)
        connection.timeout = self.timeout
        return connection


def _get_connection():
    global _uid, _models
    if _uid and _models:
        return _uid, _models

    with _connection_lock:
        if _uid and _models:
            return _uid, _models
        if not all((url, db, username, password)):
            raise RuntimeError("ODOO_URL, ODOO_DB, ODOO_USERNAME, and ODOO_PASSWORD are required.")
        try:
            timeout = float(os.getenv("ODOO_TIMEOUT_SECONDS", "10"))
        except ValueError as exc:
            raise RuntimeError("ODOO_TIMEOUT_SECONDS must be a number.") from exc
        if timeout <= 0:
            raise RuntimeError("ODOO_TIMEOUT_SECONDS must be greater than zero.")

        transport = _TimeoutSafeTransport(timeout) if url.lower().startswith("https://") else _TimeoutTransport(timeout)
        common = xmlrpc.client.ServerProxy(f"{url.rstrip('/')}/xmlrpc/2/common", transport=transport)
        uid = common.authenticate(db, username, password, {})
        if not uid:
            raise RuntimeError("Odoo authentication failed.")

        _uid = uid
        _models = xmlrpc.client.ServerProxy(
            f"{url.rstrip('/')}/xmlrpc/2/object",
            transport=transport,
        )
        return _uid, _models

def get_customer_poc(crm_id):
    """
    Fetches the customer and POC (Point of Contact) from Odoo using the crm_id.
    """
    # customer_data = models.execute_kw(db, uid, password,
    #                                   'crm.lead', 'search_read',
    #                                   [
    #       [['id', '=', crm_id]]
    #     ], {'fields': ['id', 
    #         'name', 
    #         # 'phone',
    #         'partner_id',
    #         'x_studio_sales_poc_1',           
    #         # 'x_studio_sales_poc_mob_no_1',    
    #         # 'x_studio_installation_poc_no_1', 
    #         # 'x_studio_supervisor_1' 
    #         ]})
    
    # if not customer_data:
    #     return None, None, None


    # if str(crm_id).isdigit():
    #     search_domain = [['id', '=', int(crm_id)]]
    # else:
    #     search_domain = [['x_studio_custom_id', '=', str(crm_id)]]

    # print(f"crm_id received: '{crm_id}' | isdigit: {str(crm_id).isdigit()}")  # 👈 add this

    cache_key = str(crm_id)
    cached = _cache_get(cache_key)
    if cached is not None:
        logger.debug("Odoo lookup for %s served from cache", cache_key)
        return cached

    uid, models = _get_connection()

    if str(crm_id).isdigit():
        search_domain = [['id', '=', int(crm_id)]]
    else:
        search_domain = [['x_studio_custom_id', '=', str(crm_id)]]

    # print(f"search_domain: {search_domain}") 

    customer_data = models.execute_kw(db, uid, password,
                                    'crm.lead', 'search_read',
                                    [search_domain],   # ✅ single wrap
                                    {'fields': ['id',
                                        'name',
                                        'partner_id',
                                        'x_studio_sales_poc_1',
                                    ], 'limit': 1})

    # Fallback to custom_id if id search returned nothing
    if not customer_data and str(crm_id).isdigit():
        customer_data = models.execute_kw(db, uid, password,
                                        'crm.lead', 'search_read',
                                        [[['x_studio_custom_id', '=', str(crm_id)]]],
                                        {'fields': ['id',
                                            'name',
                                            'partner_id',
                                            'x_studio_sales_poc_1',
                                        ], 'limit': 1})

    if not customer_data:
        _cache_put(cache_key, (None, None, None))
        return None, None, None

    # Access the first item from the list returned by search_read
    lead = customer_data[0]  # This is the first lead in the list
    # print(f"Fetched Lead Data: {lead}")  # Debugging statement to check the structure of the lead data

    # Safely access the fields
    project_name= lead.get('name', 'Default Project Name')
   
    # project_name = lead.get('name', [None, 'Default Project Name'])[1]
    # customer = lead.get('partner_id', 'Default Customer')[1]  # Default if 'name' is not found
    partner = lead.get('partner_id', False)
    customer = partner[1] if partner else 'Default Customer'
    poc = lead.get('x_studio_sales_poc_1', 'Default POC')  # Default if POC is not found
    logger.info(
        "Fetched from Odoo — Project Name: %s, Customer: %s, POC: %s", project_name, customer, poc
    )

    result = (project_name, customer, poc)
    _cache_put(cache_key, result)
    return result


def get_warranty_details_by_so(so_number: str):
    """Return the warranty-handbook fields for an exact Odoo sales order."""
    uid, models = _get_connection()
    orders = models.execute_kw(
        db,
        uid,
        password,
        "sale.order",
        "search_read",
        [[["name", "=ilike", so_number.strip()]]],
        {
            "fields": [
                "name",
                "partner_id",
                "x_studio_project_name",
                "invoice_ids",
            ],
            "limit": 1,
        },
    )
    if not orders:
        return None

    order = orders[0]
    project = order.get("x_studio_project_name")
    leads = models.execute_kw(
        db,
        uid,
        password,
        "crm.lead",
        "read",
        [[project[0]]],
        {
            "fields": [
                "contact_name",
                "x_studio_name_of_person",
                "partner_name",
                "phone",
                "mobile",
                "street",
                "street2",
                "city",
                "state_id",
                "country_id",
                "zip",
                "x_studio_pincode",
            ]
        },
    ) if project else []
    customer = leads[0] if leads else {}
    state = customer.get("state_id")
    country = customer.get("country_id")
    address = ", ".join(
        str(value)
        for value in (
            customer.get("street"),
            customer.get("street2"),
            customer.get("city"),
            state[1] if state else None,
            country[1] if country else None,
        )
        if value
    )

    invoice_number = ""
    invoice_ids = order.get("invoice_ids") or []
    if invoice_ids:
        invoices = models.execute_kw(
            db,
            uid,
            password,
            "account.move",
            "read",
            [invoice_ids],
            {"fields": ["name", "move_type", "state", "invoice_date"]},
        )
        invoices = [
            invoice
            for invoice in invoices
            if invoice.get("move_type") in {"out_invoice", "out_refund"}
            and invoice.get("state") != "cancel"
            and invoice.get("name") not in {False, "/"}
        ]
        if invoices:
            invoices.sort(
                key=lambda invoice: (
                    invoice.get("state") == "posted",
                    invoice.get("invoice_date") or "",
                    invoice["id"],
                ),
                reverse=True,
            )
            invoice_number = invoices[0]["name"]

    distributor = order.get("partner_id")
    return {
        "soNumber": order["name"],
        "customerName": (
            customer.get("contact_name")
            or customer.get("x_studio_name_of_person")
            or customer.get("partner_name")
            or ""
        ),
        "contactNumber": (
            customer.get("mobile")
            or customer.get("phone")
            or ""
        ),
        "invoiceNumber": invoice_number,
        "distributorName": distributor[1] if distributor else "",
        "address": address,
        "pinCode": customer.get("zip") or customer.get("x_studio_pincode") or "",
    }
