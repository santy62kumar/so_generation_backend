import xmlrpc.client
from dotenv import load_dotenv
import os
# Odoo connection details
load_dotenv()

url      = os.getenv('ODOO_URL')
db       = os.getenv('ODOO_DB')
username = os.getenv('ODOO_USERNAME')
password = os.getenv('ODOO_PASSWORD')

# Authenticate with Odoo
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})

if not uid:
    raise Exception("Authentication failed")

print("✅ Authentication Success:", uid)

# Create object proxy to interact with Odoo models
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

def get_customer_poc(crm_id):
    """
    Fetches the customer and POC (Point of Contact) from Odoo using the crm_id.
    """
    customer_data = models.execute_kw(db, uid, password,
                                      'crm.lead', 'search_read',
                                      [
          [['id', '=', crm_id]]
        ], {'fields': ['id', 
            'name', 
            # 'phone',
            'partner_id',
            
            'x_studio_sales_poc_1',           
            # 'x_studio_sales_poc_mob_no_1',    
            # 'x_studio_installation_poc_no_1', 
            # 'x_studio_supervisor_1' 
            ]})
    
    if not customer_data:
        return None, None

    # Access the first item from the list returned by search_read
    lead = customer_data[0]  # This is the first lead in the list

    # Safely access the fields
    project_name= lead.get('name', 'Default Project Name')
   
    # project_name = lead.get('name', [None, 'Default Project Name'])[1]
    customer = lead.get('partner_id', 'Default Customer')[1]  # Default if 'name' is not found
    poc = lead.get('x_studio_sales_poc_1', 'Default POC')  # Default if POC is not found
    
    return project_name, customer, poc
