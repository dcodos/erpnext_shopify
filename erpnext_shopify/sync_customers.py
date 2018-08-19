from __future__ import unicode_literals
import frappe
from frappe import _
import requests.exceptions
from .shopify_requests import get_shopify_customers, post_request, put_request
from .utils import make_shopify_log

def sync_customers():
	shopify_customer_list = []
	sync_shopify_customers(shopify_customer_list)
	frappe.local.form_dict.count_dict["customers"] = len(shopify_customer_list)

def sync_shopify_customers(shopify_customer_list):
	for shopify_customer in get_shopify_customers():
		if not frappe.db.get_value("Customer", {"shopify_customer_id": shopify_customer.get('id')}, "name"):
			create_customer(shopify_customer, shopify_customer_list)

def create_customer(shopify_customer, shopify_customer_list):
	import frappe.utils.nestedset
	shopify_settings = frappe.get_doc("Shopify Settings", "Shopify Settings")

	cust_name = (shopify_customer.get("first_name") + " " + (shopify_customer.get("last_name") \
		and  shopify_customer.get("last_name") or "")) if shopify_customer.get("first_name")\
		else shopify_customer.get("email")

	try:
		customer = frappe.get_doc({
			"doctype": "Customer",
			"name": shopify_customer.get("id"),
			"customer_name" : cust_name,
			"shopify_customer_id": shopify_customer.get("id"),
			"sync_with_shopify": 1,
			"customer_group": shopify_settings.customer_group,
			"territory": frappe.utils.nestedset.get_root_of("Territory"),
			"customer_type": _("Individual")
		})
		customer.flags.ignore_mandatory = True
		customer.insert()

		if customer:
			create_customer_address(customer, shopify_customer)

		shopify_customer_list.append(shopify_customer.get("id"))
		frappe.db.commit()

	except Exception as e:
		if e.args[0] and e.args[0].startswith("402"):
			raise e
		else:
			make_shopify_log(title=e.message, status="Error", method="create_customer", message=frappe.get_traceback(),
				request_data=shopify_customer, exception=True)

def create_customer_address(customer, shopify_customer):
	if not shopify_customer.get("addresses"):
		return

	for i, address in enumerate(shopify_customer.get("addresses")):
		address_title, address_type = get_address_title_and_type(customer.customer_name, i)
		try :
			frappe.get_doc({
				"doctype": "Address",
				"shopify_address_id": address.get("id"),
				"address_title": address_title,
				"address_type": address_type,
				"address_line1": address.get("address1") or "Address 1",
				"address_line2": address.get("address2"),
				"city": address.get("city") or "City",
				"state": address.get("province"),
				"pincode": address.get("zip"),
				"country": address.get("country"),
				"phone": address.get("phone"),
				"email_id": shopify_customer.get("email"),
				"links": [{
					"link_doctype": "Customer",
					"link_name": customer.name
				}]
			}).insert(ignore_mandatory=True)

		except Exception as e:
			make_shopify_log(title=e.message, status="Error", method="create_customer_address", message=frappe.get_traceback(),
				request_data=shopify_customer, exception=True)

def get_address_title_and_type(customer_name, index):
	address_type = _("Billing")
	address_title = customer_name
	if frappe.db.get_value("Address", "{0}-{1}".format(customer_name.strip(), address_type)):
		address_title = "{0}-{1}".format(customer_name.strip(), index)

	return address_title, address_type

def sync_customer_address(customer, address):
	address_name = address.pop("name")

	shopify_address = post_request("/admin/customers/{0}/addresses.json".format(customer.shopify_customer_id),
	{"address": address})

	address = frappe.get_doc("Address", address_name)
	address.shopify_address_id = shopify_address['customer_address'].get("id")
	address.save()

def update_address_details(customer, last_sync_datetime):
	customer_addresses = get_customer_addresses(customer, last_sync_datetime)
	for address in customer_addresses:
		if address.shopify_address_id:
			url = "/admin/customers/{0}/addresses/{1}.json".format(customer.shopify_customer_id,\
			address.shopify_address_id)

			address["id"] = address["shopify_address_id"]

			del address["shopify_address_id"]

			put_request(url, { "address": address})

		else:
			sync_customer_address(customer, address)

def get_customer_addresses(customer, last_sync_datetime=None):
	conditions = ["dl.parent = addr.name", "dl.link_doctype = 'Customer'",
		"dl.link_name = '{0}'".format(frappe.db.escape(customer['name']))]

	if last_sync_datetime:
		last_sync_condition = "addr.modified >= '{0}'".format(last_sync_datetime)
		conditions.append(last_sync_condition)

	address_query = """select addr.name, addr.address_line1 as address1, addr.address_line2 as address2,
		addr.city as city, addr.state as province, addr.country as country, addr.pincode as zip,
		addr.shopify_address_id from tabAddress addr, `tabDynamic Link` dl
		where {0}""".format(' and '.join(conditions))

	return frappe.db.sql(address_query, as_dict=1)
