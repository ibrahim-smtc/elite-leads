"""
Google Sheets -> Perfox CRM connector.

Reads any registered Google Sheet, finds rows that are new or changed, and posts each one, as it is,
to the CRM's "Address for records". The CRM's records reviewer (a scheduled Perfox agent) turns each
record into a lead. This package never calls an AI model.
"""

__version__ = "1.0.0"
