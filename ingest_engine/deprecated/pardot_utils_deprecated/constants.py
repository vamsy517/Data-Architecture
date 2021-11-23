import os

PARDOT_LOGIN_URL = 'https://pi.pardot.com/api/login/version/4'
B2C_EMAIL = os.environ['B2C_EMAIL']
B2C_PASSWORD = os.environ['B2C_PASSWORD']
B2C_USER_KEY = os.environ['B2C_USER_KEY']
B2B_EMAIL = os.environ['B2B_EMAIL']
B2B_PASSWORD = os.environ['B2B_PASSWORD']
B2B_USER_KEY = os.environ['B2B_USER_KEY']
B2C_DATASET = 'PardotB2C'
B2B_DATASET = 'Pardot'

# Logs
B2C_LIST_MEMBERSHIP_LOG = 'update_pardot_list_memberships_results_b2c.csv'
B2B_LIST_MEMBERSHIP_LOG = 'update_pardot_list_memberships_results.csv'
B2C_LISTS_LOG = 'update_lists_results_B2C.csv'
B2B_LISTS_LOG = 'update_lists_results.csv'
B2C_EMAIL_LOG = 'update_pardot_emails_results_b2c.csv'
B2B_EMAIL_LOG = 'update_pardot_emails_results.csv'
B2C_PROSPECTS_LOG = 'update_prospects_results_b2c.csv'
B2B_PROSPECTS_LOG = 'update_prospects_results.csv'

LIST_MEMBERSHIP_API = 'listMembership'
LISTS_API = 'list'
EMAIL_API = 'emailClick'
PROSPECTS_API = 'prospect'
LIST_MEMBERSHIP_TABLE = 'List_Memberships'
LISTS_TABLE = 'Lists'
EMAIL_CLICKS_TABLE = 'EmailClicks'
EMAILS_TABLE = 'Emails'
PROSPECTS_TABLE = 'Prospects'
LIST_MEMBERSHIP_DATA_NAME = 'list_membership'
LISTS_DATA_NAME = 'list'
EMAILS_DATA_NAME = 'emailClick'
PROSPECTS_DATA_NAME = 'prospect'

GET_BATCH_URL = 'https://pi.pardot.com/api/{pardot_api}/version/4/do/query'
GET_BATCH_EMAIL_URL = 'https://pi.pardot.com/api/email/version/4/do/stats/id/{list_email_id}?'
GET_EMAIL = 'https://pi.pardot.com/api/{pardot_api}/version/4/do/read/email/{email}'
CREATE_EMAIL = 'https://pi.pardot.com/api/{pardot_api}/version/4/do/create/email/{email}'
UPDATE_PROSPECT = 'https://pi.pardot.com/api/{pardot_api}/version/4/do/update/id/{prospect_id}'
UPDATE_LIST = 'https://pi.pardot.com/api/{pardot_api}/version/4/do/create/list_id/{list_id}/prospect_id/{prospect_id}'
