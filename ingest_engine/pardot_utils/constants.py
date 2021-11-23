from airflow.models import Variable
import os

PARDOT_LOGIN_URL = 'https://login.salesforce.com/services/oauth2/token'
B2C_EMAIL = os.environ['B2C_EMAIL']
B2C_PASSWORD = os.environ['B2C_PASSWORD']
B2C_USER_KEY = os.environ['B2C_USER_KEY']
B2B_EMAIL = os.environ['B2B_EMAIL']
B2B_PASSWORD = os.environ['B2B_PASSWORD']
B2B_USER_KEY = os.environ['B2B_USER_KEY']
B2C_DATASET = 'PardotB2C'
B2B_DATASET = 'Pardot'
PARDOT_CLIENT_ID = Variable.get('PARDOT_CLIENT_ID')
PARDOT_CLIENT_SECRET = Variable.get('PARDOT_CLIENT_SECRET')
PARDOT_USERNAME = Variable.get('PARDOT_USERNAME')
PARDOT_PASSWORD = Variable.get('PARDOT_PASSWORD')
PARDOT_BUSINESS_UNIT_ID = Variable.get('PARDOT_BUSINESS_UNIT_ID')
GDM_DATASET = 'pardot_GDM'
NSMG_DATASET = 'Pardot_NSMG'
VERDICT_DATASET = 'Pardot_Verdict'

# Logs
B2C_LIST_MEMBERSHIP_LOG = 'update_pardot_list_memberships_results_b2c.csv'
B2B_LIST_MEMBERSHIP_LOG = 'update_pardot_list_memberships_results.csv'
B2C_LISTS_LOG = 'update_lists_results_B2C.csv'
B2B_LISTS_LOG = 'update_lists_results.csv'
B2C_EMAIL_LOG = 'update_pardot_emails_results_b2c.csv'
B2B_EMAIL_LOG = 'update_pardot_emails_results.csv'
B2C_PROSPECTS_LOG = 'update_prospects_results_b2c.csv'
B2B_PROSPECTS_LOG = 'update_prospects_results.csv'
NSMG_PROSPECTS_LOG = 'update_prospects_results_nsmg.csv'
VERDICT_PROSPECTS_LOG = 'update_prospects_results_verdict.csv'
NSMG_EMAIL_LOG = 'update_emails_results_nsmg.csv'
VERDICT_EMAIL_LOG = 'update_emails_results_verdict.csv'
NSMG_LISTS_LOG = 'update_lists_results_nsmg.csv'
VERDICT_LISTS_LOG = 'update_lists_results_verdict.csv'
NSMG_LIST_MEMBERSHIP_LOG = 'update_list_membership_results_nsmg.csv'
VERDICT_LIST_MEMBERSHIP_LOG = 'update_lists_membership_results_verdict.csv'
NSMG_OPPORTUNITY_LOG = 'update_list_membership_results_nsmg.csv'
VERDICT_OPPORTUNITY_LOG = 'update_lists_membership_results_verdict.csv'
NSMG_VISITOR_ACTIVITY_LOG = 'update_visitor_activity_results_nsmg.csv'
VERDICT_VISITOR_ACTIVITY_LOG = 'update_visitor_activity_results_verdict.csv'
NSMG_VISITORS_LOG = 'update_visitors_results_nsmg.csv'
VERDICT_VISITORS_LOG = 'update_visitors_results_verdict.csv'
NSMG_VISITS_LOG = 'update_visits_results_nsmg.csv'
VERDICT_VISITS_LOG = 'update_visits_results_verdict.csv'
TAGS_LOG = 'update_visits_tags_verdict.csv'
TAG_OBJECTS_LOG = 'update_visits_tag_objects_verdict.csv'
USERS_LOG = 'update_visits_users_verdict.csv'
PROSPECT_ACCOUNT_LOG = 'update_visits_account_verdict.csv'

LIST_MEMBERSHIP_API = 'listMembership'
LISTS_API = 'list'
EMAIL_API = 'emailClick'
PROSPECTS_API = 'prospect'
OPPORTUNITY_API = 'opportunity'
VISITOR_ACTIVITY_API = 'visitorActivity'
VISITORS_API = 'visitor'
VISITS_API = 'visit'
TAG_API = 'tag'
TAG_OBJECTS_API = 'tagObject'
USERS_API = 'user'
PROSPECT_ACCOUNT_API = 'prospectAccount'

LIST_MEMBERSHIP_TABLE = 'List_Memberships'
LISTS_TABLE = 'Lists'
EMAIL_CLICKS_TABLE = 'EmailClicks'
EMAILS_TABLE = 'Emails'
PROSPECTS_TABLE = 'Prospects'
OPPORTUNITY_TABLE = 'Opportunity'
VISITOR_ACTIVITY_TABLE = 'visitor_activity'
VISITORS_TABLE = 'visitors'
VISITS_TABLE = 'Visits'
TAGS_TABLE = 'Tags'
TAGS_OBJECTS_TABLE = 'TagsObjects'
USERS_TABLE = 'Users'
PROSPECT_ACCOUNT_TABLE = 'ProspectAccounts'

LIST_MEMBERSHIP_DATA_NAME = 'list_membership'
LISTS_DATA_NAME = 'list'
EMAILS_DATA_NAME = 'emailClick'
PROSPECTS_DATA_NAME = 'prospect'
OPPORTUNITY_DATA_NAME = 'opportunity'
VISITOR_ACTIVITY_DATA_NAME = 'visitor_activity'
VISITORS_DATA_NAME = 'visitor'
VISITS_DATA_NAME = 'visits'
TAGS_DATA_NAME = 'tag'
TAG_OBJECTS_DATA_NAME = 'tagObject'
USERS_DATA_NAME = 'user'
PROSPECT_ACCOUNT_DATA_NAME = 'prospectAccount'

GET_BATCH_URL = 'https://pi.pardot.com/api/{pardot_api}/version/4/do/query'
GET_BATCH_EMAIL_URL = 'https://pi.pardot.com/api/email/version/4/do/stats/id/{list_email_id}?'
GET_EMAIL = 'https://pi.pardot.com/api/{pardot_api}/version/4/do/read/email/{email}'
CREATE_EMAIL = 'https://pi.pardot.com/api/{pardot_api}/version/4/do/create/email/{email}'
UPDATE_PROSPECT = 'https://pi.pardot.com/api/{pardot_api}/version/4/do/update/id/{prospect_id}'
UPDATE_LIST = 'https://pi.pardot.com/api/{pardot_api}/version/4/do/create/list_id/{list_id}/prospect_id/{prospect_id}'
