# Main DAGs folder
MAIN_FOLDER = 'ingest_engine'

# Connection to Postgre
POSTGRE_AUTH = 'postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres'

# GBQ Constants:
# set project id in GBQ
PULLDATA_PROJECT_ID = 'project-pulldata'
# set permutive project id in GBQ
PERMUTIVE_PROJECT_ID = 'permutive-1258'
# set permutive dataset
PERMUTIVE_DATASET = 'global_data'

BUCKET_SERVICE_ACCOUNT_JSON = '/home/ingest/credentials/pulldata-bucket.json'

CREDENTIALS_FOLDER = '/home/ingest/credentials/'
# Logging
LOGS_FOLDER = '/home/ingest/logs/'
UPDATE_PROCESSING_RESULTS = 'update_processing_results.csv'
API_TEST_LOG = 'api_test_log.csv'
CLEANUP_LOG = 'cleanup_log.csv'
ECONOMICS_UPDATE_LOG = 'update_log.csv'
UPDATE_COVID19_PROCESSING_RESULTS = 'update_covid19_processing_results.csv'
UPDATE_COMPANIES_PROCESSING_RESULTS = 'update_companies_processing_results.csv'
UPDATE_MNC_PROCESSING_RESULTS = 'update_mnc_processing_results.csv'
UPDATE_SURVEYS_PROCESSING_RESULTS = 'update_surveys_processing_results.csv'
UPDATE_SURVEYS_GD_PROCESSING_RESULTS = 'update_surveys_gd_processing_results.csv'
PROCESSING_RESULTS = 'processing_results.csv'
HEALTH_CHECK_LOG = 'health_check_log.csv'
PERMUTIVE_PARDOT_MATCH_LOG_B2B = 'update_permutive_pardot_match.csv'
PERMUTIVE_PARDOT_MATCH_LOG_B2C = 'update_permutive_pardot_b2c_match.csv'
# Logging messages
SUCC_UPDATE_LOG_MSG = 'Updated successfully.'
SUCC_REPLACE_LOG_MSG = 'Replaced successfully.'
NO_DATA_SKIP_UPLOAD = 'There is no data, skipping upload.'
NO_DATA_SKIP_REPLACE = 'There is no data, skipping replacement.'
ALREADY_EXISTS_SKIP_UPLOAD = 'Already exists. Skipping upload.'
FRANKLIN_API_PICKLE_REFRESH_URL="https://nsmg.nsmgendpoints.co.uk/franklinuser/reload_pickles"


