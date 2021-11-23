from ingest_utils.constants import PERMUTIVE_PROJECT_ID, PERMUTIVE_DATASET
from company_names_update.constants import CLEARBIT_EVENTS

# Query to get all distinct company names
GET_COMPANY_NAMES = f"""
        SELECT distinct properties.company.name FROM `{PERMUTIVE_PROJECT_ID}.{PERMUTIVE_DATASET}.{CLEARBIT_EVENTS}`
        where properties.company.name is not null
        """
