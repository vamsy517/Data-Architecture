from google.cloud import bigquery  # type: ignore

EXTRA_USER_INFO = ["recency", "frequency", "country", "continent", "province", "city"]
EXTRA_USER_INFO_TYPES = {
    "recency": bigquery.enums.SqlTypeNames.FLOAT,
    "frequency": bigquery.enums.SqlTypeNames.FLOAT,
    "country": bigquery.enums.SqlTypeNames.STRING,
    "continent": bigquery.enums.SqlTypeNames.STRING,
    "province": bigquery.enums.SqlTypeNames.STRING,
    "city": bigquery.enums.SqlTypeNames.STRING,
}
# DROP_NA_USER_INFO_SUBSET = ["recency"]

CV_SPLITS = 10
CV_REPS = 3

UPLOAD_LIMIT = 5000000

MIN_INDUSTRY_N_THRESHOLD = 10

# Grid search
N_ITERS = 100
CV = 3
N_JOBS = -1


# Job titles
JOB_TITLES_DATASET = "Segmentation_jobs_experiment"
JOB_TITLES_TABLE = "users_jobs"

MODEL_TARGET = {"industry": "industrygroup", "job_title": "job_title_11"}

MODEL_FEATURES = {
    "industry": [
        "number_of_sessions",
        "number_of_views",
        "visits_from_social",
        "visits_from_search",
        "visits_from_email",
        "visits_from_NSMG_network",
        "visits_from_own_website",
        "visit_from_bookmark_or_url",
        "visits_from_web",
        "visits_from_mobile",
        "lead_or_not",
        "avg_timestamp_for_continent",
        "visits_from_northamerica",
        "visits_from_southamerica",
        "visits_from_europe",
        "visits_from_africa",
        "visits_from_asia",
        "visits_from_othercontinent",
        "visits_from_linux",
        "visits_from_android",
        "visits_from_windows10",
        "visits_from_windows_older",
        "visits_from_iphone",
        "visits_from_mac",
        "visits_from_apple_other",
        "average_completion",
        "average_dwell",
        "average_virality",
    ],
    "job_title": [
        "number_of_sessions",
        "number_of_views",
        "visits_from_social",
        "visits_from_search",
        "visits_from_email",
        "visits_from_NSMG_network",
        "visits_from_own_website",
        "visit_from_bookmark_or_url",
        "visits_from_web",
        "visits_from_mobile",
        "lead_or_not",
        "avg_timestamp_for_continent",
        "visits_from_northamerica",
        "visits_from_southamerica",
        "visits_from_europe",
        "visits_from_africa",
        "visits_from_asia",
        "visits_from_othercontinent",
        "visits_from_linux",
        "visits_from_android",
        "visits_from_windows10",
        "visits_from_windows_older",
        "visits_from_iphone",
        "visits_from_mac",
        "visits_from_apple_other",
        "average_completion",
        "average_dwell",
        "average_virality",
    ],
}

PROBABILISTIC_PREDICTION = {"industry": True, "job_title": False}
