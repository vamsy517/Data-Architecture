RANDOM_SEED = 0

LIST_OF_MUSTHAVE_FEATURES = [
    "number_of_sessions",
    "number_of_views",
    "visits_from_social",
    "visits_from_search",
    "visits_from_email",
    "visits_from_own_website",
    "lead_or_not",
    "avg_timestamp_for_continent",
    "average_completion",
    "average_dwell",
    "average_virality",
]

LIST_OF_MUSTHAVE_FEATURES_JOBS_11 = [
    "number_of_sessions",
    "number_of_views",
    "visits_from_social",
    "visits_from_search",
    "visits_from_email",
    "visits_from_own_website",
    "lead_or_not",
    "avg_timestamp_for_continent",
    "average_completion",
    "average_dwell",
    "average_virality",
    "job_title_11_business_owner__co_owner",
    "job_title_11_ceo_or_president",
    "job_title_11_group_or_senior_manager",
    "job_title_11_head_of_department_function",
    "job_title_11_manager",
    "job_title_11_managing_director_or_equivalent",
    "job_title_11_member_of_staff__other",
    "job_title_11_other_c_level_executive",
    "job_title_11_partner",
    "job_title_11_senior_executive_svp_or_corporate_vice_president_or_equivalent",
    "job_title_11_supervisor",
]

LIST_OF_MUSTHAVE_FEATURES_JOBS_SENIORITY = [
    "number_of_sessions",
    "number_of_views",
    "visits_from_social",
    "visits_from_search",
    "visits_from_email",
    "visits_from_own_website",
    "lead_or_not",
    "avg_timestamp_for_continent",
    "average_completion",
    "average_dwell",
    "average_virality",
    "job_title_seniority_c_level_1",
    "job_title_seniority_c_level_2",
    "job_title_seniority_d_level_1",
    "job_title_seniority_d_level_2",
    "job_title_seniority_m_level",
]

BINARY_C_LEVEL = [
    "number_of_sessions",
    "number_of_views",
    "visits_from_social",
    "visits_from_search",
    "visits_from_email",
    "visits_from_own_website",
    "lead_or_not",
    "avg_timestamp_for_continent",
    "average_completion",
    "average_dwell",
    "average_virality",
    "job_title_seniority_c_level_1",
]

LOWER_BOUND_WCSS = 5
UPPER_BOUND_WCSS = 20
STEP_WCSS = 1

KMEANS_INIT = "k-means++"
KMEANS_ITER = 300
KMEANS_NINIT = 10
KMEANS_K = 1

RF_MIN_SAMPLES_SPLIT = 10

NA_HOUR_DIFF = -6
SA_HOUR_DIFF = -3
EU_HOUR_DIFF = 1
AF_HOUR_DFF = 1
AS_HOUR_DIFF = 5

LINUX_AGENT_STR = "Linux"
ANDROID_AGENT_STR = "Android"
WIN10_AGENT_STR = "Windows NT 10.0"
WIN_AGENT_STR = "Windows"
IPHONE_AGENT_STR = "iPhone"
MAC_AGENT_STR = "Macintosh"
APPLE_AGENT_STR = "AppleWebKit"

SOCAL_REF_LIST = [
    "facebook",
    "linkedin",
    "twitter",
    "t.co/",
    "youtube",
    "instagram",
    "reddit",
    "pinterest",
]
SEARCH_REF_LIST = ["ask", "baidu", "bing", "duckduckgo", "google", "yahoo", "yandex"]
NSMG_REF_LIST = [
    "energymonitor.ai",
    "investmentmonitor.ai",
    "citymonitor.ai",
    "newstatesman.com",
    "cbronline.com",
    "techmonitor.ai",
    "elitetraveler.com",
]
EMAIL_REF_LIST = ["Pardot", "pardot"]

DATASET = "Segmentation_v2"
DATASET_TEST = "Segmentation_Test"

STORAGE_BUCKET = "nsmg-segmentation-2"

PULLDATA_PROJECT = "project-pulldata"
PERMUTIVE_PROJECT = "permutive-1258"

PAGEVIEWS_TABLE = 'global_data.pageview_events'
LEADS_DATASET_TABLE = "Audience.Leads"
USERS_CLUSTERS_TABLE = "users_clusters"
USERS_JOBS_TABLE = "users_jobs"
BEHAVIOURAL_PROFILES_TABLE = "behavioural_profiles"
INDUSTRY_PROFILES_TABLE = "industry_profiles"
JOBS_PROFILES_TABLE = "jobs_profiles"
SENIORITY_PROFILES_TABLE = "seniority_profiles"
WISHLIST_INDUSTRY_PROFILES_TABLE = "wishlist_industry_profiles"
WISHLIST_SUBINDUSTRY_PROFILES_TABLE = "wishlist_subIndustry_profiles"

NS_WISHLIST_DOMAINS_GROUPS = {
    "newstatesman.com": ["New Statesman"],
    "spearswms.com": ["Others"],
    "elitetraveler.com": ["Others"],
    "pressgazette.co.uk": ["Others"],
    "citymonitor.ai": ["Others"],
    "techmonitor.ai": ["Others"],
    "capitalmonitor.ai": ["Others"],
    "worldoffinewine.com": ["Others"],
}

WISHLIST_TABLES = {
    "global_data": [
        "wishlist_companies_gd_match",
        "category",
        "name_to_seek_in_clearbit",
        "industry",
    ],
    "ns_media": [
        "ns_wishlist",
        "Organisation",
        "name_to_seek_in_clearbit",
        "Industry_and_tier",
    ],
}

ARTICLE_NAMES_TO_EXCLUDE = """
"Press Gazette - Journalism News",
"New Statesman | Britain's Current Affairs & Politics Magazine",
"Computer Business Review | Breaking Tech News & IT Insights",
"Press Gazette – Journalism News",
"Britain's Current Affairs & Politics Magazine",
"Hybrid Archives - Computer Business Review",
"Emerging Technology Archives - Computer Business Review",
"Analytics Archives - Computer Business Review",
"News Archives - Press Gazette",
"Writers",
"Elite Traveler | The Private Jet Lifestyle Magazine",
"Software Archives - Computer Business Review",
"News – Press Gazette",
"Politics",
"Data Centre Archives - Computer Business Review",
"Page not found - Computer Business Review",
"News – Press Gazette",
"Page not found - Press Gazette",
"Global Current Affairs, Politics & Culture",
"Big Data Archives - Computer Business Review",
"Page not found – Press Gazette",
"Jobs4Journalists journalism jobs: The UK's best editorial recruitment site",
"National Newspapers Archives - Press Gazette",
"Cyber Security Archives - Computer Business Review",
"Jobs4Journalists – Press Gazette",
"Subscribe to the Elite Traveler Email Newsletter | Elite Traveler",
"Private Archives - Computer Business Review",
"National Newspapers – Press Gazette",
"Culture",
"Regional Newspapers – Press Gazette",
"subscribe",
"Page not found",
"Magazines Archives - Press Gazette",
"City Monitor - Home",
"Computer Business Review | Cloud News, Features & Analysis",
"",
"Elite Traveler TV Archives | Elite Traveler",
"ABC – Press Gazette",
"Page Three Archives - Press Gazette",
"New Statesman Long Read Articles",
"Search",
"Contact Us - Computer Business Review",
"Travel News Archives | Elite Traveler",
"Ed Targett, Author at Computer Business Review",
"Computer Business Review | Editorial Team at CBR",
"Internet of Things Archives - Computer Business Review",
"Magazines – Press Gazette",
"ABC Archives - Press Gazette",
"White Papers - Computer Business Review",
"About Us - Computer Business Review",
"Page not found | Elite Traveler",
"World",
"Obituaries Archives - Press Gazette",
"Jobs4Journalists - Press Gazette",
"Audience Data – Press Gazette",
"The Sunday Sport Archives - Press Gazette",
"Regional Newspapers Archives - Press Gazette",
"National Press Audience Data Archives - Press Gazette",
"Luxury Transport Archives | Elite Traveler",
"The Staggers",
"Books",
"Events",
"Science & Tech",
"My Account",
"The New Statesman podcasts",
"[current-page:page-title]",
"Subscribe12",
"Home - City Monitor",
"Page not found - City Monitor",
"About us - City Monitor",
"Transport Archives - City Monitor",
"Energy Monitor - Inside the global clean energy transition",
"Home - Energy Monitor",
"Page not found - Energy Monitor",
"About us - Energy Monitor",
"Contact us - Energy Monitor",
"Investment Monitor - Business intelligence for leaders in foreign direct investment",
"Home - Investment Monitor",
"About us - Investment Monitor",
"Page not found - Investment Monitor",
"Contact us - Investment Monitor",
"Contact us - City Monitor",
"Housing Archives - City Monitor",
"History Archives - City Monitor",
"Newsletter - Energy Monitor",
"Newsletter - City Monitor",
"subscribe12-old",
"xmas20",
"Activate"
"""
