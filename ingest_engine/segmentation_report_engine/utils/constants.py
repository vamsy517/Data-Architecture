from datetime import datetime, timedelta


# set segmentation folder
SEGMENTATION_FOLDER = '/home/airflow_gcp/segmentation/'
# set random_state variable to ensure equality of results between different runs
RANDOM_STATE = 0

# set list of processed dates (currently it is set only to yesterday date)
DATES = (datetime.strftime((datetime.now() - timedelta(1)), '%Y-%m-%d'),)
# set today date
DATE = datetime.strftime(datetime.now(), '%Y-%m-%d')
# set list of analyzed features
DIM_FOR_MEASURING_BEHAVIOUR = ['number_of_views', 'visits_from_social', 'visits_from_search',
                               'visits_from_email', 'lead_or_not', 'avg_timestamp_for_continent',
                               'average_virality']
# set list of industry features
DIM_FOR_MEASURING_INDUSTRY = ['industry_per_cluster', 'industry_total', 'weight_ratio']
# set list of Jobs features
DIM_FOR_MEASURING_JOBS = ['jobs_per_cluster', 'jobs_total', 'weight_ratio']
# set list of Jobs features
DIM_FOR_MEASURING_SENIORITY = ['seniority_per_cluster', 'seniority_total', 'weight_ratio']
# set Permutive pageview events table
PAGEVIEW_EVENTS = 'pageview_events'
DATASET_V2 = 'Segmentation_v2'
# set uploaded to GBQ table names
TB_PERCENT_RETURNING = 'percent_returning_visitors_per_day'
TB_RF_METRICS_REPORT = 'RF_metrics_report'
TB_ENGINE_REPORT = 'Segmentation_Engine_Report'
# set table names, used in constants_queries
TB_USERS_CLUSTERS_V2 = 'users_clusters'
# set list of social platforms
SOCIAL_LIST = ['facebook', 'linkedin', 'twitter', 't.co/', 'youtube', 'instagram', 'reddit', 'pinterest']
# set list of search platforms
SEARCH_LIST = ['ask', 'baidu', 'bing', 'duckduckgo', 'google', 'yahoo', 'yandex']
# set list of email platforms
EMAIL_LIST = ['Pardot', 'pardot']
# set list of NSMG Network domains
NSMG_NETWORK = ['energymonitor.ai', 'investmentmonitor.ai', 'citymonitor.ai', 'newstatesman.com',
                   'cbronline.com', 'techmonitor.ai', 'elitetraveler.com']

# set hour difference for localize_hour function
NA_HOUR_DIFF = -6
SA_HOUR_DIFF = -3
EU_HOUR_DIFF = 1
AF_HOUR_DFF = 1
AS_HOUR_DIFF = 5

# set user agent strings for extract_os_group function
LINUX_AGENT_STR = 'Linux'
ANDROID_AGENT_STR = 'Android'
WIN10_AGENT_STR = 'Windows NT 10.0'
WIN_AGENT_STR = 'Windows'
IPHONE_AGENT_STR = 'iPhone'
MAC_AGENT_STR = 'Macintosh'
APPLE_AGENT_STR = 'AppleWebKit'