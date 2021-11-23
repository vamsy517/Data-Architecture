from segmentation_report_engine.utils.constants import *
from ingest_utils.constants import PULLDATA_PROJECT_ID, PERMUTIVE_PROJECT_ID, PERMUTIVE_DATASET
from ingest_utils.database_gbq import get_gbq_table


def generate_actual_domains_v2() -> dict:
    """
    Generate domains dictionary for Segmentation V2
    :return: dictionary, containing list of domains for Segmentation V2
    """
    df_domains = get_gbq_table(PULLDATA_PROJECT_ID,
                               f"""SELECT distinct domain 
                               FROM `{PULLDATA_PROJECT_ID}.{DATASET_V2}.{TB_USERS_CLUSTERS_V2}`""")
    df_domains['key'] = df_domains['domain'].apply(lambda x: x.split(".")[0])
    df_domains['domain'] = df_domains['domain'].apply(lambda x: (x,))
    df_domains.set_index("key", drop=True, inplace=True)
    return df_domains.to_dict()['domain']


def generate_clusters_dict(table: str) -> dict:
    """
    Generate clusters dictionary with number of clusters per domain for Segmentation V2
    :param table: table from GBQ, used to generate the cluster dictionary
    :return: dictionary, containing list of domains and number clusters for Segmentation V2
    """
    df_clusters = get_gbq_table(PULLDATA_PROJECT_ID,
                                f"""SELECT domain, 
                                CASE WHEN max(cluster) is null THEN 0
                                ELSE max(cluster) + 1
                                END as n_clusters FROM `{PULLDATA_PROJECT_ID}.{DATASET_V2}.{table}` 
                                group by domain""")
    df_clusters.set_index("domain", drop=True, inplace=True)
    return df_clusters.to_dict()['n_clusters']


def extend_domain(domains_dict: dict, domain: str) -> tuple:
    """
    Add www. to the provided domain
    :param domains_dict: dictionary, containing domains for Segmentation V2
    :param domain: provided domain, which to be extended
    :return: a tuple with the original domain and its extended version with www.
    """
    domain_url = domains_dict.get(domain)
    domain_url += tuple('www.' + domain for domain in domain_url)
    return domain_url


def generate_case_statement(domains_dict: dict) -> str:
    """
    Generates the CASE statement in RAW_PAGEVIEW_DATA
    :param domains_dict: dictionary, containing domains for Segmentation V2
    :return: string case statement for all domains
    """
    case_query = ""
    for key, value in domains_dict.items():
        for v in value:
            case_query += f" WHEN properties.client.domain = '{v}' THEN 'www.{value[0]}' "
    return case_query


def generate_domain_filter(domains_dict: dict) -> str:
    """
    Generates the domain filter in RAW_PAGEVIEW_DATA
    :param domains_dict: dictionary, containing domains for Segmentation V2
    :return: string domain filter
    """
    domains_filter = ()
    for domain in domains_dict.keys():
        domains_filter += extend_domain(domains_dict, domain)
    return str(domains_filter)


# generate actual domains list for Segmentation V2
ACTUAL_DOMAINS_V2 = generate_actual_domains_v2()


# set queries, containing cluster data for Segmentation V2
CLUSTER_DATA_V2 = f"""
            cluster_data as (
            SELECT * except (domain),
            concat('www.', domain) as domain
            FROM `{PULLDATA_PROJECT_ID}.{DATASET_V2}.{TB_USERS_CLUSTERS_V2}`
            WHERE EXTRACT(DATE FROM DATETIME(cluster_change_date))='{DATES[0]}'
            )
            """


# set query, which gets raw pageview data between yesterday and 31 days ago for specific domains
RAW_PAGEVIEW_DATA = """
            #extract ALL Monitor data between yesterday and 31 days ago
            raw_data as (select *,
            EXTRACT(DATE FROM DATETIME (time)) as day,
            CASE
            {generate_case_statement} """ + f"""
            ELSE properties.client.domain
            END as domain,
            row_number() over (partition by user_id,properties.client.domain order by time DESC) as rn
            FROM `{PERMUTIVE_PROJECT_ID}.{PERMUTIVE_DATASET}.{PAGEVIEW_EVENTS}`
            WHERE properties.client.type in ('amp', 'web')
            AND properties.client.user_agent NOT LIKE '%bot%'
            AND properties.client.user_agent NOT LIKE '%Google Web Preview%' """ + """
            AND properties.client.domain in {generate_domain_filter} """ + f"""
            AND EXTRACT(DATE FROM DATETIME (time)) >= DATE_ADD('{DATE}', INTERVAL -31 DAY)
            AND EXTRACT(DATE FROM DATETIME (time)) <= '{DATES[0]}'
            )
            """

# generate different queries for Segmentation V1 and V2
RAW_PAGEVIEW_DATA_V2 = RAW_PAGEVIEW_DATA.format(generate_case_statement=generate_case_statement(ACTUAL_DOMAINS_V2),
                                                generate_domain_filter=generate_domain_filter(ACTUAL_DOMAINS_V2))

# set query, which gets only latest data
LATEST_PAGEVIEW_DATA = f"""
            # keep only latest entry for each user for yesterday
            latest_data as (
            select day, user_id, domain,time,properties.client.type ,properties.client.url, properties.client.referrer,
            properties.client.user_agent, properties.geo_info.continent from raw_data where rn=1 and day='{DATES[0]}'
            )
            """
# set query, which gets number of visits for each user for the past 30 days prior to the current entry
GET_VISITS = """
            # get number of visits for each user for the past 30 days prior to the current entry
            get_visits as (
            select L.*,
            (select
            count(distinct R.day)
            from raw_data as R
            where R.user_id = L.user_id
            and R.day < L.day
            and R.day >= L.day - 30
            and R.domain = L.domain
            ) as nb_visits_30_days
            from latest_data as L
            )
            """
# set query, which adds the cluster for each user
ADD_CLUSTER = """
            # add the cluster for each user
            join_cluster as (
            select gv.*,
            cd.cluster,
            CASE
                WHEN nb_visits_30_days>0 THEN 'Y'
                ELSE 'N'
                END as returning_visitor
            from get_visits as gv
            left join cluster_data as cd
            on cd.user_id = gv.user_id and cd.domain = gv.domain
            )
            """
# set query, which calculates number of returning visitors per cluster, domain and day
CALCULATE_RETURNING = """
            # count the number of returning visitors
            calculate_returning as (
            select domain, cluster, day,
            count(distinct user_id) as nb_returning
            from join_cluster
            where returning_visitor = 'Y'
            group by cluster,day,domain
            )
            """
# set query, which calculates total number of visitors per cluster, domain and day
CALCULATE_TOTAL = """
            # count the total number of visitors
            calculate_total as (
            select domain, cluster, day,
            count(distinct user_id) as nb_total
            from join_cluster
            group by cluster,day,domain
            )
            """
# set query, which calculates percent of returning visitors
CALCULATE_PERCENT_RETURNING_VISITORS = """
            # calculate percent of returning visitors
            calculate_percent_returning as (
            select ct.*,cr.nb_returning,
            ROUND((cr.nb_returning/ct.nb_total)*100, 2) as percent_returning from calculate_total as ct
            join calculate_returning as cr
            on ct.cluster=cr.cluster and ct.domain= cr.domain
            )
            """
# set query, which selects the final columns
FINAL_PERCENT_RETURNING_PER_DAY = """
            select domain,cluster,day,percent_returning from calculate_percent_returning
        """
# set query, which selects the final columns
FINAL_RF_METRICS_REPORT = """ 
                    select * from (
                        select * from (
                            select user_id, time, domain, type, referrer, url, continent, 
                            user_agent, cluster, 
                            row_number() over (partition by user_id order by time DESC) as rn
                    FROM join_cluster where nb_visits_30_days=0
                            )
                    where cluster is not null
                    )  where rn = 1
                    """
# set list of ctes, all present in the main query for both tables for Segmentation V2
MAIN_QUERY_LIST_V2 = [CLUSTER_DATA_V2, RAW_PAGEVIEW_DATA_V2, LATEST_PAGEVIEW_DATA, GET_VISITS, ADD_CLUSTER]

# set query, which gets base data from Segmentation_v2.{table} for specific domain and table for Segmentation V2
BASE_SEGMENTATION_DATA_V2 = """
        SELECT * FROM `{project}.{dataset}.{table}` 
        WHERE domain = '{domain}'
        AND EXTRACT(DATE FROM DATETIME (date)) = EXTRACT(DATE FROM DATETIME (
        (select min(date) FROM `{project}.{dataset}.{table}` 
        WHERE domain = '{domain}')))
        """

# set query, which gets yesterday data from Segmentation_v2.{table} for specific domain and table
YESTERDAY_SEGMENTATION_DATA_V2 = """
        SELECT * FROM `{project}.{dataset}.{table}`
        WHERE domain = '{domain}' 
        AND EXTRACT(DATE FROM DATETIME (date)) = '{yesterday}'
        """

# set query, which gets base data from Segmentation_v2.percent_returning_visitors_per_day
BASE_RETURNING_VISITORS_DATA_V2 = f"""
        WITH get_min_day AS (
        SELECT domain,min(day) as min_day FROM `{PULLDATA_PROJECT_ID}.{DATASET_V2}.{TB_PERCENT_RETURNING}` 
        GROUP BY domain
        ),
        get_base_day AS (
        SELECT original.* FROM `{PULLDATA_PROJECT_ID}.{DATASET_V2}.{TB_PERCENT_RETURNING}` as original
        JOIN get_min_day as gmd
        ON original.domain = gmd.domain
        AND original.day = gmd.min_day
        )
        SELECT domain, avg(percent_returning) as base_avg FROM get_base_day
        GROUP BY domain
        """

# set query, which gets base data from Segmentation_v2.RF_metrics_report
BASE_CLASSIFIER_DATA_V2 = f"""
        WITH get_min_day AS (
        SELECT domain,min(date) as min_day FROM `{PULLDATA_PROJECT_ID}.{DATASET_V2}.{TB_RF_METRICS_REPORT}`
        GROUP BY domain ),
        get_base_day AS (
        SELECT original.* FROM `{PULLDATA_PROJECT_ID}.{DATASET_V2}.{TB_RF_METRICS_REPORT}` as original
        JOIN get_min_day as gmd
        ON original.domain = gmd.domain
        AND original.date = gmd.min_day 
        )
        SELECT domain, avg(weighted_avg_precision) as base_avg_precision,
        avg(weighted_avg_recall) as base_avg_recall,
        avg(weighted_avg_f1_score) as base_avg_f1_score
        FROM ( 
        SELECT domain,
        sum(precision * support) / sum(support) as weighted_avg_precision,
        sum(recall * support) / sum(support) as weighted_avg_recall,
        sum(f1_score * support) / sum(support) as weighted_avg_f1_score from get_base_day
        GROUP BY domain)
        GROUP BY domain """
