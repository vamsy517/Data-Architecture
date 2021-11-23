import re
import pandas as pd
from google.cloud import bigquery, bigquery_storage  # type: ignore
from google.cloud.exceptions import NotFound
import segmentation.cluster_model.segmentation_constants as constants
import segmentation.probabilistic_model.classifier_constants as classifier_constants
from segmentation.downloader.cluster_model_downloader import ClusterDownloader
from datetime import datetime, timedelta
from segmentation.config.load_config import load_config
from typing import Tuple


# Config
config = load_config()


class ProbDownloader:
    """
    Class for uploading and downloading data from BigQuery for the probability models

    Args:
        model_type (str): specify model type
        data_version (int): data version

    """

    # initialize the class
    def __init__(self, model_type: str, data_version: int):
        self._model_type = model_type
        self._data_version = data_version
        (
            self.bqclient,
            self.bqstorageclient,
            self.pulldata_project,
            self.permutive_project,
        ) = self.get_clients()

    def get_clients(self):
        """
        Gets BigQuery connection information

        Returns:
            tuple: BigQuery client and project names

        """
        # set the permutive credentials
        pulldata_project = constants.PULLDATA_PROJECT
        permutive_project = constants.PERMUTIVE_PROJECT
        bqclient = bigquery.Client(project=pulldata_project)
        bqstorageclient = bigquery_storage.BigQueryReadClient()

        return bqclient, bqstorageclient, pulldata_project, permutive_project

    def upload_attribute_probabilities(
        self, df: pd.DataFrame, domain: str, attribute: str
    ) -> None:
        """
        Uploads probability table to BigQuery

        Args:
            df (pd.DataFrame): dataframe to upload
            domain (str): which domain
            attribute (str): which attribute

        """

        # Create table ID
        table_id = f"Segmentation_v2.{attribute}_{domain}_probabilities"

        # Configure upload
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=[
                bigquery.SchemaField("cluster", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("attribute", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "cluster_total", bigquery.enums.SqlTypeNames.INTEGER
                ),
                bigquery.SchemaField(
                    "data_date", bigquery.enums.SqlTypeNames.TIMESTAMP
                ),
                bigquery.SchemaField("user_count", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField(
                    "user_count_total", bigquery.enums.SqlTypeNames.INTEGER
                ),
                bigquery.SchemaField("alpha", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("alpha_prime", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField(
                    "alpha_prime_sum", bigquery.enums.SqlTypeNames.FLOAT
                ),
                bigquery.SchemaField("probability", bigquery.enums.SqlTypeNames.FLOAT),
            ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_TRUNCATE",
        )

        job = self.bqclient.load_table_from_dataframe(
            df, f"{self.pulldata_project}.{table_id}", job_config=job_config
        )  # Make an API request.
        job.result()

    def get_probability_table_from_bq(
        self, domain: str, attribute="industry", date="latest"
    ) -> pd.DataFrame:
        """
        Gets table from BigQuery with each user's probability for given attribute on given domain. These are outputs from the probabilistic, cluster level models

        Args:
            domain (str): which domain
            attribute (str): which attribute - currently just industry
            date (str): latest or specific date

        Returns:
            pd.DataFrame: The return value dataframe

        """

        if date == "latest":
            sql = f"""SELECT T1.*
                      FROM `project-pulldata.Segmentation_v2.{attribute}_{domain}_probabilities` as T1
                      INNER JOIN (
                        SELECT max(data_date) as data_date FROM `project-pulldata.Segmentation_v2.{attribute}_{domain}_probabilities`
                      ) as T2
                      ON T1.data_date = T2.data_date"""
        else:
            sql = f"""SELECT *
                      FROM `project-pulldata.Segmentation_v2.{attribute}_{domain}_probabilities`
                      WHERE data_date = '{date}'"""

        prob_df = (
            self.bqclient.query(sql)
            .result()
            .to_dataframe(bqstorage_client=self.bqstorageclient)
        )

        return prob_df

    def get_user_probability_table_from_bq(
        self,
        attribute="industry",
        timestamp="latest",
        max_only=False,
    ) -> pd.DataFrame:
        """
        Get user probability table for each attribute level - these are the results from the classification model

        Args:
            attribute (str): which attribute
            timestamp (str): latest or specific date
            max_only (bool): include highest probability score only

        Returns:
            pd.DataFrame: The return value dataframe

        """

        domains_string = "' ,'".join(config["domains_in_app"])
        # make sure we mark unknowns as 'unknown' rather than 'Others' - latest instructions...
        if attribute == "company":
            sql = f"""
                        WITH T3 AS (
                            SELECT
                              T1.user_id,
                              T1.properties.client.domain AS domain,
                              CASE WHEN T2.company_name is null THEN "Unknown"
                              ELSE T2.company_name END AS company_name,
                              ROW_NUMBER() OVER(PARTITION BY T1.user_id) AS rank
                            FROM
                              `{self.permutive_project}.global_data.clearbit_events` AS T1
                            LEFT JOIN (
                              SELECT
                                properties.company.name AS company_name,
                                COUNT(DISTINCT user_id) AS cnt
                              FROM
                                `{self.permutive_project}.global_data.clearbit_events`
                              GROUP BY
                                properties.company.name
                              ORDER BY
                                cnt DESC
                              LIMIT
                                100) T2
                            ON
                              T1.properties.company.name = T2.company_name
                            WHERE
                              T1.properties.client.domain IN ('{domains_string}')
                            GROUP BY
                              T1.user_id,
                              T1.properties.client.domain,
                              T2.company_name)
                        SELECT user_id, company_name, city as company_city, country as company_country,
                        state as company_state, cdna.* except(name, rn, city, country, state)
                        FROM T3
                        LEFT JOIN `{self.pulldata_project}.Segmentation_v2.company_dna_20210722` as cdna
                        on T3.company_name = cdna.name
                        WHERE rank = 1
            """
        else:
            if timestamp == "latest":
                if max_only:
                    sql = f"""
                               WITH
                                  TBL1 AS (
                                  SELECT
                                    timestamp,
                                    user_id,
                                    recency, 
                                    frequency,
                                    country,
                                    continent,
                                    province,
                                    city,
                                    domain,
                                    attribute,
                                    known,
                                    probability
                                  FROM (
                                    SELECT
                                      *,
                                      ROW_NUMBER() OVER (PARTITION BY timestamp, user_id ORDER BY probability DESC ) rn
                                    FROM
                                      `{self.pulldata_project}.Segmentation_v2.{attribute}_user_probabilities` ) t
                                  WHERE
                                    rn = 1)
                                SELECT
                                  T1.*
                                FROM
                                  TBL1 AS T1
                                INNER JOIN (
                                  SELECT
                                    domain,
                                    MAX(timestamp) AS timestamp
                                  FROM
                                    `{self.pulldata_project}.Segmentation_v2.{attribute}_user_probabilities`
                                  WHERE
                                    domain IN ('{domains_string}')
                                  GROUP BY
                                    domain) AS T2
                                ON
                                  T1.timestamp = T2.timestamp
                                  AND T1.domain = T2.domain
                                WHERE T1.domain in ('{domains_string}')
                               """
                else:
                    sql = f"""SELECT T1.*
                               FROM `{self.pulldata_project}.Segmentation_v2.{attribute}_user_probabilities` as T1
                               INNER JOIN
                                (SELECT domain, MAX(timestamp) as timestamp 
                                FROM `{self.pulldata_project}.Segmentation_v2.{attribute}_user_probabilities`
                                WHERE domain in ('{domains_string}')
                                GROUP BY domain) as T2
                                ON T1.timestamp = T2.timestamp AND T1.domain = T2.domain
                               WHERE T1.domain in ('{domains_string}')"""
            else:
                if max_only:
                    sql = f"""
                                WITH
                                   TBL1 AS (
                                   SELECT
                                     timestamp,
                                     user_id,
                                     recency, 
                                     frequency,
                                     country,
                                     continent,
                                     province,
                                     city,
                                     domain,
                                     attribute,
                                     known,
                                     probability
                                   FROM (
                                     SELECT
                                       *,
                                       ROW_NUMBER() OVER (PARTITION BY timestamp, user_id ORDER BY probability DESC ) rn
                                     FROM
                                       `{self.pulldata_project}.Segmentation_v2.{attribute}_user_probabilities` ) t
                                   WHERE
                                     rn = 1)
                                 SELECT
                                   *
                                 FROM
                                   TBL1
                                 WHERE domain in ('{domains_string}')
                                    AND timestamp = '{timestamp}'
                                """
                else:
                    sql = f"""SELECT *
                               FROM `{self.pulldata_project}.Segmentation_v2.{attribute}_user_probabilities`
                               WHERE domain in ('{domains_string}')
                               AND timestamp = '{timestamp}'"""

        prob_df = (
            self.bqclient.query(sql)
            .result()
            .to_dataframe(bqstorage_client=self.bqstorageclient)
        )

        return prob_df

    def download_attribute_profiles(
        self, domain: str, attribute: str
    ) -> Tuple[pd.DataFrame, str]:
        """
        Downloads counts for each attribute level for each cluster, used in the probabilistic model

        Args:
            domain (str): which domain
            attribute (str): which attribute

        Returns:
            tuple: The return value dataframe and the domain string

        """
        attribute_table_name = f"{attribute}_profiles"
        domain = domain.replace(" ", "").replace("_", "").lower()
        dataset_name = f"Segmentation_{domain}"

        sql = f"""SELECT t1.{attribute}, t1.cluster, t1.{attribute}_per_cluster, t1.cluster_total, t1.date as data_date
                  FROM `{self.pulldata_project}.{dataset_name}.{attribute_table_name}` AS t1
                  INNER JOIN (
                    SELECT MAX(date) as max_date
                    FROM `{self.pulldata_project}.{dataset_name}.{attribute_table_name}`) AS t2
                  ON t1.date = t2.max_date
                  LIMIT 1000"""

        attribute_profile_df = (
            self.bqclient.query(sql)
            .result()
            .to_dataframe(bqstorage_client=self.bqstorageclient)
        )

        attribute_profile_df.columns = [
            "attribute",
            "cluster",
            "user_count",
            "cluster_total",
            "data_date",
        ]

        return attribute_profile_df, domain

    def get_features_and_attributes(
        self,
        domain: str,
        attribute: str,
        end_date: str,
        lookback_period: int,
    ) -> pd.DataFrame:
        """
        Gets training data for the classification model - for each user their features and their assigned attribute

        Args:
            domain (str): which domain
            attribute (str): which attribute
            end_date (str): data end date
            lookback_period (int): num of days to look back

        Returns:
            pd.DataFrame: The return value dataframe

        """

        # Set table_id to the ID of the destination table.
        domain_no_dot = re.sub("\\.", "_", domain)
        table_id = f"{self.pulldata_project}.{constants.DATASET}.features_{attribute}_{domain_no_dot}"

        # Check if table exists
        if self.does_table_exist(table_id):

            self.bqclient.get_table(table_id)
            features_query = f"""
                SELECT * 
                FROM `{table_id}` 
                WHERE end_date = '{end_date}'
                AND lookback_period = '{lookback_period}'"""

            features_df = (
                self.bqclient.query(features_query)
                .result()
                .to_dataframe(bqstorage_client=self.bqstorageclient)
            )

            if features_df.shape[0] == 0:
                features_df = self.create_features_attributes_table(
                    attribute, domain, end_date, lookback_period, table_id
                )
            else:
                print("Using existing table.")

        else:
            features_df = self.create_features_attributes_table(
                attribute, domain, end_date, lookback_period, table_id
            )

        return features_df

    def get_industry_data(
        self, domain: str, end_date: str, lookback_period: int, table_id: str
    ) -> pd.DataFrame:
        """
        Creates features and attributes table for each user

        Args:
            domain (str): which domain
            end_date (str): data end date
            lookback_period (int): num of days to look back
            table_id (str): BQ table

        Returns:
            pd.DataFrame: The return value dataframe

        """

        # Create virality temp table
        domains_quotations = [f"'{domain}', 'www.{domain}'"]
        full_list_of_domains = ", ".join(domains_quotations)

        # also create a list to merge the alternate domains' data with a case
        case_domains = [
            f"WHEN properties.client.domain = '{domain}' THEN 'www.{domain}'"
        ]
        domains_case_query = " ".join(case_domains)

        cluster_downloader = ClusterDownloader(
            model_type="classification", data_version=self._data_version
        )
        virality_table_id = cluster_downloader.generate_virality(
            full_list_of_domains, domains_case_query, end_date
        )

        # Generate query
        # calculate the start date
        start_date = datetime.strftime(
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(lookback_period),
            "%Y-%m-%d",
        )
        # transform the referrer sites to a query string
        social_refs = [
            f"WHEN properties.client.referrer like '%{site}%' then 1"
            for site in constants.SOCAL_REF_LIST
        ]
        social_ref_case_query = " ".join(social_refs)
        search_refs = [
            f"WHEN properties.client.referrer like '%{site}%' then 1"
            for site in constants.SEARCH_REF_LIST
        ]
        search_ref_case_query = " ".join(search_refs)
        email_refs = [
            f"WHEN properties.client.referrer like '%{site}%' then 1"
            for site in constants.EMAIL_REF_LIST
        ]
        email_ref_case_query = " ".join(email_refs)
        nsmg_refs = [
            f"WHEN properties.client.referrer like '%{site}%' then 1"
            for site in constants.NSMG_REF_LIST
        ]
        nsmg_ref_case_query = " ".join(nsmg_refs)
        own_refs = [
            f"WHEN properties.client.referrer like '%{site}%' then 1"
            for site in [domain, f"www.{domain}"]
        ]
        own_ref_case_query = " ".join(own_refs)

        features_query = f"""
                                WITH
                                  industry AS (
                                  SELECT
                                    user_id,
                                    sector,
                                    subindustry,
                                    industry,
                                    industrygroup
                                  FROM (
                                    SELECT
                                      user_id,
                                      properties.company.category.sector,
                                      properties.company.category.subindustry,
                                      properties.company.category.industry,
                                      properties.company.category.industrygroup,
                                      properties.geoIP.country,
                                      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY TIME DESC) AS rn
                                    FROM
                                      `{self.permutive_project}.global_data.clearbit_events`
                                    WHERE
                                      properties.client.domain in ({full_list_of_domains})
                                      AND properties.client.user_agent NOT LIKE '%bot%'
                                      AND properties.client.user_agent NOT LIKE '%Google Web Preview%' )
                                  WHERE
                                    rn = 1
                                    AND industry IS NOT NULL),
                                  get_raw_data AS (
                                  SELECT
                                    *
                                  FROM
                                    {self.permutive_project}.global_data.pageview_events
                                  WHERE
                                    DATE(_PARTITIONTIME) >= "{start_date}"
                                    AND DATE(_PARTITIONTIME) <= "{end_date}"
                                    AND properties.client.user_agent NOT LIKE '%bot%'
                                    AND properties.client.user_agent NOT LIKE '%Google Web Preview%'
                                    AND properties.client.domain in ({full_list_of_domains})),
                                  views_and_sessions AS (
                                  SELECT
                                    user_id,
                                    COUNT(DISTINCT session_id) AS number_of_sessions,
                                    COUNT(DISTINCT
                                      CASE
                                        WHEN properties.client.type = 'web' THEN view_id
                                      ELSE
                                      event_id
                                    END
                                      ) AS number_of_views
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id),
                                  geographic_info AS (
                                  SELECT
                                    user_id,
                                    country,
                                    continent,
                                    province,
                                    city
                                  FROM (
                                    SELECT
                                      user_id,
                                      properties.geo_info.country as country,
                                      properties.geo_info.continent as continent,
                                      properties.geo_info.province as province,
                                      properties.geo_info.city as city,
                                      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY time DESC) AS rn
                                    FROM
                                      get_raw_data )
                                  WHERE
                                    rn = 1),
                                  referrers AS (
                                  SELECT
                                    user_id,
                                    SUM(--Social
                                      CASE {social_ref_case_query}
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_social,
                                    SUM(--Search
                                      CASE {search_ref_case_query}
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_search,
                                    SUM(--Email
                                      CASE {email_ref_case_query}
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_email,
                                    SUM(--NSMG network
                                      CASE {nsmg_ref_case_query}
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_NSMG_network,
                                    SUM(--own website > redirect / linkclicks
                                      CASE {own_ref_case_query}
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_own_website,
                                    SUM(-- Typed-in URL
                                      CASE
                                        WHEN properties.client.referrer IS NULL THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visit_from_bookmark_or_url
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id ),
                                  amp_or_web AS (
                                  SELECT
                                    user_id,
                                    SUM(--Web
                                      CASE
                                        WHEN properties.client.type = 'web' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_web,
                                    SUM(--Mobile
                                      CASE
                                        WHEN properties.client.type = 'amp' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_mobile
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id ),
                                  lead_or_not AS(
                                  SELECT
                                    user_id,
                                    CASE
                                      WHEN earliest_event IS NULL THEN 0
                                    ELSE
                                    1
                                  END
                                    AS lead_or_not
                                  FROM (
                                    SELECT
                                      *
                                    FROM
                                      views_and_sessions AS users
                                    LEFT JOIN (
                                      SELECT
                                        user_id AS user_id_orig,
                                        earliest_event
                                      FROM
                                        {self.pulldata_project}.{constants.LEADS_DATASET_TABLE}) AS orig_leads
                                    ON
                                      users.user_id = orig_leads.user_id_orig ) ),
                                  timezone_corrected_timestamp AS (
                                  SELECT
                                    user_id,
                                    AVG( EXTRACT(hour
                                      FROM
                                        time AT TIME ZONE
                                        CASE
                                          WHEN properties.geo_info.continent IN ('North America') THEN 'US/Central'
                                          WHEN properties.geo_info.continent IN ('South America') THEN 'Brazil/West'
                                          WHEN properties.geo_info.continent IN ('Europe', 'Africa') THEN 'Europe/Berlin'
                                          WHEN properties.geo_info.continent IN ('Asia') THEN 'Asia/Karachi'
                                        ELSE
                                        'Etc/UTC'
                                      END
                                        ) ) AS avg_timestamp_for_continent
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id ),
                                  visits_from_continent AS (
                                  SELECT
                                    user_id,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent IN ('North America') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_northamerica,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent IN ('South America') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_southamerica,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent IN ('Europe') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_europe,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent IN ('Africa') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_africa,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent IN ('Asia') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_asia,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent NOT IN ('North America', 'South America', 'Europe', 'Africa', 'Asia') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_othercontinent
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id ),
                                  visits_from_os AS (
                                  SELECT
                                    user_id,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.LINUX_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_linux,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.ANDROID_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_android,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.WIN10_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_windows10,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.WIN_AGENT_STR}%' AND properties.client.user_agent NOT LIKE '%{constants.WIN10_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_windows_older,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.IPHONE_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_iphone,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.MAC_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_mac,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.APPLE_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_apple_other,
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id),
                                  dwell_and_completion AS (
                                  SELECT
                                    user_id,
                                    AVG(properties.aggregations.PageviewEngagement.completion) AS average_completion,
                                    AVG(properties.aggregations.PageviewEngagement.engaged_time) AS average_dwell
                                  FROM
                                    {self.permutive_project}.global_data.pageviewcomplete_events
                                  WHERE
                                    DATE(_PARTITIONTIME) >= "{start_date}"
                                    AND DATE(_PARTITIONTIME) <= "{end_date}"
                                    AND properties.aggregations.PageviewEngagement.completion != 0
                                    AND properties.aggregations.PageviewEngagement.engaged_time != 0
                                    AND properties.client.user_agent NOT LIKE '%bot%'
                                    AND properties.client.user_agent NOT LIKE '%Google Web Preview%'
                                    # leave only those events that are for the publications we work for
                                    AND properties.client.domain in ({full_list_of_domains})
                                  GROUP BY
                                    user_id ),
                                  avearge_virality AS (
                                  SELECT
                                    user_id,
                                    AVG(articles.virality) AS average_virality
                                  FROM
                                    get_raw_data AS users
                                  LEFT JOIN
                                    {virality_table_id} AS articles
                                  ON
                                    users.properties.client.title = articles.title
                                  GROUP BY
                                    user_id ),
                                  recency_and_frequency AS (
                                          SELECT
                                            user_id,
                                            MAX(time) AS latest_date,
                                            MIN(time) AS earliest_date,
                                            COUNT(DISTINCT session_id) AS n_sessions
                                          FROM
                                            {self.permutive_project}.global_data.pageviewcomplete_events
                                          WHERE
                                            DATE(_PARTITIONTIME) >= DATE_SUB(DATE "{end_date}", INTERVAL 1 YEAR)
                                            AND DATE(_PARTITIONTIME) <= "{end_date}"
                                            AND properties.client.user_agent NOT LIKE '%bot%'
                                            AND properties.client.user_agent NOT LIKE '%Google Web Preview%'
                                            AND properties.client.domain in ({full_list_of_domains})
                                          GROUP BY
                                            user_id
                                  )
                                SELECT
                                  views.*,
                                  ind.*EXCEPT(user_id),
                                  gg.*EXCEPT(user_id),
                                  ref.*EXCEPT(user_id),
                                  amp.*EXCEPT(user_id),
                                  lead.*EXCEPT(user_id),
                                  tz.*EXCEPT(user_id),
                                  cont.*EXCEPT(user_id),
                                  os.*EXCEPT(user_id),
                                  dwell.*EXCEPT(user_id),
                                  vir.*EXCEPT(user_id),
                                  '{end_date}' as end_date,
                                  '{lookback_period}' as lookback_period,
                                  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), rf.latest_date, day) AS recency,
                                  CASE
                                    WHEN (TIMESTAMP_DIFF(rf.latest_date, rf.earliest_date, day) / 28) < 1 THEN NULL
                                  ELSE
                                    n_sessions / (TIMESTAMP_DIFF(rf.latest_date, rf.earliest_date, day) / 28)
                                  END AS frequency
                                FROM
                                  views_and_sessions AS views
                                LEFT JOIN
                                  referrers AS ref
                                ON
                                  views.user_id = ref.user_id
                                LEFT JOIN
                                  amp_or_web AS amp
                                ON
                                  views.user_id = amp.user_id
                                LEFT JOIN
                                  lead_or_not AS lead
                                ON
                                  views.user_id = lead.user_id
                                LEFT JOIN
                                  timezone_corrected_timestamp AS tz
                                ON
                                  views.user_id = tz.user_id
                                LEFT JOIN
                                  visits_from_continent AS cont
                                ON
                                  views.user_id = cont.user_id
                                LEFT JOIN
                                  visits_from_os AS os
                                ON
                                  views.user_id = os.user_id
                                LEFT JOIN
                                  dwell_and_completion AS dwell
                                ON
                                  views.user_id = dwell.user_id
                                LEFT JOIN
                                  avearge_virality AS vir
                                ON
                                  views.user_id = vir.user_id
                                LEFT JOIN
                                  industry AS ind
                                ON
                                  views.user_id = ind.user_id
                                LEFT JOIN
                                  recency_and_frequency as rf
                                ON
                                  views.user_id = rf.user_id
                                LEFT JOIN
                                    geographic_info AS gg
                                ON
                                  views.user_id = gg.user_id
                                """

        # Run
        job_config = bigquery.QueryJobConfig(
            destination=table_id, write_disposition="WRITE_APPEND"
        )
        print(f"Saving table at {table_id}.")

        features_df = (
            self.bqclient.query(features_query, job_config=job_config)
            .result()
            .to_dataframe(bqstorage_client=self.bqstorageclient)
        )

        if virality_table_id is not None:
            self.bqclient.delete_table(
                virality_table_id, not_found_ok=True
            )  # Make an API request.

            print(f"Deleted temp virality table {virality_table_id}.")

        return features_df

    def get_job_title_data(
        self, domain: str, end_date: str, lookback_period: int, table_id: str
    ) -> pd.DataFrame:
        """
        Creates features and attributes table for each user

        Args:
            domain (str): which domain
            end_date (str): data end date
            lookback_period (int): num of days to look back
            table_id (str): BQ table

        Returns:
            pd.DataFrame: The return value dataframe

        """

        # Create virality temp table
        domains_quotations = [f"'{domain}', 'www.{domain}'"]
        full_list_of_domains = ", ".join(domains_quotations)

        # also create a list to merge the alternate domains' data with a case
        case_domains = [
            f"WHEN properties.client.domain = '{domain}' THEN 'www.{domain}'"
        ]
        domains_case_query = " ".join(case_domains)

        cluster_downloader = ClusterDownloader(
            model_type="classification", data_version=self._data_version
        )
        virality_table_id = cluster_downloader.generate_virality(
            full_list_of_domains, domains_case_query, end_date
        )

        # Generate query
        # calculate the start date
        start_date = datetime.strftime(
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(lookback_period),
            "%Y-%m-%d",
        )
        # transform the referrer sites to a query string
        social_refs = [
            f"WHEN properties.client.referrer like '%{site}%' then 1"
            for site in constants.SOCAL_REF_LIST
        ]
        social_ref_case_query = " ".join(social_refs)
        search_refs = [
            f"WHEN properties.client.referrer like '%{site}%' then 1"
            for site in constants.SEARCH_REF_LIST
        ]
        search_ref_case_query = " ".join(search_refs)
        email_refs = [
            f"WHEN properties.client.referrer like '%{site}%' then 1"
            for site in constants.EMAIL_REF_LIST
        ]
        email_ref_case_query = " ".join(email_refs)
        nsmg_refs = [
            f"WHEN properties.client.referrer like '%{site}%' then 1"
            for site in constants.NSMG_REF_LIST
        ]
        nsmg_ref_case_query = " ".join(nsmg_refs)
        own_refs = [
            f"WHEN properties.client.referrer like '%{site}%' then 1"
            for site in [domain, f"www.{domain}"]
        ]
        own_ref_case_query = " ".join(own_refs)

        features_query = f"""
                                WITH
                                  industry AS (
                                  SELECT
                                    user_id,
                                    sector,
                                    subindustry,
                                    industry,
                                    industrygroup
                                  FROM (
                                    SELECT
                                      user_id,
                                      properties.company.category.sector,
                                      properties.company.category.subindustry,
                                      properties.company.category.industry,
                                      properties.company.category.industrygroup,
                                      properties.geoIP.country,
                                      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY TIME DESC) AS rn
                                    FROM
                                      `{self.permutive_project}.global_data.clearbit_events`
                                    WHERE
                                      properties.client.domain in ({full_list_of_domains})
                                      AND properties.client.user_agent NOT LIKE '%bot%'
                                      AND properties.client.user_agent NOT LIKE '%Google Web Preview%' )
                                  WHERE
                                    rn = 1
                                    AND industry IS NOT NULL),
                                  get_raw_data AS (
                                  SELECT
                                    *
                                  FROM
                                    {self.permutive_project}.global_data.pageview_events
                                  WHERE
                                    DATE(_PARTITIONTIME) >= "{start_date}"
                                    AND DATE(_PARTITIONTIME) <= "{end_date}"
                                    AND properties.client.user_agent NOT LIKE '%bot%'
                                    AND properties.client.user_agent NOT LIKE '%Google Web Preview%'
                                    AND properties.client.domain in ({full_list_of_domains})),
                                  views_and_sessions AS (
                                  SELECT
                                    user_id,
                                    COUNT(DISTINCT session_id) AS number_of_sessions,
                                    COUNT(DISTINCT
                                      CASE
                                        WHEN properties.client.type = 'web' THEN view_id
                                      ELSE
                                      event_id
                                    END
                                      ) AS number_of_views
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id),
                                  geographic_info AS (
                                  SELECT
                                    user_id,
                                    country,
                                    continent,
                                    province,
                                    city
                                  FROM (
                                    SELECT
                                      user_id,
                                      properties.geo_info.country as country,
                                      properties.geo_info.continent as continent,
                                      properties.geo_info.province as province,
                                      properties.geo_info.city as city,
                                      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY time DESC) AS rn
                                    FROM
                                      get_raw_data )
                                  WHERE
                                    rn = 1),
                                  referrers AS (
                                  SELECT
                                    user_id,
                                    SUM(--Social
                                      CASE {social_ref_case_query}
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_social,
                                    SUM(--Search
                                      CASE {search_ref_case_query}
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_search,
                                    SUM(--Email
                                      CASE {email_ref_case_query}
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_email,
                                    SUM(--NSMG network
                                      CASE {nsmg_ref_case_query}
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_NSMG_network,
                                    SUM(--own website > redirect / linkclicks
                                      CASE {own_ref_case_query}
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_own_website,
                                    SUM(-- Typed-in URL
                                      CASE
                                        WHEN properties.client.referrer IS NULL THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visit_from_bookmark_or_url
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id ),
                                  amp_or_web AS (
                                  SELECT
                                    user_id,
                                    SUM(--Web
                                      CASE
                                        WHEN properties.client.type = 'web' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_web,
                                    SUM(--Mobile
                                      CASE
                                        WHEN properties.client.type = 'amp' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_mobile
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id ),
                                  lead_or_not AS(
                                  SELECT
                                    user_id,
                                    CASE
                                      WHEN earliest_event IS NULL THEN 0
                                    ELSE
                                    1
                                  END
                                    AS lead_or_not
                                  FROM (
                                    SELECT
                                      *
                                    FROM
                                      views_and_sessions AS users
                                    LEFT JOIN (
                                      SELECT
                                        user_id AS user_id_orig,
                                        earliest_event
                                      FROM
                                        {self.pulldata_project}.{constants.LEADS_DATASET_TABLE}) AS orig_leads
                                    ON
                                      users.user_id = orig_leads.user_id_orig ) ),
                                  timezone_corrected_timestamp AS (
                                  SELECT
                                    user_id,
                                    AVG( EXTRACT(hour
                                      FROM
                                        time AT TIME ZONE
                                        CASE
                                          WHEN properties.geo_info.continent IN ('North America') THEN 'US/Central'
                                          WHEN properties.geo_info.continent IN ('South America') THEN 'Brazil/West'
                                          WHEN properties.geo_info.continent IN ('Europe', 'Africa') THEN 'Europe/Berlin'
                                          WHEN properties.geo_info.continent IN ('Asia') THEN 'Asia/Karachi'
                                        ELSE
                                        'Etc/UTC'
                                      END
                                        ) ) AS avg_timestamp_for_continent
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id ),
                                  visits_from_continent AS (
                                  SELECT
                                    user_id,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent IN ('North America') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_northamerica,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent IN ('South America') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_southamerica,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent IN ('Europe') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_europe,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent IN ('Africa') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_africa,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent IN ('Asia') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_asia,
                                    SUM(CASE
                                        WHEN properties.geo_info.continent NOT IN ('North America', 'South America', 'Europe', 'Africa', 'Asia') THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_othercontinent
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id ),
                                  visits_from_os AS (
                                  SELECT
                                    user_id,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.LINUX_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_linux,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.ANDROID_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_android,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.WIN10_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_windows10,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.WIN_AGENT_STR}%' AND properties.client.user_agent NOT LIKE '%{constants.WIN10_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_windows_older,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.IPHONE_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_iphone,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.MAC_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_mac,
                                    SUM(CASE
                                        WHEN properties.client.user_agent LIKE '%{constants.APPLE_AGENT_STR}%' THEN 1
                                      ELSE
                                      0
                                    END
                                      ) AS visits_from_apple_other,
                                  FROM
                                    get_raw_data
                                  GROUP BY
                                    user_id),
                                  dwell_and_completion AS (
                                  SELECT
                                    user_id,
                                    AVG(properties.aggregations.PageviewEngagement.completion) AS average_completion,
                                    AVG(properties.aggregations.PageviewEngagement.engaged_time) AS average_dwell
                                  FROM
                                    {self.permutive_project}.global_data.pageviewcomplete_events
                                  WHERE
                                    DATE(_PARTITIONTIME) >= "{start_date}"
                                    AND DATE(_PARTITIONTIME) <= "{end_date}"
                                    AND properties.aggregations.PageviewEngagement.completion != 0
                                    AND properties.aggregations.PageviewEngagement.engaged_time != 0
                                    AND properties.client.user_agent NOT LIKE '%bot%'
                                    AND properties.client.user_agent NOT LIKE '%Google Web Preview%'
                                    # leave only those events that are for the publications we work for
                                    AND properties.client.domain in ({full_list_of_domains})
                                  GROUP BY
                                    user_id ),
                                  avearge_virality AS (
                                  SELECT
                                    user_id,
                                    AVG(articles.virality) AS average_virality
                                  FROM
                                    get_raw_data AS users
                                  LEFT JOIN
                                    {virality_table_id} AS articles
                                  ON
                                    users.properties.client.title = articles.title
                                  GROUP BY
                                    user_id ),
                                  recency_and_frequency AS (
                                          SELECT
                                            user_id,
                                            MAX(time) AS latest_date,
                                            MIN(time) AS earliest_date,
                                            COUNT(DISTINCT session_id) AS n_sessions
                                          FROM
                                            {self.permutive_project}.global_data.pageviewcomplete_events
                                          WHERE
                                            DATE(_PARTITIONTIME) >= DATE_SUB(DATE "{end_date}", INTERVAL 1 YEAR)
                                            AND DATE(_PARTITIONTIME) <= "{end_date}"
                                            AND properties.client.user_agent NOT LIKE '%bot%'
                                            AND properties.client.user_agent NOT LIKE '%Google Web Preview%'
                                            AND properties.client.domain in ({full_list_of_domains})
                                          GROUP BY
                                            user_id
                                  )
                                SELECT
                                  views.*,
                                  job_titles.*EXCEPT(permutive_id),
                                  ind.*EXCEPT(user_id),
                                  gg.*EXCEPT(user_id),
                                  ref.*EXCEPT(user_id),
                                  amp.*EXCEPT(user_id),
                                  lead.*EXCEPT(user_id),
                                  tz.*EXCEPT(user_id),
                                  cont.*EXCEPT(user_id),
                                  os.*EXCEPT(user_id),
                                  dwell.*EXCEPT(user_id),
                                  vir.*EXCEPT(user_id),
                                  '{end_date}' as end_date,
                                  '{lookback_period}' as lookback_period,
                                  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), rf.latest_date, day) AS recency,
                                  CASE
                                    WHEN (TIMESTAMP_DIFF(rf.latest_date, rf.earliest_date, day) / 28) < 1 THEN NULL
                                  ELSE
                                    n_sessions / (TIMESTAMP_DIFF(rf.latest_date, rf.earliest_date, day) / 28)
                                  END AS frequency
                                FROM
                                  views_and_sessions AS views
                                LEFT JOIN
                                  referrers AS ref
                                ON
                                  views.user_id = ref.user_id
                                LEFT JOIN
                                  amp_or_web AS amp
                                ON
                                  views.user_id = amp.user_id
                                LEFT JOIN
                                  lead_or_not AS lead
                                ON
                                  views.user_id = lead.user_id
                                LEFT JOIN
                                  timezone_corrected_timestamp AS tz
                                ON
                                  views.user_id = tz.user_id
                                LEFT JOIN
                                  visits_from_continent AS cont
                                ON
                                  views.user_id = cont.user_id
                                LEFT JOIN
                                  visits_from_os AS os
                                ON
                                  views.user_id = os.user_id
                                LEFT JOIN
                                  dwell_and_completion AS dwell
                                ON
                                  views.user_id = dwell.user_id
                                LEFT JOIN
                                  avearge_virality AS vir
                                ON
                                  views.user_id = vir.user_id
                                LEFT JOIN
                                  industry AS ind
                                ON
                                  views.user_id = ind.user_id
                                LEFT JOIN
                                  recency_and_frequency as rf
                                ON
                                  views.user_id = rf.user_id
                                LEFT JOIN
                                    geographic_info AS gg
                                ON
                                  views.user_id = gg.user_id
                                LEFT JOIN
                                    {self.pulldata_project}.{classifier_constants.JOB_TITLES_DATASET}.{classifier_constants.JOB_TITLES_TABLE} as job_titles
                                ON
                                  views.user_id = job_titles.permutive_id
                                """

        # Run
        job_config = bigquery.QueryJobConfig(
            destination=table_id, write_disposition="WRITE_APPEND"
        )
        print(f"Saving table at {table_id}.")

        features_df = (
            self.bqclient.query(features_query, job_config=job_config)
            .result()
            .to_dataframe(bqstorage_client=self.bqstorageclient)
        )

        if virality_table_id is not None:
            self.bqclient.delete_table(
                virality_table_id, not_found_ok=True
            )  # Make an API request.

            print(f"Deleted temp virality table {virality_table_id}.")

        return features_df

    def create_features_attributes_table(
        self,
        attribute: str,
        domain: str,
        end_date: str,
        lookback_period: int,
        table_id: str,
    ) -> pd.DataFrame:
        """
        Creates features and attributes table for each user

        Args:
            attribute (str): attribute
            domain (str): which domain
            end_date (str): data end date
            lookback_period (int): num of days to look back
            table_id (str): BQ table

        Returns:
            pd.DataFrame: The return value dataframe

        """

        if attribute == "industry":
            df = self.get_industry_data(domain, end_date, lookback_period, table_id)
            return df
        if attribute == "job_title":
            df = self.get_job_title_data(domain, end_date, lookback_period, table_id)
            return df

    def does_table_exist(self, table_id: str) -> bool:
        """
        Checks if given table exists in BigQuery

        Args:
            table_id (str): BQ table

        Returns:
            bool: The return value

        """
        try:
            self.bqclient.get_table(table_id)  # Make an API request.
            print("Table {} already exists.".format(table_id))
            return True
        except NotFound:
            print("Table {} is not found. Creating.".format(table_id))
            return False

    def upload_user_attribute_probabilities(
        self, df: pd.DataFrame, attribute: str
    ) -> None:
        """
        Uploads user attribute probabilities to BigQuery - the outputs from the classification model

        Args:
            df (pd.DataFrame): table to upload
            attribute (str): which attribute
        """

        # #TODO: decide what to do about this - we currently have an arbitrary number of rows cut-off
        if df.shape[0] > classifier_constants.UPLOAD_LIMIT:
            df = df.sample(classifier_constants.UPLOAD_LIMIT)

        table_id = f"Segmentation_v2.{attribute}_user_probabilities"

        # Schema list
        main_keys = [
            bigquery.SchemaField("timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("domain", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "lookback_period", bigquery.enums.SqlTypeNames.INTEGER
            ),
            bigquery.SchemaField("end_date", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("known", bigquery.enums.SqlTypeNames.INTEGER),
        ]

        extra_info_keys = [
            bigquery.SchemaField(i, classifier_constants.EXTRA_USER_INFO_TYPES[i])
            for i in classifier_constants.EXTRA_USER_INFO
        ]

        data_cols = [
            bigquery.SchemaField("attribute", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("probability", bigquery.enums.SqlTypeNames.FLOAT),
        ]

        full_schema = main_keys + extra_info_keys + data_cols

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=full_schema,
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_APPEND",
        )

        job = self.bqclient.load_table_from_dataframe(
            df, f"{self.pulldata_project}.{table_id}", job_config=job_config
        )  # Make an API request.
        job.result()
