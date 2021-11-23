import pandas as pd
import numpy as np
import pathlib
import time
from datetime import datetime, timedelta
from google.cloud import bigquery, bigquery_storage  # type: ignore
import segmentation.cluster_model.segmentation_constants as constants
from segmentation.config.load_config import load_config
import warnings


# Config
config = load_config()


class ClusterDownloader:
    """
    BLA BLA

    Args:

    Returns:
        bla

    """

    # initialize the class
    def __init__(self, model_type, data_version):
        self._model_type = model_type
        self._data_version = data_version
        self.wishlist_profile = None
        self.industry_data = None
        self.wishlist_industry_data = None
        self.jobs_data = None
        self.seniority_data = None
        (
            self.bqclient,
            self.bqstorageclient,
            self.pulldata_project,
            self.permutive_project,
        ) = self.get_clients()

    def get_clients(self):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """
        # set the permutive credentials
        pulldata_project = constants.PULLDATA_PROJECT
        permutive_project = constants.PERMUTIVE_PROJECT
        bqclient = bigquery.Client(project=pulldata_project)
        bqstorageclient = bigquery_storage.BigQueryReadClient()
        return bqclient, bqstorageclient, pulldata_project, permutive_project

    def generate_virality(
        self,
        full_list_of_domains,
        domains_case_query,
        end_date,
    ):
        virality_query = f"""
            # this cte gets the raw data that we will perform calculations on
            with get_raw_data as (
            SELECT *,
            from {self.permutive_project}.global_data.pageview_events
            # make sure we calculate virality AS OF the date we want to fit for
            WHERE DATE(_PARTITIONTIME) <= "{end_date}"
            and properties.client.user_agent not like '%bot%'
            and properties.client.user_agent not like '%Google Web Preview%'
            AND properties.client.domain in ({full_list_of_domains})
            and properties.client.title not in (
            {constants.ARTICLE_NAMES_TO_EXCLUDE}
            )
            ),
            views_per_article as (
            select
            properties.client.title,
            CASE
              {domains_case_query}
              ELSE properties.client.domain
            END as domain,
            count(distinct CASE WHEN properties.client.type = 'web' THEN view_id ELSE event_id END) as views_per_article
            FROM get_raw_data
            group by title, domain
            ),
            average_views_per_domain as (
            select
            CASE
              {domains_case_query}
              ELSE properties.client.domain
            END as domain_fixed,
            count(distinct CASE WHEN properties.client.type = 'web' THEN view_id ELSE event_id END) /
            count(distinct properties.client.title) as average_views_domain
            FROM get_raw_data
            group by domain_fixed
            ),
            virality as (
            select views.*, average_per_domain.*,
            views_per_article / average_views_domain as virality
            from views_per_article as views
            left join average_views_per_domain as average_per_domain
            on views.domain = average_per_domain.domain_fixed
            )
            select * from virality
            order by virality desc
            """
        # persist the table
        timestamp = round(time.time())
        # Set table_id to the ID of the destination table.
        virality_table_id = (
            f"{self.pulldata_project}.{constants.DATASET}.virality_temp_{timestamp}"
        )
        job_config = bigquery.QueryJobConfig(destination=virality_table_id)
        print(f"Saving temp virality table at {virality_table_id}.")
        # Start the query, passing in the extra configuration.
        query_job = self.bqclient.query(
            virality_query, job_config=job_config
        )  # Make an API request.
        query_job.result()  # Wait for the job to complete.
        return virality_table_id

    def get_features(
        self,
        domain,
        full_list_of_domains,
        end_date,
        lookback_period,
        virality_table_id,
    ):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """
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
            # from this cte one gets all the raw data that is then filtered and crunched to get the engineered features
            # almost all features start with this since none are aggregated per user by default
            with get_raw_data as (
            SELECT * from {self.permutive_project}.global_data.pageview_events
            # select the date range that you want to fit on later down the line
            WHERE DATE(_PARTITIONTIME) >= "{start_date}" and DATE(_PARTITIONTIME) <= "{end_date}"
            # exclude bots
            and properties.client.user_agent not like '%bot%'
            and properties.client.user_agent not like '%Google Web Preview%'
            # leave only those events that are for the publications we work for
            AND properties.client.domain in ({full_list_of_domains})),

            # this cte gets the number of sessions and number of views per user
            views_and_sessions as (
            SELECT user_id,
            # count the distinct sessions
            count(distinct session_id) as number_of_sessions,
            # count the distinct 'views (either view_id or event_id)
            count(distinct CASE WHEN properties.client.type = 'web' THEN view_id ELSE event_id END) as number_of_views
            # from the raw data
            FROM get_raw_data
            # and group by user
            group by user_id),

            # this cte gets the existing segments generated by permutive for each user - we don't group by user here,
            # as 'segments' columns is already user-wide
            # however, it is not time-agnostic - as user behavious changes with usage, the segments assigned to them change
            # we decide to get only the most recent segments for each user
            permutive_segments as (
            SELECT user_id, segments from (
            SELECT user_id, segments,
            # create windowing as rownumber over ordered partitions, leaving only the latest segments field
            row_number() over (partition by user_id order by time DESC) as rn
            FROM get_raw_data
            )
            # only the first, when ordered by time desc, means latest
            where rn = 1),

            # this cte gets the number of visits from the various referrer groups
            referrers as (
            select
            user_id,
            # social group
            sum(--Social
            CASE {social_ref_case_query} else 0 end) as visits_from_social,
            sum(--Search
            CASE {search_ref_case_query} else 0 end) as visits_from_search,
            sum(--Email
            CASE {email_ref_case_query} else 0 end) as visits_from_email,
            sum(--NSMG network
            CASE {nsmg_ref_case_query} else 0 end) as visits_from_NSMG_network,
            sum(--own website > redirect / linkclicks
            CASE {own_ref_case_query} else 0 end) as visits_from_own_website,
            # when the referrer is null, this means the url was directly fed to the browser
            sum(-- Typed-in URL
            CASE WHEN properties.client.referrer is NULL then 1 else 0 end) as visit_from_bookmark_or_url
            FROM get_raw_data
            group by user_id
            ),

            # this cte gets the number of visits from either mobile or web
            amp_or_web as (
            select
            user_id,
            sum(--Web
            CASE WHEN properties.client.type = 'web' THEN 1 ELSE 0 END) as visits_from_web,
            sum(--Mobile
            CASE WHEN properties.client.type = 'amp' THEN 1 ELSE 0 END) as visits_from_mobile
            FROM get_raw_data
            group by user_id
            ),

            # this cte returns a binary code with 1 if the user is a lead, and 0 otherwise
            lead_or_not as(
            select
            user_id,
            # we join with an existing and daily-updated table to get the result
            # if the join with the 'leads' table is successful for the user, they will get an 'earliest_event' value
            # if that value is not null, then the user is a lead
            case when earliest_event is null then 0 else 1 end as lead_or_not
            from(
            select * from views_and_sessions as users
            left join (select user_id as user_id_orig, earliest_event from {self.pulldata_project}.{constants.LEADS_DATASET_TABLE}) as orig_leads
            on users.user_id = orig_leads.user_id_orig
            )
            ),

            # this cte returns the average time of day during which the user opens a page
            timezone_corrected_timestamp as (
            select
            user_id,
            avg(
            # we use the continent of the user as a proxy for their location
            # no other options were easily attainable
            extract(hour from time at TIME ZONE CASE WHEN properties.geo_info.continent in ('North America') then 'US/Central'
            WHEN properties.geo_info.continent in ('South America') then 'Brazil/West'
            WHEN properties.geo_info.continent in ('Europe', 'Africa') then 'Europe/Berlin'
            WHEN properties.geo_info.continent in ('Asia') then 'Asia/Karachi'
            ELSE 'Etc/UTC'
            END)
            ) as avg_timestamp_for_continent
            FROM get_raw_data
            group by user_id
            ),

            # this cte returns the number of visits from the various continents - NOTE! Might want to exclude this re biases...
            visits_from_continent as (
            select
            user_id,
            sum(CASE WHEN properties.geo_info.continent in ('North America') THEN 1 ELSE 0 END) as visits_from_northamerica,
            sum(CASE WHEN properties.geo_info.continent in ('South America') THEN 1 ELSE 0 END) as visits_from_southamerica,
            sum(CASE WHEN properties.geo_info.continent in ('Europe') THEN 1 ELSE 0 END) as visits_from_europe,
            sum(CASE WHEN properties.geo_info.continent in ('Africa') THEN 1 ELSE 0 END) as visits_from_africa,
            sum(CASE WHEN properties.geo_info.continent in ('Asia') THEN 1 ELSE 0 END) as visits_from_asia,
            sum(CASE WHEN properties.geo_info.continent not in ('North America', 'South America', 'Europe', 'Africa', 'Asia')
            THEN 1 ELSE 0 END) as visits_from_othercontinent
            FROM get_raw_data
            group by user_id
            ),

            # this cte returns the number of visits from various OSs
            visits_from_os as (
            select
            user_id,
            sum(CASE WHEN properties.client.user_agent like '%{constants.LINUX_AGENT_STR}%' THEN 1 ELSE 0 END) as visits_from_linux,
            sum(CASE WHEN properties.client.user_agent like '%{constants.ANDROID_AGENT_STR}%' THEN 1 ELSE 0 END) as visits_from_android,
            sum(CASE WHEN properties.client.user_agent like '%{constants.WIN10_AGENT_STR}%' THEN 1 ELSE 0 END) as visits_from_windows10,
            sum(CASE WHEN properties.client.user_agent like '%{constants.WIN_AGENT_STR}%'
            and properties.client.user_agent not like '%{constants.WIN10_AGENT_STR}%' THEN 1 ELSE 0 END) as visits_from_windows_older,
            sum(CASE WHEN properties.client.user_agent like '%{constants.IPHONE_AGENT_STR}%' THEN 1 ELSE 0 END) as visits_from_iphone,
            sum(CASE WHEN properties.client.user_agent like '%{constants.MAC_AGENT_STR}%' THEN 1 ELSE 0 END) as visits_from_mac,
            sum(CASE WHEN properties.client.user_agent like '%{constants.APPLE_AGENT_STR}%' THEN 1 ELSE 0 END) as visits_from_apple_other,
            FROM get_raw_data
            group by user_id),

            dwell_and_completion as (
            select user_id,
            avg(properties.aggregations.PageviewEngagement.completion) as average_completion,
            avg(properties.aggregations.PageviewEngagement.engaged_time) as average_dwell
            from {self.permutive_project}.global_data.pageviewcomplete_events
            WHERE DATE(_PARTITIONTIME) >= "{start_date}" and DATE(_PARTITIONTIME) <= "{end_date}"
            # exclude instances where both of the metrics are = 0 - some bug?
            and properties.aggregations.PageviewEngagement.completion != 0
            and properties.aggregations.PageviewEngagement.engaged_time != 0
            # exclude bots
            and properties.client.user_agent not like '%bot%'
            and properties.client.user_agent not like '%Google Web Preview%'
            # leave only those events that are for the publications we work for
            AND properties.client.domain in ({full_list_of_domains})
            group by user_id
            ),

            # this cte returns the average virality of articles read by the user
            avearge_virality as (
            select user_id,
            avg(articles.virality) as average_virality
            from get_raw_data as users
            # we use a helper table, which contains each article in our domain, alongside its 'virality'
            # a page's virality is the ratio between 1) the total number of views for the page, and
            # 2) the average number of views for pages within the publication
            left join {virality_table_id} as articles
            on users.properties.client.title = articles.title
            group by user_id
            )

            # join all of the above for the final result
            # the resulting table is saved as a bigquery table, and then downloaded in python for fitting
            select vas.*,ref.*except(user_id),aow.*except(user_id),lon.*except(user_id),time.*except(user_id),
            seg.*except(user_id),con.*except(user_id),os.*except(user_id),dac.*except(user_id),vir.*except(user_id)
            from views_and_sessions as vas
            left join referrers as ref
            on vas.user_id = ref.user_id
            left join amp_or_web as aow
            on vas.user_id = aow.user_id
            left join lead_or_not as lon
            on vas.user_id = lon.user_id
            left join timezone_corrected_timestamp as time
            on vas.user_id = time.user_id
            left join permutive_segments as seg
            on vas.user_id = seg.user_id
            left join visits_from_continent as con
            on vas.user_id = con.user_id
            left join visits_from_os as os
            on vas.user_id = os.user_id
            left join dwell_and_completion as dac
            on vas.user_id = dac.user_id
            left join avearge_virality as vir
            on vas.user_id = vir.user_id

                    """
        features_df = (
            self.bqclient.query(features_query)
            .result()
            .to_dataframe(bqstorage_client=self.bqstorageclient)
        )
        return features_df

    def get_files(self, params, domains):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """

        # simulating running in a kubernetes pod, let's not expect to have the training data downloaded to the VM,
        # i.e., it would be faster to download the data to the pod's root than to read from the VM...
        # ---- BELOW IS WHILE ON VM ONLY?---- excl. above simulation...
        # check whether downloaded data already exists, and if not - download it...
        # start by getting the GBQ clients, and the project_id for pulldata (to upload via pandas)
        # extend the list of domains, add quotation marks and concatenate it into a string
        domains_quotations = [f"'{domain}', 'www.{domain}'" for domain in domains]
        full_list_of_domains = ", ".join(domains_quotations)

        # also create a list to merge the alternate domains' data with a case
        case_domains = [
            f"WHEN properties.client.domain = '{domain}' THEN 'www.{domain}'"
            for domain in domains
        ]
        domains_case_query = " ".join(case_domains)

        # If we're evaluating use end dates
        if self._model_type == "evaluation":
            dates_key = "evaluation_end_date"
        else:
            dates_key = "end_date"

        # NOTE: generate a virality table ONLY IF A FILE HAS NOT BEEN DOWNLOADED - in the 'else' below
        virality_table_id = None

        # next, start generating full features data files for each domain / lookback / end_date combination
        # create folders that will hold the downloaded data
        for i, (domain, domain_quotations, case_domain) in enumerate(
            zip(domains, domains_quotations, case_domains)
        ):

            if self._model_type == "prediction":  # Prediction
                domain_dir = f"{config['pred_dir']}/{domain}"
                pathlib.Path(domain_dir).mkdir(parents=True, exist_ok=True)
                data_filename = pathlib.Path(domain_dir + f"/{params[dates_key]}.csv")
                # lastly, if in inference, then 'lookback_period' will be supplied as a list corresponding to the list of domains
                lookback_period = params["lookback_period"][i]

            else:  # Grid search OR evaluation
                lookback_period = params["lookback_period"]
                domain_dir = f"{config['root_dir']}/training_data/v_{self._data_version}/{domain}/lookback_{lookback_period}"
                pathlib.Path(domain_dir).mkdir(parents=True, exist_ok=True)
                data_filename = pathlib.Path(domain_dir + f"/{params[dates_key]}.csv")

            if data_filename.is_file():
                print(
                    f"Using already downloaded data for {domain}, with lookback period of {lookback_period} as of {params[dates_key]}."
                )
            else:
                # ----- if we were to download (and aggregate) full data for users across
                # all domains (outside the for loop, for all domains together),
                # there will be cases like, one user has 30 visits on New Statesman, and 1 visit on
                # energymonitor, and the features for this user will show 31 visits total, and this will be sent for
                # training both energymonitor and NSMG models. We probably don't want that; we want to target that
                # user for his behavioural metrics on the single specific domains only
                # ~3% of users visit more than 1 domain
                # -----
                # generate the temp virality table and persist it onto bigquery
                # Only do this the first time a file is found to not be already downloaded... no - outside..
                # this table will be deleted at the end of this script
                # the method that generates it returns the name of the table (unique for each run, by timestamp)
                if virality_table_id is None:
                    virality_table_id = self.generate_virality(
                        full_list_of_domains, domains_case_query, params[dates_key]
                    )

                print(
                    f"Downloading data for {domain}, with lookback period of {lookback_period} as of {params[dates_key]}. "
                )
                # get the features data
                features_df = self.get_features(
                    domain,
                    domain_quotations,
                    params[dates_key],
                    lookback_period,
                    virality_table_id,
                )

                # persist the features df to disk (if we move to kubernetes - pods should be mounted on a data storage...)
                features_df.to_csv(data_filename)

        # delete the temp virality table - only if one has been created!
        if virality_table_id is not None:
            self.bqclient.delete_table(
                virality_table_id, not_found_ok=True
            )  # Make an API request.

            print(f"Deleted temp virality table {virality_table_id}.")

        return True

    def download_clearbit_data(self, end_date, lookback_period, domain):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """
        start_date = datetime.strftime(
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(int(lookback_period)),
            "%Y-%m-%d",
        )
        domains_str = f"'{domain}', 'www.{domain}'"
        clearbit_query = f"""
        SELECT * from (
        SELECT user_id,
        properties.company.name,
        properties.company.category.sector,
        properties.company.category.subIndustry,
        properties.company.category.industry,
        properties.company.category.industryGroup,
        # get only the MOST RECENT industry / company for a user
        ROW_NUMBER() OVER (partition by user_id ORDER BY time DESC) AS rn
        FROM {self.permutive_project}.global_data.clearbit_events
        WHERE DATE(_PARTITIONTIME) >= "{start_date}" and DATE(_PARTITIONTIME) <= "{end_date}"
        and properties.client.domain in ({domains_str})
        and properties.client.user_agent not like '%bot%'
        and properties.client.user_agent not like '%Google Web Preview%'
        ) where rn = 1
        """
        clearbit_df = (
            self.bqclient.query(clearbit_query)
            .result()
            .to_dataframe(bqstorage_client=self.bqstorageclient)
        )
        # let's persist the df as a class attribute to use later as needed
        self.clearbit_data = clearbit_df
        return clearbit_df

    def download_users_jobs(self, end_date, lookback_period, domain):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """

        jobs_query = f"""
        SELECT distinct permutive_id as user_id, job_title_11 as jobs
        FROM {self.pulldata_project}.{constants.DATASET}.{constants.USERS_JOBS_TABLE}
        """
        jobs_df = (
            self.bqclient.query(jobs_query)
            .result()
            .to_dataframe(bqstorage_client=self.bqstorageclient)
        )
        # let's persist the df as a class attribute to use later as needed
        self.jobs_data = jobs_df
        return jobs_df

    def download_users_seniority(self, end_date, lookback_period, domain):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """

        seniority_query = f"""
        SELECT distinct permutive_id as user_id, job_title_seniority as seniority
        FROM {self.pulldata_project}.{constants.DATASET}.{constants.USERS_JOBS_TABLE}
        """
        seniority_df = (
            self.bqclient.query(seniority_query)
            .result()
            .to_dataframe(bqstorage_client=self.bqstorageclient)
        )
        # let's persist the df as a class attribute to use later as needed
        self.seniority_data = seniority_df
        return seniority_df

    def profile_predicted_data(
        self, merged_df, end_date, domain, lookback_period, profiling_dimension
    ):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """
        # first thing's first - let's group by cluster and by the PROFILING_DIMENSION
        profile_df = (
            merged_df.groupby(["cluster", profiling_dimension], as_index=False)
            .agg("count")
            .iloc[:, :3]
        )
        profile_df.columns = [
            "cluster",
            profiling_dimension,
            f"{profiling_dimension}_per_cluster",
        ]
        # let's also extract the 'cluster_total' - the original data by cluster, and the 'domain_total'
        cluster_totals = merged_df.groupby("cluster").count()
        cluster_totals.reset_index(inplace=True)
        cluster_totals = cluster_totals.iloc[:, :2]
        cluster_totals.columns = ["cluster", "cluster_total"]
        # merge with the main dataframe
        profile_df = profile_df.merge(cluster_totals, how="left", on="cluster")
        # also calculate the 'domain_total' - same for every row
        profile_df["domain_total"] = merged_df.shape[0]
        # also calculate the 'industry_per_domain' column - in a separate group by
        industry_totals = merged_df.groupby(profiling_dimension).count()
        industry_totals.reset_index(inplace=True)
        industry_totals = industry_totals.iloc[:, :2]
        industry_totals.columns = [profiling_dimension, f"{profiling_dimension}_total"]
        # merge with the main df
        profile_df = profile_df.merge(
            industry_totals, how="left", on=profiling_dimension
        )
        # we need to branch out a little, and make sure we account for a scenario
        # where the dataframe is empty (no clearbit data...) - we need to add an empty row...
        if profile_df.shape[0] == 0:
            # this will add a null row to the profiles daily, and enable calcs below
            profile_df = profile_df.append(pd.Series(), ignore_index=True)
        # finally, we can calculate the proportions
        # proportion of cluster in industry (KNOWNS)
        profile_df[f"{profiling_dimension}Cluster_of_cluster"] = profile_df.apply(
            lambda x: x[f"{profiling_dimension}_per_cluster"] / x["cluster_total"],
            axis=1,
        )
        # proportion distribution of users in industry
        profile_df[
            f"{profiling_dimension}Cluster_of_{profiling_dimension}"
        ] = profile_df.apply(
            lambda x: x[f"{profiling_dimension}_per_cluster"]
            / x[f"{profiling_dimension}_total"],
            axis=1,
        )
        # distribution of clusters across whole domain
        profile_df["cluster_of_domain"] = profile_df.apply(
            lambda x: x["cluster_total"] / x["domain_total"], axis=1
        )
        # finally, the weight_ratio is calculated as industryCluster_of_industry / cluster_of_domain
        # in simple terms, the ratio is 1 when we see as many members of an industry as the size of the cluster would suggest
        # if there is nothing special about the cluster; when
        # when the weight ratio is higher than 1, it means that a cluster has a higher-than-expected proportion of users from
        # the respective industry
        profile_df["weight_ratio"] = profile_df.apply(
            lambda x: x[f"{profiling_dimension}Cluster_of_{profiling_dimension}"]
            / x["cluster_of_domain"],
            axis=1,
        )
        # pandas bug, make sure after merge cluster is integer... nans are special case
        if not profile_df.isnull().values.all():
            profile_df.cluster = profile_df.cluster.astype(int)
        # add the date as a timestamp and domain
        profile_df["date"] = datetime.strptime(end_date, "%Y-%m-%d")
        profile_df["domain"] = domain
        profile_df["lookback_period"] = lookback_period
        return profile_df

    def get_wishlist_companies(self, wishlist_table):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """
        # wishlist_industries_filename = pathlib.Path(f"{config['pred_dir']}/wishlist_companies.csv")
        # todo: persist the table? very small - 1-2MB, not really important...
        wishlist = bigquery.TableReference.from_string(
            f"{self.pulldata_project}.{constants.DATASET}.{wishlist_table}"
        )
        rows = self.bqclient.list_rows(wishlist)
        wishlist_df = rows.to_dataframe(bqstorage_client=self.bqstorageclient)
        return wishlist_df

    def mask_wishlist_companies(self, clearbit_data, wishlist_data):
        if pd.notnull(wishlist_data):
            response = clearbit_data
        else:
            response = None
        return response

    def generate_wishlist_profile(self, merged_df, end_date, domain, lookback_period):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """
        # first, let's get the merged df - containing predictions and clearbit data
        # and map the clearbit data to the wishlist
        # to do that, we need to first download the wishlist and mapping tables
        # hence, we compare to the NS_WISHLIST_DOMAINS_GROUPS constant - if the
        # domain is one of the keys, the domain is from the NSMG network, otherwise it's gd
        if domain in list(constants.NS_WISHLIST_DOMAINS_GROUPS.keys()):
            domain_group = "ns_media"
        else:
            domain_group = "global_data"
        # find the corresponding wishlist table
        wishlist_table = constants.WISHLIST_TABLES[domain_group][0]
        wishlist_df = self.get_wishlist_companies(wishlist_table)
        # we need to merge the mapping with the merged_df
        merge_col = constants.WISHLIST_TABLES[domain_group][2]
        # as there are duplicates in company names, delete duplicates
        # TODO: uniform tables...
        # since the GD table contains duplicates, we need to clean those
        if domain_group == "global_data":
            wishlist_df = wishlist_df[merge_col].drop_duplicates()
        # meanwhile, for NSMG we have separate lists for newstatesman and the other domains
        else:
            wishlist_df = wishlist_df.drop_duplicates()
            domain_category = constants.NS_WISHLIST_DOMAINS_GROUPS[domain][0]
            filter_col = constants.WISHLIST_TABLES[domain_group][1]
            wishlist_df = wishlist_df[wishlist_df[filter_col] == domain_category]
        # merge the predictions with the wishlist dataframe
        preds_cb_wishlist_df = merged_df.merge(
            wishlist_df, how="left", left_on="name", right_on=merge_col
        )
        # since we don't have an mapped industry (per company) for the wishlist of companies
        # to aggregate by, let's use the existing clearbit mapping, but rename the column
        profiling_col = constants.WISHLIST_TABLES[domain_group][3]
        # wishlist_df.rename(columns={profiling_col: f"wishlist_{profiling_col}"}, inplace=True)
        profiling_dimension = "wishlist_industry"
        preds_cb_wishlist_df[profiling_dimension] = preds_cb_wishlist_df.apply(
            lambda x: self.mask_wishlist_companies(x[profiling_col], x[merge_col]),
            axis=1,
        )
        # let's save the data to a class attribute for later access
        self.wishlist_industry_data = preds_cb_wishlist_df
        # finally, instruct the profiling function on the column to aggregate on
        wishlist_profile_df = self.profile_predicted_data(
            preds_cb_wishlist_df, end_date, domain, lookback_period, profiling_dimension
        )
        return wishlist_profile_df

    def download_c_level_data(self, dates_and_predictions, domain):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """
        # this method is being used by evaluator during evaluation of grid, and by daily segmentation & profiling script / dag
        # let's first download the data we need by selecting all clearbit events from GBQ with the respective date and domain
        # the first parameter - dates_and_predictions, contains: 1) [end_date, lookback_period, predictions_df, dir/'daily_profiling']
        end_date = dates_and_predictions[0]
        lookback_period = dates_and_predictions[1]
        seniority_df = self.download_users_seniority(end_date, lookback_period, domain)

        if seniority_df.empty:
            warnings.warn("No jobs data available")

        # let's merge the predicted clusters onto the jobs data
        pred_df = dates_and_predictions[2]

        # since there are some users for whom there are no jobs events, let's left join onto the predictions
        merged_df = pd.merge(
            pred_df, seniority_df, how="left", left_on=["user_id"], right_on=["user_id"]
        )

        # now, let's calculate the jobs_profiles
        # TODO: _model_type here holds the dimension on which we will profile the data
        score = self.extract_c_level_seg_score(merged_df)
        score_df = pd.DataFrame({"C_level_score": score}, index=[0])

        return score_df

    def extract_c_level_seg_score(self, merged_df):
        # make sure users without job titles are present in downstream calculation by filling nans
        merged_df = merged_df.fillna("non-declared")
        # calculate C-level projection only when there is at least one c-level user in the domain
        seniority_distinct = merged_df["seniority"].unique()
        if "C-level-1" in seniority_distinct:
            seniority_group = (
                merged_df.groupby(["cluster", "seniority"])
                .count()
                .unstack(fill_value=0)
                .stack()
            )
            ceos = seniority_group.xs("C-level-1", level="seniority")[
                "user_id"
            ].tolist()
            non_declared = seniority_group.xs("non-declared", level="seniority")[
                "user_id"
            ].tolist()
            # first identify the ceo cluster and the raw proportion of CEOs in that
            ceo_cluster = np.argmax(ceos)
            proportion_ceos = ceos[ceo_cluster] / np.sum(ceos)
            # notably - the non_declared should the propor
            # TODO think about non-declared proportion calculation
            proportion_nondeclared = non_declared[ceo_cluster] / np.sum(non_declared)
            # regularize the proportion
            regularised_ceos_prop = self.regularised_ceos_proportion(proportion_ceos)
            regularised_nondeclared_prop = self.regularised_nondeclared_proportion(
                proportion_nondeclared
            )
            # calculate the metric
            c_level_projection = self.harmonic_mean(
                regularised_ceos_prop, regularised_nondeclared_prop
            )
        else:
            c_level_projection = 0
        return c_level_projection

    # regularise the proportion to the deviation > making sure we reward models where a single cluster has
    # more significant concentration of CEOs by squaring or cubing the original proportion
    def regularised_ceos_proportion(self, original_prop):
        # the tunable exponent will allow us to change sharpness of function
        exponent = 2
        return original_prop ** exponent

    def regularised_nondeclared_proportion(self, original_prop):
        if original_prop != 0:
            # first we raise the original proportion, which is less than 1
            # to a power less than 1, to get a logarithmic curve
            exponent_log = 0.2  # must be less than 1
            # round to this number...
            alpha = 0.001
            original_prop = round(original_prop, len(str(alpha).split(".")[-1]) + 1)
            log_func = (original_prop - alpha) ** exponent_log
            # then subtract the exponentiation function
            exponent_exp = -3  # must be negative
            # finally, let's centre a bit to the smaller percentages
            centre_que = 0.05
            exponent_func = 1.0 / (
                1
                + centre_que
                * ((original_prop) / (1 - original_prop + alpha)) ** exponent_exp
            )
            regularised_prop = log_func - exponent_func
            # finally, return the greater of: 1) the product of or the function, and 2) 0
            rectified_regularised_prop = max([regularised_prop, 0])
        else:
            rectified_regularised_prop = 0
        return rectified_regularised_prop

    def harmonic_mean(self, prop_ceos, prop_nondeclared):
        harmonic_mean = (
            2 * (prop_ceos * prop_nondeclared) / (prop_ceos + prop_nondeclared)
        )
        return harmonic_mean

    def download_industry_data(self, dates_and_predictions, domain):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """
        # this method is being used by evaluator during evaluation of grid, and by daily segmentation & profiling script / dag
        # let's first download the data we need by selecting all clearbit events from GBQ with the respective date and domain
        # the first parameter - dates_and_predictions, contains: 1) [end_date, lookback_period, predictions_df, dir/'daily_profiling']
        end_date = dates_and_predictions[0]
        lookback_period = dates_and_predictions[1]
        clearbit_df = self.download_clearbit_data(end_date, lookback_period, domain)

        if clearbit_df.empty:
            warnings.warn("No industry data available")

        # let's merge the predicted clusters onto the clearbit data
        pred_df = dates_and_predictions[2]

        # since there are some users for whom there are no clearbit events, let's left join onto the predictions
        merged_df = pd.merge(
            pred_df, clearbit_df, how="left", left_on=["user_id"], right_on=["user_id"]
        )
        # set the merged df as a class attribute to be accessed later
        self.industry_data = merged_df
        # now, let's calculate the industry_profiles
        # TODO: _model_type here holds the dimension on which we will profile the data
        profile_df = self.profile_predicted_data(
            merged_df, end_date, domain, lookback_period, self._model_type
        )
        # generate wishlist profile as well
        # TODO: Each instance of the class is used once, but still bad as attribute...
        self.wishlist_profile = self.generate_wishlist_profile(
            merged_df, end_date, domain, lookback_period
        )
        return profile_df

    def download_jobs_data(self, dates_and_predictions, domain):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """
        # this method is being used by evaluator during evaluation of grid, and by daily segmentation & profiling script / dag
        # let's first download the data we need by selecting all clearbit events from GBQ with the respective date and domain
        # the first parameter - dates_and_predictions, contains: 1) [end_date, lookback_period, predictions_df, dir/'daily_profiling']
        end_date = dates_and_predictions[0]
        lookback_period = dates_and_predictions[1]
        jobs_df = self.download_users_jobs(end_date, lookback_period, domain)

        if jobs_df.empty:
            warnings.warn("No jobs data available")

        # let's merge the predicted clusters onto the jobs data
        pred_df = dates_and_predictions[2]

        # since there are some users for whom there are no jobs events, let's left join onto the predictions
        merged_df = pd.merge(
            pred_df, jobs_df, how="left", left_on=["user_id"], right_on=["user_id"]
        )
        # set the merged df as a class attribute to be accessed later
        self.jobs_data = merged_df

        # now, let's calculate the jobs_profiles
        # TODO: _model_type here holds the dimension on which we will profile the data
        profile_df = self.profile_predicted_data(
            merged_df, end_date, domain, lookback_period, self._model_type
        )

        return profile_df

    def download_seniority_data(self, dates_and_predictions, domain):
        """
        BLA BLA

        Args:

        Returns:
            bla

        """
        # this method is being used by evaluator during evaluation of grid, and by daily segmentation & profiling script / dag
        # let's first download the data we need by selecting all clearbit events from GBQ with the respective date and domain
        # the first parameter - dates_and_predictions, contains: 1) [end_date, lookback_period, predictions_df, dir/'daily_profiling']
        end_date = dates_and_predictions[0]
        lookback_period = dates_and_predictions[1]
        seniority_df = self.download_users_seniority(end_date, lookback_period, domain)

        if seniority_df.empty:
            warnings.warn("No jobs data available")

        # let's merge the predicted clusters onto the jobs data
        pred_df = dates_and_predictions[2]

        # since there are some users for whom there are no jobs events, let's left join onto the predictions
        merged_df = pd.merge(
            pred_df, seniority_df, how="left", left_on=["user_id"], right_on=["user_id"]
        )
        # set the merged df as a class attribute to be accessed later
        self.seniority_data = merged_df

        # now, let's calculate the jobs_profiles
        # TODO: _model_type here holds the dimension on which we will profile the data
        profile_df = self.profile_predicted_data(
            merged_df, end_date, domain, lookback_period, self._model_type
        )

        return profile_df
