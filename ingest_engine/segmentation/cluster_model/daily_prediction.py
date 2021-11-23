from google.cloud import storage, bigquery  # type: ignore
import pathlib
from datetime import datetime, timedelta
import pandas as pd
import os
import random
import shutil
from pickle import load
import segmentation.cluster_model.segmentation_constants as constants
from segmentation.downloader.cluster_model_downloader import ClusterDownloader
from segmentation.config.load_config import load_config


# Config
config = load_config()


class Predictor:

    # initialize the class
    def __init__(self, data_version):
        self._pred_dir = config["pred_dir"]
        self._project_id = constants.PULLDATA_PROJECT
        self._bucket = constants.STORAGE_BUCKET
        self._data_version = data_version
        self.combined_latest_pred = None
        self.industry_distributions = {}
        self.jobs_distributions = {}
        self.scv_predictions_list = []

    def upload_to_storage(self, domain, file):
        client = storage.Client(project=self._project_id)
        curr_bucket = client.bucket(self._bucket)
        blob = curr_bucket.blob(f"{domain}/latest_pred.csv")
        with open(file, "rb") as my_file:
            blob.upload_from_file(my_file)
        return True

    def download_existing_clusters(self, end_date, lookback_periods_all, gcp_clients):
        # since the lookbacks can differ, let's take the biggest one and use that
        # as the period to download the full data for
        biggest_lookback = max(lookback_periods_all)
        # first, get the clients to work with gbq
        bqclient, bqstorageclient, pulldata_project, permutive_project = gcp_clients
        # then, download the data for only within the (biggest) lookback period
        start_date = datetime.strftime(
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(int(biggest_lookback)),
            "%Y-%m-%d",
        )
        # write the query to be executed
        # we partition by user id and order by time to get only the latest segments for each user
        existing_clusters_query = f"""
        SELECT * except(rn) FROM (
            SELECT uc.user_id ,uc.cluster ,uc.cluster_change_date, uc.domain, pe.last_activity_date,
            row_number() over (partition by uc.user_id, uc.domain order by uc.cluster_change_date DESC) as rn
            FROM {constants.PULLDATA_PROJECT}.{constants.DATASET}.{constants.USERS_CLUSTERS_TABLE} as uc
            left join
            (select * except(rn)
            from
            (select user_id, time as last_activity_date, 
            REGEXP_REPLACE(properties.client.domain, '^www\\\\.','') as domain,
            row_number() over (partition by user_id, properties.client.domain order by time DESC) as rn
            from {constants.PERMUTIVE_PROJECT}.{constants.PAGEVIEWS_TABLE}
            WHERE date(_PARTITIONTIME) >= "{start_date}" and date(_PARTITIONTIME) <= "{end_date}")
            where rn=1)
            as pe
            on uc.user_id = pe.user_id  and uc.domain = pe.domain
            WHERE DATE(uc.last_activity_date) >= "{start_date}"  and DATE(uc.last_activity_date) <= "{end_date}"
        ) where rn = 1 
        """
        # existing_clusters_query = f"""
        #     SELECT * except(rn) FROM (
        #     SELECT *,
        #     row_number() over (partition by user_id, domain order by date DESC) as rn
        #     FROM {constants.PULLDATA_PROJECT}.{constants.DATASET}.{constants.USERS_CLUSTERS_TABLE}
        #     WHERE DATE(date) >= "{start_date}" and DATE(date) <= "{end_date}"
        #     ) where rn = 1
        #     """

        existing_df = (
            bqclient.query(existing_clusters_query)
            .result()
            .to_dataframe(bqstorage_client=bqstorageclient)
        )
        # finally, write the existing clusters as a csv to disk
        existing_df.to_csv(f"{self._pred_dir}/existing_clusters.csv")
        return True

    def preprocess_for_prediction(self, df, domain_dir):
        # load the imputer and impute the df
        imputer = load(open(f"{domain_dir}/imputer.pkl", "rb"))
        imputed_array = imputer.transform(df)
        imputed_df = pd.DataFrame(
            data=imputed_array, index=df.index, columns=df.columns
        )
        # load the selector (IF ANY) and select only the respective features
        selection_filename_list = [
            item for item in os.listdir(domain_dir) if "selection_for_" in item
        ]
        if len(selection_filename_list) > 0:
            assert len(selection_filename_list) == 1
            selection_filename = selection_filename_list[0]
            cols_to_keep = pd.read_csv(f"{domain_dir}/{selection_filename}")[
                "feature"
            ].tolist()
            selected_df = imputed_df[sorted(cols_to_keep)]
        else:
            selected_df = imputed_df
        # order the columns
        selected_df = selected_df.reindex(sorted(selected_df.columns), axis=1)
        # now, load the scaler and scale the data
        scaler = load(open(f"{domain_dir}/scaler.pkl", "rb"))
        scaled_array = scaler.transform(selected_df)
        scaled_df = pd.DataFrame(
            data=scaled_array, index=selected_df.index, columns=selected_df.columns
        )
        return scaled_df

    def predict_clusters(self, df, domain_dir):
        # load the model and predict
        model_filename_list = [
            item for item in os.listdir(domain_dir) if "clusters.sav" in item
        ]
        assert len(model_filename_list) == 1
        model_filename = model_filename_list[0]
        kmeans = load(open(f"{domain_dir}/{model_filename}", "rb"))
        predicted_clusters = kmeans.predict(df)
        predicted_df = df.copy()
        # add the 'cluster' as a column
        predicted_df["cluster"] = predicted_clusters
        # make sure those are integers
        predicted_df.cluster = predicted_df.cluster.astype(int)
        # and reset the index - moving the user_id to a column
        predicted_df.reset_index(inplace=True)
        predicted_df_clean = predicted_df.loc[:, ["user_id", "cluster"]]
        return predicted_df_clean

    def upload_to_gbq(
        self, predictions_df, domain, end_date, lookback_period, gcp_clients
    ):
        bqclient, bqstorageclient, pulldata_project, permutive_project = gcp_clients
        # to upload only the rows that are changed from last time
        # we need to find the rows in the new predictions that do not exist in the existing predictions (excl. the 'date')
        # start by reading the existing clusters
        existing_df = pd.read_csv(
            f"{self._pred_dir}/existing_clusters.csv", index_col=0
        )
        # select only the data that corresponds to the current domain and lookback period
        existing_df = existing_df[existing_df["domain"] == domain]
        existing_df["last_activity_date"] = pd.to_datetime(existing_df["last_activity_date"])
        start_date = datetime.strftime(
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(int(lookback_period)),
            "%Y-%m-%d",
        )
        # TODO maybe redundant: query already filters
        existing_df = existing_df[existing_df["last_activity_date"] >= start_date]
        # to do the comparison, let's first cut the date and domain columns,
        # so that we're left with rows to strictly compare
        existing_df = existing_df.drop(["cluster_change_date", "domain"], axis=1)
        # finally, let's get the rows that are present in the new predictions and not present in the existing ones
        existing_df.columns = ["user_id", "cluster_existing", "last_activity_date"]
        new_df = predictions_df.merge(
            existing_df, how="left", left_on="user_id", right_on="user_id"
        )
        new_rows = new_df[new_df["cluster"] != new_df["cluster_existing"]]
        # finally, let's drop the 'old' cluster col which we used only for comparison
        new_rows = new_rows.drop(["cluster_existing"], axis=1)
        # there is a bug in pandas where after a merge ints are turned to floats.. let's reverse that
        new_rows.cluster = new_rows.cluster.astype(int)
        # let's add the date again, and upload to bigquery
        new_rows["cluster_change_date"] = datetime.strptime(end_date, "%Y-%m-%d")
        new_rows["last_activity_date"] = new_rows["last_activity_date"].apply(
            lambda x: datetime.strptime(end_date, "%Y-%m-%d") if pd.isna(x) else x)
        new_rows["domain"] = domain
        print(f"{new_rows.shape[0]} rows changed since last run for {domain}.")
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=[
                bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("cluster", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("cluster_change_date", bigquery.enums.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("domain", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("last_activity_date", bigquery.enums.SqlTypeNames.TIMESTAMP),

            ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_APPEND",
        )
        job = bqclient.load_table_from_dataframe(
            new_rows,
            f"{self._project_id}.{constants.DATASET}.{constants.USERS_CLUSTERS_TABLE}",
            job_config=job_config,
        )  # Make an API request.
        job.result()
        print("Uploaded to GBQ.")
        return True

    def upload_scv_predictions(self, predictions_df, gcp_clients):
        bqclient, bqstorageclient, pulldata_project, permutive_project = gcp_clients

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            # schema=[
            #     bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.STRING),
            #     bigquery.SchemaField("cluster", bigquery.enums.SqlTypeNames.INTEGER),
            #     bigquery.SchemaField("industry", bigquery.enums.SqlTypeNames.STRING),
            #     bigquery.SchemaField("wishlist_industry", bigquery.enums.SqlTypeNames.STRING),
            #     bigquery.SchemaField("jobs", bigquery.enums.SqlTypeNames.STRING),
            #     bigquery.SchemaField("seniority", bigquery.enums.SqlTypeNames.STRING),
            #     bigquery.SchemaField("domain", bigquery.enums.SqlTypeNames.STRING),
            #     bigquery.SchemaField("predicted_industry", bigquery.enums.SqlTypeNames.BOOLEAN),
            #     bigquery.SchemaField("predicted_jobs", bigquery.enums.SqlTypeNames.BOOLEAN),
            #
            #
            # ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_TRUNCATE",
        )
        job = bqclient.load_table_from_dataframe(
            predictions_df,
            f"{self._project_id}.{constants.DATASET}.industry_jobs_predictions",
            job_config=job_config,
        )  # Make an API request.
        job.result()
        print("Uploaded to GBQ.")
        return True

    def get_behavioural_profile(
        self, df, predictions_df, domain, lookback_period, end_date
    ):
        # we need the features data (but only the LIST_OF_MUSTHAVE_FEATURES
        # otherwise domains will have different dimensions)
        # merged with the predictions and grouped by industry and cluster
        features_df = df[constants.LIST_OF_MUSTHAVE_FEATURES]
        joined_df = features_df.merge(predictions_df, how="left", on="user_id")
        aggregated_df_mean = joined_df.groupby("cluster").mean()
        aggregated_df_count = joined_df.groupby("cluster").count().iloc[:, 1]
        aggregated_df_count.rename("count", inplace=True)
        aggregated_df = pd.DataFrame(aggregated_df_count).merge(
            aggregated_df_mean, how="left", on="cluster"
        )
        # let's add the identification columns
        aggregated_df["date"] = datetime.strptime(end_date, "%Y-%m-%d")
        aggregated_df["domain"] = domain
        aggregated_df["lookback_period"] = lookback_period
        # and reset the index - moving the cluster to a column
        aggregated_df.reset_index(inplace=True)
        return aggregated_df

    def set_behavioural_profile(
        self, df, predictions_df, domain, lookback_period, end_date, gcp_clients
    ):
        # let's start with the behavioural one
        bqclient, bqstorageclient, pulldata_project, permutive_project = gcp_clients
        behavioural_profile = self.get_behavioural_profile(
            df, predictions_df, domain, lookback_period, end_date
        )
        if self.check_profile_upload("behaviour", domain, end_date, gcp_clients):
            # let's upload the profiles to bigquery
            print(f"Appending {domain} behavioural profile data to GBQ table.")
            bp_job_config = bigquery.LoadJobConfig(
                # Specify a (partial) schema. All columns are always written to the
                # table. The schema is used to assist in data type definitions.
                schema=[
                    bigquery.SchemaField(
                        "cluster", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                    bigquery.SchemaField("count", bigquery.enums.SqlTypeNames.INTEGER),
                    bigquery.SchemaField(
                        "number_of_sessions", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "number_of_views", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "visits_from_social", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "visits_from_search", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "visits_from_email", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "visits_from_own_website", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "lead_or_not", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "avg_timestamp_for_continent", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "average_completion", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "average_dwell", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "average_virality", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.TIMESTAMP),
                    bigquery.SchemaField("domain", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "lookback_period", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                ],
                # Optionally, set the write disposition. BigQuery appends loaded rows
                # to an existing table by default, but with WRITE_TRUNCATE write
                # disposition it replaces the table with the loaded data.
                write_disposition="WRITE_APPEND",
            )
            bp_job = bqclient.load_table_from_dataframe(
                behavioural_profile,
                f"{self._project_id}.{constants.DATASET}.{constants.BEHAVIOURAL_PROFILES_TABLE}",
                job_config=bp_job_config,
            )  # Make an API request.
            bp_job.result()
        else:
            print(
                f"Behavioural profile for {domain} for {end_date} already exists in BigQuery. Skipping upload."
            )

    def set_profile(
        self,
        date_and_predictions,
        domain,
        downloader,
        gcp_clients,
        model_type,
        table_in_gbq,
    ):
        bqclient, bqstorageclient, pulldata_project, permutive_project = gcp_clients
        profile = downloader(date_and_predictions, domain)
        end_date = date_and_predictions[0]
        # we should first see if we need to upload a profile (or if one has been
        # already uploaded today) - makes safe re-triggers of the task
        if self.check_profile_upload(model_type, domain, end_date, gcp_clients):
            # let's upload the profiles to bigquery
            print(f"Appending {domain} {model_type} profile data to GBQ table.")
            ip_job_config = bigquery.LoadJobConfig(
                # Specify a (partial) schema. All columns are always written to the
                # table. The schema is used to assist in data type definitions.
                schema=[
                    bigquery.SchemaField(
                        "cluster", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                    bigquery.SchemaField(
                        model_type, bigquery.enums.SqlTypeNames.STRING
                    ),
                    bigquery.SchemaField(
                        f"{model_type}_per_cluster", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                    bigquery.SchemaField(
                        "cluster_total", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                    bigquery.SchemaField(
                        "domain_total", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                    bigquery.SchemaField(
                        f"{model_type}_total", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                    bigquery.SchemaField(
                        f"{model_type}Cluster_of_cluster",
                        bigquery.enums.SqlTypeNames.FLOAT,
                    ),
                    bigquery.SchemaField(
                        f"{model_type}Cluster_of_{model_type}",
                        bigquery.enums.SqlTypeNames.FLOAT,
                    ),
                    bigquery.SchemaField(
                        "cluster_of_domain", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "weight_ratio", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.TIMESTAMP),
                    bigquery.SchemaField("domain", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "lookback_period", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                ],
                # Optionally, set the write disposition. BigQuery appends loaded rows
                # to an existing table by default, but with WRITE_TRUNCATE write
                # disposition it replaces the table with the loaded data.
                write_disposition="WRITE_APPEND",
            )
            ip_job = bqclient.load_table_from_dataframe(
                profile,
                f"{self._project_id}.{constants.DATASET}.{table_in_gbq}",
                job_config=ip_job_config,
            )  # Make an API request.
            ip_job.result()
        else:
            print(
                f"{model_type} profile for {domain} for {end_date} already exists in BigQuery. Skipping upload."
            )

    def set_wishlist_industry_profile(
        self, domain, industry_downloader, end_date, gcp_clients
    ):
        bqclient, bqstorageclient, pulldata_project, permutive_project = gcp_clients
        # we've generated and persisted the wishlist profile in the downloader class
        # TODO: think of a better way to handle this... instance is unique, but still..
        wishlist_industry_profile = industry_downloader.wishlist_profile
        if self.check_profile_upload(
            "wishlist_industry", domain, end_date, gcp_clients
        ):
            # let's upload the profiles to bigquery
            print(f"Appending {domain} wishlist industry profile data to GBQ table.")
            wip_job_config = bigquery.LoadJobConfig(
                # Specify a (partial) schema. All columns are always written to the
                # table. The schema is used to assist in data type definitions.
                schema=[
                    bigquery.SchemaField(
                        "cluster", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                    bigquery.SchemaField(
                        "wishlist_industry", bigquery.enums.SqlTypeNames.STRING
                    ),
                    bigquery.SchemaField(
                        "wishlist_industry_per_cluster",
                        bigquery.enums.SqlTypeNames.INTEGER,
                    ),
                    bigquery.SchemaField(
                        "cluster_total", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                    bigquery.SchemaField(
                        "domain_total", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                    bigquery.SchemaField(
                        "wishlist_industry_total", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                    bigquery.SchemaField(
                        "wishlist_industryCluster_of_cluster",
                        bigquery.enums.SqlTypeNames.FLOAT,
                    ),
                    bigquery.SchemaField(
                        "wishlist_industryCluster_of_wishlist_industry",
                        bigquery.enums.SqlTypeNames.FLOAT,
                    ),
                    bigquery.SchemaField(
                        "cluster_of_domain", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField(
                        "weight_ratio", bigquery.enums.SqlTypeNames.FLOAT
                    ),
                    bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.TIMESTAMP),
                    bigquery.SchemaField("domain", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField(
                        "lookback_period", bigquery.enums.SqlTypeNames.INTEGER
                    ),
                ],
                # Optionally, set the write disposition. BigQuery appends loaded rows
                # to an existing table by default, but with WRITE_TRUNCATE write
                # disposition it replaces the table with the loaded data.
                write_disposition="WRITE_APPEND",
            )
            wip_job = bqclient.load_table_from_dataframe(
                wishlist_industry_profile,
                f"{self._project_id}.{constants.DATASET}.{constants.WISHLIST_INDUSTRY_PROFILES_TABLE}",
                job_config=wip_job_config,
            )  # Make an API request.
            wip_job.result()
        else:
            print(
                f"Wishlist industry profile for {domain} for {end_date} already exists in BigQuery. Skipping upload."
            )

    def combine_profile_attributes(
        self, industry_downloader, jobs_downloader, seniority_downloader
    ):
        # first, let's get all the dataframes
        industry_merged_df = industry_downloader.industry_data[
            ["user_id", "cluster", "industry"]
        ]
        wishlist_industry_merged_df = industry_downloader.wishlist_industry_data[
            ["user_id", "wishlist_industry"]
        ]
        jobs_merged_df = jobs_downloader.jobs_data[["user_id", "jobs"]]
        seniority_merged_df = seniority_downloader.seniority_data[
            ["user_id", "seniority"]
        ]
        # merge them together
        combined_df = industry_merged_df.merge(
            wishlist_industry_merged_df,
            how="left",
            left_on="user_id",
            right_on="user_id",
        )
        combined_df = combined_df.merge(
            jobs_merged_df, how="left", left_on="user_id", right_on="user_id"
        )
        combined_df = combined_df.merge(
            seniority_merged_df, how="left", left_on="user_id", right_on="user_id"
        )
        return combined_df

    def check_profile_upload(self, profile_type, domain, end_date, gcp_clients):
        if profile_type == "behaviour":
            table_to_check = constants.BEHAVIOURAL_PROFILES_TABLE
        elif profile_type == "industry":
            table_to_check = constants.INDUSTRY_PROFILES_TABLE
        elif profile_type == "wishlist_industry":
            table_to_check = constants.WISHLIST_INDUSTRY_PROFILES_TABLE
        elif profile_type == "jobs":
            table_to_check = constants.JOBS_PROFILES_TABLE
        elif profile_type == "seniority":
            table_to_check = constants.SENIORITY_PROFILES_TABLE
        bqclient, bqstorageclient, pulldata_project, permutive_project = gcp_clients
        profile_check_query = f"""
        SELECT * FROM {pulldata_project}.{constants.DATASET}.{table_to_check}
        WHERE domain = "{domain}"
        and date = "{end_date}"
        """
        check_df = (
            bqclient.query(profile_check_query)
            .result()
            .to_dataframe(bqstorage_client=bqstorageclient)
        )
        # return true if there are NO records in the profile for the day
        return check_df.shape[0] == 0

    def profile_clusters(
        self, df, predictions_df, domain, lookback_period, end_date, gcp_clients
    ):
        # we must produce 2 profiles - behavioural and industry (for now...)
        self.set_behavioural_profile(
            df, predictions_df, domain, lookback_period, end_date, gcp_clients
        )
        # now, let's get the industry profiles - we should use the method in the data_downloader module
        # the model_type here is also the dimension we use for profiling...
        model_type = "industry"
        industry_downloader = ClusterDownloader(model_type, self._data_version)
        # we need to supply the last parameter as 'daily_profiling' to indicate to the evaluator
        # that we're in inference mode
        date_and_predictions = [end_date, lookback_period, predictions_df]
        self.set_profile(
            date_and_predictions,
            domain,
            industry_downloader.download_industry_data,
            gcp_clients,
            model_type,
            constants.INDUSTRY_PROFILES_TABLE,
        )
        # finally, let's set the wishlist industry profile
        self.set_wishlist_industry_profile(
            domain, industry_downloader, end_date, gcp_clients
        )
        # now, let's get the jobs profiles - we should use the method in the data_downloader module
        # the model_type here is also the dimension we use for profiling...
        model_type = "jobs"
        jobs_downloader = ClusterDownloader(model_type, self._data_version)
        # we need to supply the last parameter as 'daily_profiling' to indicate to the evaluator
        # that we're in inference mode
        self.set_profile(
            date_and_predictions,
            domain,
            jobs_downloader.download_jobs_data,
            gcp_clients,
            model_type,
            constants.JOBS_PROFILES_TABLE,
        )
        # now, let's get the seniority profiles - we should use the method in the data_downloader module
        # the model_type here is also the dimension we use for profiling...
        model_type = "seniority"
        seniority_downloader = ClusterDownloader(model_type, self._data_version)
        # we need to supply the last parameter as 'daily_profiling' to indicate to the evaluator
        # that we're in inference mode
        self.set_profile(
            date_and_predictions,
            domain,
            seniority_downloader.download_seniority_data,
            gcp_clients,
            model_type,
            constants.SENIORITY_PROFILES_TABLE,
        )
        # finally, let's generate a combined latest pred df with all attributes - job, industry, seniority and cluster
        self.combined_latest_pred = self.combine_profile_attributes(
            industry_downloader, jobs_downloader, seniority_downloader
        )
        return True

    def produce_predictions_and_profiles(
        self, domains, lookback_periods_all, end_date, gcp_clients
    ):
        # before we start, let's download some data that we will need later on
        # TODO: should i pass the dataframe itself here? Memory taken up, but won't have to write/read?
        self.download_existing_clusters(end_date, lookback_periods_all, gcp_clients)
        # let's go over each domain
        for domain, lookback_period in zip(domains, lookback_periods_all):
            # first, identify the location of the features and artefacts
            domain_dir = f"{self._pred_dir}/{domain}"
            # now load the features data, and remove unnecessary columns
            df = pd.read_csv(f"{domain_dir}/{end_date}.csv", index_col="user_id")
            df = df.drop(["Unnamed: 0", "segments"], axis=1)
            # preprocess the data for prediction
            preprocessed_df = self.preprocess_for_prediction(df, domain_dir)
            # predict the preprocessed data
            predictions_df = self.predict_clusters(preprocessed_df, domain_dir)
            # having extracted our predictions, we come to a key moment - we want to upload these predictions to gbq
            # however, since our plan is to have a single table for all domains, we should upload only
            # those rows where there is a change for the user
            self.upload_to_gbq(
                predictions_df, domain, end_date, lookback_period, gcp_clients
            )
            # now, let's profile our predictions
            self.profile_clusters(
                df, predictions_df, domain, lookback_period, end_date, gcp_clients
            )
            # having generated the profiles, let's persist the combined predictions with attributes
            # to be served by the API
            print(f"Saving latest predictions for {domain}.")
            pred_filename = f"{domain_dir}/latest_pred.csv"
            self.combined_latest_pred.to_csv(pred_filename)
            # also upload to google cloud storage
            self.upload_to_storage(domain, pred_filename)

            # Generate SCV Predictions
            scv_predictions = self.combined_latest_pred.copy()
            scv_predictions["domain"] = domain
            scv_predictions["predicted_industry"] = scv_predictions["industry"].apply(
                lambda x: True if pd.isnull(x) else False
            )
            scv_predictions["predicted_jobs"] = scv_predictions["jobs"].apply(
                lambda x: True if pd.isnull(x) else False
            )

            self.industry_distributions[domain] = self.calculate_distributions(
                scv_predictions, "industry", domain
            )
            self.jobs_distributions[domain] = self.calculate_distributions(
                scv_predictions, "jobs", domain
            )

            scv_predictions["predicted_industry"] = scv_predictions[
                "predicted_industry"
            ].apply(lambda x: True if x == "Unknown" else x)
            scv_predictions["predicted_jobs"] = scv_predictions["predicted_jobs"].apply(
                lambda x: True if x == "Unknown" else x
            )

            scv_predictions["cluster"] = scv_predictions["cluster"] + 1

            scv_predictions["industry"] = scv_predictions.apply(
                lambda x: x["industry"]
                if x["predicted_industry"] is False
                else self.predict_industry(domain=domain, cluster=x["cluster"]),
                axis=1,
            )
            scv_predictions["jobs"] = scv_predictions.apply(
                lambda x: x["jobs"]
                if x["predicted_jobs"] is False
                else self.predict_jobs(domain=domain, cluster=x["cluster"]),
                axis=1,
            )

            self.scv_predictions_list.append(scv_predictions)

            # finally, let's do some housekeeping and delete any files other than the one downloaded for today
            # also filter for any non-csv files (ipynb checkpoints for some reason...)
            csvs_to_delete = [
                pathlib.Path(f"{domain_dir}/{item}")
                for item in os.listdir(domain_dir)
                if ".csv" in item
            ]
            whitelist = [
                pathlib.Path(f"{domain_dir}/{end_date}.csv"),
                pathlib.Path(f"{domain_dir}/latest_pred.csv"),
            ]
            files_to_delete = [item for item in csvs_to_delete if item not in whitelist]
            print(f"Deleting files: {files_to_delete}")
            for path in files_to_delete:
                pathlib.Path.unlink(path)

        scv_predictions_all = pd.concat(self.scv_predictions_list, axis=0)
        self.upload_scv_predictions(
            predictions_df=scv_predictions_all, gcp_clients=gcp_clients
        )

        return True

    def predict_industry(self, domain, cluster):
        # get the original distributions, calculated at instantiation
        domain_distribution = self.industry_distributions[domain]
        cluster_distribution = domain_distribution[cluster]
        keys = list(cluster_distribution.keys())
        probabilities = list(cluster_distribution.values())
        # sample a response from the original distribution
        # TODO - do we want a random choice? Or other discretization?
        prediction = random.choices(keys, probabilities)
        return prediction[0]

    def predict_jobs(self, domain, cluster):
        domain_distribution = self.jobs_distributions[domain]
        cluster_distribution = domain_distribution[cluster]
        keys = list(cluster_distribution.keys())
        probabilities = list(cluster_distribution.values())
        # sample a response from the original distribution
        # TODO - do we want a random choice? Or other discretization?
        prediction = random.choices(keys, probabilities)
        return prediction[0]

    def calculate_distributions(self, enriched_preds_df, attribute, domain):
        for i in range(9):
            row = [
                "placeholder_uid",
                i,
                "Unknown",
                "Unknown",
                "Unknown",
                "Unknown",
                domain,
                True,
                True,
            ]
            enriched_preds_df.loc[len(enriched_preds_df)] = row
        distributions_df = (
            enriched_preds_df.groupby(["cluster", attribute], as_index=False)
            .agg("count")
            .iloc[:, :3]
        )
        distributions = {}
        for cluster in distributions_df["cluster"].unique().tolist():
            cluster_df = distributions_df[distributions_df["cluster"] == cluster]
            uniques = cluster_df[attribute].unique().tolist()
            cluster_distribution = {}
            cluster_total = sum(cluster_df.iloc[:, -1].tolist())
            for unique in uniques:
                cluster_distribution[unique] = round(
                    int(cluster_df[cluster_df[attribute] == unique].iloc[:, -1].values)
                    / cluster_total,
                    3,
                )
            # make sure we increment by 1 to base on 1...
            distributions[cluster + 1] = cluster_distribution
        return distributions

    def download_from_storage(self, source_file, target_file):
        client = storage.Client(project=self._project_id)
        curr_bucket = client.bucket(self._bucket)
        blob = curr_bucket.get_blob(source_file)
        blob.download_to_filename(target_file)
        return True

    def get_domains_artefacts(self):
        storage_client = storage.Client(self._project_id)
        files_in_storage = [item for item in storage_client.list_blobs(self._bucket)]
        domains = list(set([item.name.split("/")[0] for item in files_in_storage]))
        print(domains)
        # having identified the domains, let's download the required artefacts
        # also instantiate an empty list to collect the lookback periods for all domains
        lookback_periods_all = []
        for domain in domains:
            # create the temp dir to hold artefacts (and data to predict downstream)
            domain_dir = f"{self._pred_dir}/{domain}"
            # delete current contents of domain_dir
            if os.path.isdir(domain_dir):
                shutil.rmtree(pathlib.Path(domain_dir))
            pathlib.Path(domain_dir).mkdir(parents=True, exist_ok=True)
            # download the model
            model_file = [
                item.name
                for item in files_in_storage
                if all(x in item.name for x in [domain, "clusters.sav"])
            ][0]
            self.download_from_storage(
                model_file, f"{domain_dir}/{model_file.split('/')[-1]}"
            )
            # download the imputer
            imputer_file = [
                item.name
                for item in files_in_storage
                if all(x in item.name for x in [domain, "imputer.pkl"])
            ][0]
            self.download_from_storage(
                imputer_file,
                f"{domain_dir}/{imputer_file.split('/')[-1]}",
            )
            # download the scaler
            scaler_file = [
                item.name
                for item in files_in_storage
                if all(x in item.name for x in [domain, "scaler.pkl"])
            ][0]
            self.download_from_storage(
                scaler_file,
                f"{domain_dir}/{scaler_file.split('/')[-1]}",
            )
            # check to see if there is a 'selection' file
            selection_file = [
                item.name
                for item in files_in_storage
                if all(x in item.name for x in [domain, "selection_for_"])
            ]
            if len(selection_file) > 0:
                selection_file = selection_file[0]
                self.download_from_storage(
                    selection_file,
                    f"{domain_dir}/{selection_file.split('/')[-1]}",
                )
            # lastly, we need to find out what is the lookback period for each of the domains
            lookback_period = [
                int(item.name.split("/")[-1].split("_")[1])
                for item in files_in_storage
                if all(
                    x in item.name
                    for x in [domain, "lookback_", "__fit_data_predictions.csv"]
                )
            ][0]
            lookback_periods_all.append(lookback_period)
        # set the function attribute to request after func has completed
        self.lookback_periods = lookback_periods_all
        return domains

    def predict_and_profile(self):
        # first, extract the list of domains that we are to predict for, namely
        # all domains that have been 'deployed'
        domains = self.get_domains_artefacts()
        lookback_periods_all = self.lookback_periods
        # now, let's define our 'end_date' - being yesterday...
        end_date = datetime.strftime(datetime.now() - timedelta(1), "%Y-%m-%d")
        # now, let's download the data that we'll use to predict
        model_type = "prediction"  # send to indicate we're in 'inference' mode
        data_downloader = ClusterDownloader(model_type, self._data_version)
        params = {
            "lookback_period": lookback_periods_all,
            "end_date": end_date,
        }
        data_downloader.get_files(params, domains)
        gcp_clients = (
            data_downloader.bqclient,
            data_downloader.bqstorageclient,
            data_downloader.pulldata_project,
            data_downloader.permutive_project,
        )
        # the next step is to preprocess each file with the respective artefacts,
        # and run .predict with the respective model
        self.produce_predictions_and_profiles(
            domains, lookback_periods_all, end_date, gcp_clients
        )
