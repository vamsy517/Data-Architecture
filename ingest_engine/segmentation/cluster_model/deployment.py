import pandas as pd
import numpy as np
import time
import os
import re
import shutil
import pathlib
from datetime import datetime, timedelta
from google.cloud import storage  # type: ignore
from pickle import dump
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import confusion_matrix, classification_report
import matplotlib.pyplot as plt
import seaborn as sns
from segmentation.downloader import cluster_model_downloader
from segmentation.cluster_model import segmentation_constants as constants
import segmentation.cluster_model.utils as utils
from segmentation.config.load_config import load_config

# Config
config = load_config()


class Classifier:
    # initialize the class
    def __init__(self, data_version):
        self._root_dir = config["root_dir"]
        self._data_version = data_version

    def localize_hour(self, utc_hour, continent):
        if continent == "North America":
            local_hour = utc_hour + constants.NA_HOUR_DIFF
        elif continent == "South America":
            local_hour = utc_hour + constants.SA_HOUR_DIFF
        elif continent == "Europe":
            local_hour = utc_hour + constants.EU_HOUR_DIFF
        elif continent == "Africa":
            local_hour = utc_hour + constants.AF_HOUR_DFF
        elif continent == "Asia":
            local_hour = utc_hour + constants.AS_HOUR_DIFF
        else:
            local_hour = utc_hour
        # make sure there's no negatives in the localized hour
        if local_hour < 0:
            local_hour = local_hour + 24
        # lastly, make sure 24 is 0
        if local_hour == 24:
            local_hour = 0
        return local_hour

    def extract_os_group(self, user_agent):
        if constants.LINUX_AGENT_STR in user_agent:
            os_group = "linux"
        elif constants.ANDROID_AGENT_STR in user_agent:
            os_group = "android"
        elif constants.WIN10_AGENT_STR in user_agent:
            os_group = "windows10"
        elif (
            constants.WIN_AGENT_STR in user_agent
            and constants.WIN10_AGENT_STR not in user_agent
        ):
            os_group = "windows_older"
        elif constants.IPHONE_AGENT_STR in user_agent:
            os_group = "iphone"
        elif constants.MAC_AGENT_STR in user_agent:
            os_group = "macintosh"
        elif constants.APPLE_AGENT_STR in user_agent:
            os_group = "apple_other"
        else:
            os_group = "other_os"
        return os_group

    def extract_referrer_group(self, referrer, domain):
        social_list = constants.SOCAL_REF_LIST
        search_list = constants.SEARCH_REF_LIST
        nsmg_network = constants.NSMG_REF_LIST
        email = constants.EMAIL_REF_LIST
        own_website = [domain]
        if any(wildcard in referrer for wildcard in social_list):
            referrer_group = "social"
        elif any(wildcard in referrer for wildcard in search_list):
            referrer_group = "search"
        elif any(wildcard in referrer for wildcard in nsmg_network):
            referrer_group = "NSMG"
        elif any(wildcard in referrer for wildcard in own_website):
            referrer_group = "own_website"
        elif any(wildcard in referrer for wildcard in email):
            referrer_group = "email"
        else:
            referrer_group = "other_referrer"
        return referrer_group

    def preprocess_rf_data(self, df, domain):
        # preprocess the 'time' field - it should contain only the 'hour' - 0:24
        df["hour_of_day_utc"] = df.apply(
            lambda x: int(str(x["time"]).split(" ")[1].split(":")[0]), axis=1
        )
        # approximately 'localize' the time by adjusting according ot the continent
        df["local_hour_proxy"] = df.apply(
            lambda x: self.localize_hour(x["hour_of_day_utc"], x["continent"]), axis=1
        )
        # drop the original and helper column
        df = df.drop(["time", "hour_of_day_utc"], axis=1)
        # extract the referrer group approximation
        referrer_df = df.fillna("other")
        referrer_df["referrer_group"] = referrer_df.apply(
            lambda x: self.extract_referrer_group(x["referrer"], domain), axis=1
        )
        # drop the original column
        referrer_df = referrer_df.drop(["referrer"], axis=1)
        os_df = referrer_df.copy()
        os_df["os_group"] = os_df.apply(
            lambda x: self.extract_os_group(x["user_agent"]), axis=1
        )
        # drop the original column
        os_df = os_df.drop(["user_agent"], axis=1)
        return os_df

    def download_rf_train(
        self,
        domain,
        lookback_period,
        end_date,
        bqclient,
        bqstorageclient,
        permutive_project,
    ):
        start_date = datetime.strftime(
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(int(lookback_period)),
            "%Y-%m-%d",
        )
        rf_train_query = f"""
select * except(rn) from (
select user_id, time, properties.client.domain, properties.client.referrer, properties.geo_info.continent, properties.client.user_agent,
row_number() over (partition by user_id order by time DESC) as rn
from {permutive_project}.global_data.pageview_events
where (properties.client.domain = '{domain}' or properties.client.domain = 'www.{domain}')
and DATE(_PARTITIONTIME) <= '{end_date}' and DATE(_PARTITIONTIME) >= '{start_date}'
) where rn = 1
"""
        train_df = (
            bqclient.query(rf_train_query)
            .result()
            .to_dataframe(bqstorage_client=bqstorageclient)
        )
        return train_df

    def generate_confusion_matrix(self, prod_dir, test_y, test_preds):
        TEXT_COLOR = "k"
        conf_mat = confusion_matrix(test_y, test_preds)
        conf_mat_norm = conf_mat.astype("float") / conf_mat.sum(axis=1)[:, np.newaxis]
        conf_mat_df = pd.DataFrame(conf_mat_norm)
        conf_mat_df = conf_mat_df.round(2)
        sns.heatmap(conf_mat_df, annot=True)
        plt.title("Confusion Matrix", color=TEXT_COLOR, fontsize=20)
        plt.ylabel("True label", color=TEXT_COLOR, fontsize=16, rotation=0, labelpad=30)
        plt.xlabel("Predicted label", color=TEXT_COLOR, fontsize=16)
        plt.xticks(color=TEXT_COLOR, fontsize=12)
        plt.yticks(color=TEXT_COLOR, fontsize=12, rotation=0)
        plt.savefig(f"{prod_dir}/rf_confusion_matrix.png")
        plt.close()
        return True

    def evaluate_rf_fit(self, prod_dir, df_test, encoder, clf):
        categorical_features_df_test = df_test[
            ["domain", "continent", "referrer_group", "os_group"]
        ]
        ohe_array_test = encoder.transform(categorical_features_df_test)
        ohe_df_test = pd.DataFrame(
            ohe_array_test,
            index=categorical_features_df_test.index,
            columns=encoder.get_feature_names(),
        )
        # finally, let's add the two other columns to this df to have a compelete, good-to-go df
        df_test_enc = ohe_df_test.join(df_test[["local_hour_proxy", "cluster"]])
        test_y, test_x = (
            df_test_enc["cluster"],
            df_test_enc.loc[:, df_test_enc.columns != "cluster"],
        )
        # predict and run a classification report
        test_preds = clf.predict(test_x)
        report = classification_report(test_y, test_preds, output_dict=True)
        scoring_df = pd.DataFrame(report)
        scoring_df.to_csv(f"{prod_dir}/rf_classification_report.csv")
        # finally, let's generate and persist a confusion matrix
        self.generate_confusion_matrix(prod_dir, test_y, test_preds)
        return True

    def fit_and_evaluate_rf_models(self, prod_dir, preprocessed_df):
        # let's set the random seed to be used downstream
        random_state = constants.RANDOM_SEED
        df_train, df_test = train_test_split(
            preprocessed_df, train_size=0.8, test_size=0.2, random_state=random_state
        )
        # finally, let's fit a one-hot encoder for our categorical variables, and transform the data
        # let's first isolate our categorical variables in a separate df
        categorical_features_df = df_train[
            ["domain", "continent", "referrer_group", "os_group"]
        ]
        # instantiate and fit the encoder
        encoder = OneHotEncoder(handle_unknown="ignore", sparse=False)
        encoder.fit(categorical_features_df)
        # persist the encoder
        encoder_file = f"{prod_dir}/rf_ohe_encoder.pkl"
        dump(encoder, open(encoder_file, "wb"))
        ohe_array = encoder.transform(categorical_features_df)
        ohe_df = pd.DataFrame(
            ohe_array,
            index=categorical_features_df.index,
            columns=encoder.get_feature_names(),
        )
        # finally, let's add the two other columns to this df to have a compelete, good-to-go df
        df_train_enc = ohe_df.join(df_train[["local_hour_proxy", "cluster"]])
        # let's split the targets from the features
        train_y, train_x = (
            df_train_enc["cluster"],
            df_train_enc.loc[:, df_train_enc.columns != "cluster"],
        )
        # and fit and persist the model
        clf = RandomForestClassifier(
            min_samples_split=constants.RF_MIN_SAMPLES_SPLIT,
            class_weight="balanced",
            verbose=0,
            random_state=random_state,
        )
        clf.fit(train_x, train_y)
        model_name = f"{prod_dir}/rf_classifier.sav"
        dump(clf, open(model_name, "wb"))
        # finally, let's evaluate the fit
        self.evaluate_rf_fit(prod_dir, df_test, encoder, clf)
        return True

    def fit_random_forest(self, prod_dir, domain, lookback_period, end_date):
        # to fit a random forest classifier, we need to pass a few steps
        # 1) download training data from bigquery
        # 2) preprocess data - as a lambda (to be used in serving as well?)
        # 3) merge with predictions in prod_dir
        # 4) fit encoder and model - persist artefacts in prod_dir
        # 5) evaluate model - persist classification report in prod_dir
        # let's first instantiate the clients and get project_ids
        runtype = "prediction"
        downloader = cluster_model_downloader.ClusterDownloader(
            runtype, self._data_version
        )
        (
            bqclient,
            bqstorageclient,
            pulldata_project,
            permutive_project,
        ) = downloader.get_clients()
        # call the generator method
        features_df = self.download_rf_train(
            domain,
            lookback_period,
            end_date,
            bqclient,
            bqstorageclient,
            permutive_project,
        )
        # merge with the predictions, and leave only those rows where there is a cluster prediction
        pred_df = pd.read_csv(f"{prod_dir}/fit_data_predictions.csv", index_col=0)[
            ["user_id", "cluster"]
        ]
        train_df = pd.merge(
            features_df, pred_df, how="left", left_on=["user_id"], right_on=["user_id"]
        )
        train_df = train_df[~train_df["cluster"].isna()]
        # now we have a dataframe with the required data
        # let's preprocess
        preprocessed_df = self.preprocess_rf_data(train_df, domain)
        # now, we need to fit a one-hot encoder and transform the data accordingly
        # but first - split for train and test
        preprocessed_df = preprocessed_df.set_index("user_id")
        # finally, let's fit the encoder and model
        self.fit_and_evaluate_rf_models(prod_dir, preprocessed_df)
        return True


class Deployer:
    # initialize the class
    def __init__(self, params, data_version, model_version):
        self._root_dir = config["root_dir"]
        self._params = params
        self._data_version = data_version
        self._model_version = model_version

    def upload_to_storage(self, project_id, bucket_name, source_file, target_file):
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(target_file)
        with open(source_file, "rb") as my_file:
            blob.upload_from_file(my_file)

    def remove_previous_deployment(self, project_id, bucket_name, domain):
        client = storage.Client(project=project_id)
        bucket = client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=domain)
        for blob in blobs:
            blob.delete()

    def fit_rf_classifier(self, domain, best_model_dir, prod_dir):
        print(f"Fitting random forest classifier for {domain}'s best clustering model.")
        # if so, first, let's also move the fit predictions to the deploy folder
        fit_pred_filename_candidates = [
            item
            for item in os.listdir(best_model_dir)
            if item == "fit_data_predictions.csv"
        ]
        assert len(fit_pred_filename_candidates) == 1
        fit_pred_filename = fit_pred_filename_candidates[0]
        from_fit_pred = pathlib.Path(f"{best_model_dir}/{fit_pred_filename}")
        to_fit_pred = pathlib.Path(f"{prod_dir}/{fit_pred_filename}")
        shutil.copy(from_fit_pred, to_fit_pred)
        # lastly, let's extract the parameters we need for the RF fitting process
        # we already have domain, prod_dir, predictions and we need end_date and lookback_period
        end_date = re.split("/|\\\\", best_model_dir.split("fit_for_")[-1])[0]
        lookback_period = re.split("/|\\\\", best_model_dir.split("lookback_")[-1])[0]
        # let's call the method to fit a model and send artefacts to prod_dir
        classifier = Classifier(self._data_version)
        classifier.fit_random_forest(prod_dir, domain, lookback_period, end_date)

    def push_to_storage(self, domain, best_model_dir, prod_dir):
        print(f"Deploying model configuration to storage for {domain} production.")
        runtype = "prediction"
        downloader = cluster_model_downloader.ClusterDownloader(
            runtype, self._data_version
        )
        (
            bqclient,
            bqstorageclient,
            pulldata_project,
            permutive_project,
        ) = downloader.get_clients()
        project_id = pulldata_project
        bucket = constants.STORAGE_BUCKET
        # lets remove previous deployments from bucket
        self.remove_previous_deployment(project_id, bucket, domain)

        # copy all items
        for item in os.listdir(prod_dir):
            source_file = f"{prod_dir}/{item}"
            # we need to somehow pass to storage the lookback_period variable
            # to be used during prediction
            # let's insert that in the name of the fit_data_predictions.csv file
            if item == "fit_data_predictions.csv":
                lookback_period = re.split(
                    "/|\\\\", best_model_dir.split("lookback_")[-1]
                )[0]
                target_file = f"{domain}/lookback_{lookback_period}__{item}"
            else:
                target_file = f"{domain}/{item}"
            self.upload_to_storage(project_id, bucket, source_file, target_file)

    def move_files_to_prod(
        self,
    ):
        self._params["domains"] = [
            utils.clean_domains(i) for i in self._params["domains"]
        ]
        # let's go over each domain individually
        for domain in self._params["domains"]:
            print(f"Moving {domain} best model artefacts to prod dir.")
            # within the folder for the evaluation of the models for each domain, get all 'shortlist' files - may be more than one
            # and we want to union these; do we?
            # getting the model_dir of the top model
            shortlist_metrics_dir = (
                f"{self._root_dir}/evaluation/v_{self._model_version}/{domain}"
            )
            shortlist_metrics_files = [
                item
                for item in os.listdir(shortlist_metrics_dir)
                if "_shortlist_" in item
            ]
            # let's instantiate a list to hold all the dataframes that we'll read
            shortlist_dfs_list = []
            for shortlist_file in shortlist_metrics_files:
                shortlist_df = pd.read_csv(
                    f"{shortlist_metrics_dir}/{shortlist_file}", index_col=0
                )
                shortlist_dfs_list.append(shortlist_df)
            shortlists_df = pd.concat(shortlist_dfs_list)
            # let's sort by by 'weighted_profiling_score' ascending = False
            # TODO - move the sorting dim to constants?
            # in case there are ties, let's add the other metrics by order of importance we see
            shortlists_df.sort_values(
                ["ranking_score", "cluster_variance", "centroid_similarity"],
                ascending=[False, True, False],
                inplace=True,
            )
            # filter only the target models remain
            if self._params["target_n_clusters"]:
                shortlists_df = shortlists_df[
                    shortlists_df["n_clusters"].isin(self._params["target_n_clusters"])
                ]
            if self._params["target_lookback"]:
                shortlists_df = shortlists_df[
                    shortlists_df["lookback_period"].isin(
                        self._params["target_lookback"]
                    )
                ]
            # and now to select and move to prod the best model we want to find its dir
            best_model_dir = shortlists_df.iloc[0, :]["model_dir"]
            # let's now create a folder to house the model itself, along with its artefacts
            timestamp = round(time.time())
            prod_dir = f"{self._root_dir}/deployment/v_{self._model_version}/{domain}/deployment_{timestamp}"
            pathlib.Path(prod_dir).mkdir(parents=True, exist_ok=True)
            # now we have both the target directory and the source directory;
            # let's copy all the required artefacts, including some we'll get upper in the parameter stream
            # first, let's simply copy the three items we need from the model dir to the target dir
            # namely: the model itself (.sav), and the profiles for the fit data (_profiles_fit.csv)
            model_filename_candidates = [
                item for item in os.listdir(best_model_dir) if "clusters.sav" in item
            ]
            assert len(model_filename_candidates) == 1
            model_filename = model_filename_candidates[0]
            from_file_model = pathlib.Path(f"{best_model_dir}/{model_filename}")
            to_file_model = pathlib.Path(f"{prod_dir}/{model_filename}")
            shutil.copy(from_file_model, to_file_model)
            # repeat for the profile
            profile_filename_candidates = [
                item
                for item in os.listdir(best_model_dir)
                if "_profiles_fit.csv" in item
            ]

            for profile_filename in profile_filename_candidates:
                from_file_profile = pathlib.Path(f"{best_model_dir}/{profile_filename}")
                to_file_profile = pathlib.Path(f"{prod_dir}/{profile_filename}")
                shutil.copy(from_file_profile, to_file_profile)

            # repeat for the scaler
            scaler_path = (
                "/".join(re.split("/|\\\\", best_model_dir.split("_clusters")[0])[:-1])
                + "/scaler.pkl"
            )
            from_file_scaler = pathlib.Path(f"{scaler_path}")
            to_file_scaler = pathlib.Path(f"{prod_dir}/scaler.pkl")
            shutil.copy(from_file_scaler, to_file_scaler)
            # a little tricky part here - also move the 'selection_for_{hash}.csv', if the model comes from that selection
            model_selection_hash = "/".join(
                re.split("/|\\\\", best_model_dir.split("_clusters")[0])[:-1]
            ).split("_")[-1]
            # now we have the hash of the feature selection that the best model has
            # let's inspect the folder above the scaler (selection folder) and get all .csv files
            # they are all the specific feature selections for downstream models
            # if our best model is one of those models, we need to copy that .csv to prod
            selection_folder = "/".join(
                re.split("/|\\\\", best_model_dir.split("selected_features_")[0])[:-1]
            )
            selection_files = [
                item
                for item in os.listdir(selection_folder)
                if "selection_for_" in item
            ]
            selection_files_hashes = [
                item.split("_")[-1].split(".")[0] for item in selection_files
            ]
            if model_selection_hash in selection_files_hashes:
                from_file_selector = pathlib.Path(
                    f"{selection_folder}/selection_for_{model_selection_hash}.csv"
                )
                to_file_selector = pathlib.Path(
                    f"{prod_dir}/selection_for_{model_selection_hash}.csv"
                )
                shutil.copy(from_file_selector, to_file_selector)
            # finally, let's also move the last artefact - the imputer
            imputer_path = (
                "/".join(re.split("/|\\\\", best_model_dir.split("outliers_")[0])[:-1])
                + "/imputer.pkl"
            )
            from_file_imputer = pathlib.Path(f"{imputer_path}")
            to_file_imputer = pathlib.Path(f"{prod_dir}/imputer.pkl")
            shutil.copy(from_file_imputer, to_file_imputer)
            # finally, let's see if we want to fit a random forest classifier for the model
            if self._params["fit_rf_classifiers"]:
                self.fit_rf_classifier(domain, best_model_dir, prod_dir)
            # finally, let's push the artefacts to google cloud storage
            if self._params["push_to_storage"]:
                self.push_to_storage(domain, best_model_dir, prod_dir)
