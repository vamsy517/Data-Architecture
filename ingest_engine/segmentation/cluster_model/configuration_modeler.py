import pandas as pd
import numpy as np
import os
from pickle import dump, load
import pathlib
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from matplotlib import pyplot as plt
import time

import segmentation.cluster_model.segmentation_constants as constants
import segmentation.cluster_model.utils as utils
from segmentation.config.load_config import load_config

# Config
config = load_config()


class Modeller:

    # initialize the class
    def __init__(self, model_type, data_version, model_version):
        self._root_dir = config["root_dir"]
        self._model_type = model_type
        self._data_version = data_version
        self._model_version = model_version

    def predict(self, df, model_dir, pred_id):

        # we expect the predict method to persist the dataframe
        # it has predicted in the location of the model, with the filename
        # also containing the pred_id
        model_filename_candidates = [
            item for item in os.listdir(model_dir) if "clusters.sav" in item
        ]
        assert len(model_filename_candidates) == 1

        model_filename = pathlib.Path(f"{model_dir}/{model_filename_candidates[0]}")
        kmeans = load(open(model_filename, "rb"))
        pred_file = pathlib.Path(f"{model_dir}/predictions_{pred_id}.csv")

        if pred_file.is_file():
            pass
        else:
            predicted_clusters = kmeans.predict(df)
            predicted_df = df.copy()
            predicted_df["cluster"] = predicted_clusters
            predicted_df.reset_index(inplace=True)

            # persist predictions
            predicted_df.to_csv(pred_file)

        return True

    def fit_for_wcss(self, df, i):

        start_time = time.time()

        # fit a kmeans
        kmeans = KMeans(
            n_clusters=i,
            init=constants.KMEANS_INIT,
            max_iter=constants.KMEANS_ITER,
            n_init=constants.KMEANS_NINIT,
            random_state=constants.RANDOM_SEED,
        )
        kmeans.fit(df)

        # calculate the wcss and append it to the list for reporting
        inertia = kmeans.inertia_

        total_time = time.time() - start_time

        clusters = i

        return (inertia, total_time, clusters)

    def wcss_search(self, df, config_dir):

        # finding nclusters centre automatically, we still have a lower and upper bound, hardcoded here
        lower_bound = constants.LOWER_BOUND_WCSS
        upper_bound = constants.UPPER_BOUND_WCSS
        step = constants.STEP_WCSS

        print(
            f"Searching wcss default range of {lower_bound} to {upper_bound} clusters."
        )

        # for each number in the range we search through
        list_of_nclusters = list(range(lower_bound, upper_bound + 1, step))
        wcss_data = list(map(lambda x: self.fit_for_wcss(df, x), list_of_nclusters))
        wcss = [item[0] for item in wcss_data]
        times = [item[1] for item in wcss_data]
        clusters = [item[2] for item in wcss_data]

        # to calculate the elbow, we need to find the spot where the proportion between
        # the difference in wcss for coming from the previous fit (i-1 > i) and for going to the next fit (i > i+1)
        # is the biggest, meaning this is where the elbow angle is sharpest
        diff_inertia = [
            (wcss[i - 1] - wcss[i]) / (wcss[i] - wcss[i + 1])
            if i != 0 and i != len(wcss) - 1
            else 0
            for i in range(len(wcss))
        ]

        # find which n_cluster got the sharpest elbow
        n_clusters_elbow = clusters[np.argmax(diff_inertia)]

        # save the reporting data to csv, and to an image, also plot the image
        fit_data = pd.DataFrame({"Clusters": clusters, "WCSS": wcss, "Time": times})
        fit_data.to_csv(f"{config_dir}/elbow_data.csv")

        plt.title("Elbow Method")
        plt.xlabel("Number of clusters")
        plt.ylabel("WCSS")

        # make sure the best n_cluster is visible in the filename itself
        plt.savefig(f"{config_dir}/wcss_{n_clusters_elbow}.png")
        plt.close()

        # return the reporting data as a dataframe
        return n_clusters_elbow

    def fit_model(self, df, config_dir, n_clusters):

        # construct the dir - with parameters for versioning
        fit_dir = f"{config_dir}/{n_clusters}_clusters/"
        pathlib.Path(fit_dir).mkdir(parents=True, exist_ok=True)
        model_name = pathlib.Path(fit_dir + f"fit_{n_clusters}clusters.sav")

        # check if a model with the configuration has already been fit
        if model_name.is_file():
            kmeans = load(open(model_name, "rb"))
        else:
            # if not, then we fit a new model
            kmeans = KMeans(
                n_clusters=n_clusters,
                init=constants.KMEANS_INIT,
                max_iter=constants.KMEANS_ITER,
                n_init=constants.KMEANS_NINIT,
                random_state=constants.RANDOM_SEED,
            )
            kmeans.fit(df)

            # and persist it
            dump(kmeans, open(model_name, "wb"))

        # from the fit model, extract the cluster centroids, and place in a readable dataframe
        centroid_df = pd.DataFrame(kmeans.cluster_centers_, columns=df.columns)

        # load the scaler so that you can unscale the centroids data
        scaler_name = pathlib.Path(config_dir + "/scaler.pkl")
        scaler = load(open(scaler_name, "rb"))

        # apply the inverse transform of the scaler and save the unscaled data in a readable df
        unscaled_centroid_array = scaler.inverse_transform(centroid_df)
        unscaled_centroid_df = pd.DataFrame(
            data=unscaled_centroid_array,
            index=centroid_df.index,
            columns=centroid_df.columns,
        )

        # persist the df to csv
        # TODO: probably move this to the if clause, as it refreshes centroids when we re-fit
        unscaled_centroid_df.to_csv(f"{fit_dir}unscaled_centroids.csv")

        # lastly, run predictions on the fitting data, for eventual RF feature selection
        fit_data_predictions_filename = pathlib.Path(
            fit_dir + "fit_data_predictions.csv"
        )

        if fit_data_predictions_filename.is_file():
            pass
        else:
            predicted_clusters = kmeans.labels_
            predicted_df = df.copy()
            predicted_df["cluster"] = predicted_clusters
            predicted_df.reset_index(inplace=True)

            # persist predictions
            predicted_df.to_csv(fit_data_predictions_filename)

        return True

    def model_configuration(self, df, params):

        print(f"Fitting models for configuration for {params['domains']}.")

        # create bash
        cols_to_keep_hash = utils.get_hash(params["cols_to_keep"])
        cols_to_trim_hash = utils.get_hash(params["cols_to_trim"])

        # construct the dir - with parameters for versioning
        config_dir = (
            f"{self._root_dir}/models/v_{self._model_version}/{params['domains']}/lookback_{params['lookback_period']}/fit_for_{params['end_date']}"
            f"/outliers_{cols_to_trim_hash}/zscore_{params['zscore_cutoff']}/selected_features_{cols_to_keep_hash}"
        )

        # if the centre point has been given as 'auto', let's run a wcss search and extract the midpoint algorithmically
        if params["clusters_centre"] == "auto":

            wcss_file = pathlib.Path(config_dir + "/elbow_data.csv")
            if wcss_file.is_file():
                clusters_centre = [
                    int(item.split(".")[0].split("_")[-1])
                    for item in os.listdir(config_dir)
                    if item[:5] == "wcss_"
                ][0]
                print(
                    f"Using already performed wcss search, with best n_clusters centre at {clusters_centre} clusters."
                )
            else:
                clusters_centre = self.wcss_search(df, config_dir)
        else:
            clusters_centre = params["clusters_centre"]

        # having calculated or received the n_cluster midpoint, let's fit n_clusters_fits number of models!
        # first, create a list of the n_clusters for each of the fits - taking the 'n_clusters_fits' and centring on the
        # above-calculated or received centre
        lower_bound = clusters_centre - round((constants.KMEANS_K - 1) / 2)
        upper_bound = clusters_centre + round((constants.KMEANS_K - 1) / 2) + 1
        list_of_nclusters = list(range(lower_bound, upper_bound, 1))

        # finally, fit models & persist for each of the n_clusters in the grid (also includes predictions for the evaluation set)
        list(map(lambda x: self.fit_model(df, config_dir, x), list_of_nclusters))

        return True


class Feature_Selector:

    # initialize the class
    def __init__(self, model_type, data_version, model_version):
        self._root_dir = config["root_dir"]
        self._model_type = model_type
        self._data_version = data_version
        self._model_version = model_version

    def calculate_feature_importance(self, file_loc):

        # load the predictions file
        pred_df = pd.read_csv(file_loc, index_col="user_id")
        pred_df = pred_df.drop(["Unnamed: 0"], axis=1)

        # set the features are targets to be used by the rf classifier
        features_df = pred_df.iloc[:, :-1]
        targets_df = pred_df.iloc[:, -1]

        # fit the classifier
        clf = RandomForestClassifier(random_state=constants.RANDOM_SEED)
        clf.fit(features_df, targets_df)

        # to extract the importances, we use the built-in function
        features = features_df.columns.tolist()
        feature_importances = clf.feature_importances_.tolist()
        importances_df = pd.DataFrame([features, feature_importances]).transpose()

        # lastly, let's name the column to id which fit saw that importance
        n_clusters = file_loc.split("/")[-2]
        importances_df.columns = ["feature", f"importance_{n_clusters}"]

        return importances_df

    def extract_best_features(self, params, LIST_OF_MUSTHAVE_FEATURES, fine_tune):

        print(
            f"Extracting best features for models for configuration for {params['domains']}."
        )

        cols_to_trim_hash = utils.get_hash(params["cols_to_trim"])
        cols_to_keep_hash = utils.get_hash(params["cols_to_keep"])

        # now, our base folder is the one where there are already selected features - selection coming from upstream
        config_dir = (
            f"{self._root_dir}/models/v_{self._model_version}/{params['domains']}/lookback_{params['lookback_period']}/fit_for_{params['end_date']}"
            f"/outliers_{cols_to_trim_hash}/zscore_{params['zscore_cutoff']}/selected_features_{cols_to_keep_hash}"
        )

        # for each model in the base config_dir, find the 'fit_data_predictions.csv'
        # to collate the results, let's instantiate an empty df
        all_importances = pd.DataFrame(columns=["feature"])

        # let's find the models and their predictions
        pred_files = [
            f"{config_dir}/{item}/fit_data_predictions.csv"
            for item in os.listdir(config_dir)
            if "_clusters" in item
        ]

        importance_dfs = list(
            map(lambda x: self.calculate_feature_importance(x), pred_files)
        )

        # the importances now live in a list of dataframes; let's merge them and calculate the average
        for importance_df in importance_dfs:
            all_importances = pd.merge(
                all_importances,
                importance_df,
                how="outer",
                left_on=["feature"],
                right_on=["feature"],
            )

        # aggregate the importance across the fits
        all_importances["average_importance"] = all_importances.apply(
            lambda x: np.nanmean(x.iloc[1:].tolist()), axis=1
        )

        # now we have a dataframe containing all features for our configuration, and their estimated importances
        # we now need to select only a shortlist of those (based on the fine_tune parameter)
        # and make sure we include all the features from the LIST_OF_MUSTHAVE_FEATURES parameter
        n_features_to_add = fine_tune - len(LIST_OF_MUSTHAVE_FEATURES)

        # get all rows where the feature is not in the musthave list
        other_features_df = all_importances[
            ~all_importances["feature"].isin(LIST_OF_MUSTHAVE_FEATURES)
        ]

        # sort the list and get the top N features, according to the fine_tune calc above
        other_features = other_features_df.sort_values(
            "average_importance", ascending=False
        )["feature"].tolist()

        features_to_add = other_features[:n_features_to_add]

        # extend the list of must have features with the top N from the others
        full_features = LIST_OF_MUSTHAVE_FEATURES.copy()
        full_features.extend(features_to_add)

        # finally, select the final list of features from the importances df - to be persisted and used for fitting
        final_importances = all_importances[
            all_importances["feature"].isin(full_features)
        ]

        # persist the importances file - it should be named with the hash of the features to present
        new_cols_to_keep_hash = utils.get_hash(final_importances["feature"].tolist())

        # the file should sit in the folder above the selection
        selection_filename = f"{config_dir.split('/selected_features_')[0]}/selection_for_{new_cols_to_keep_hash}.csv"
        final_importances.to_csv(selection_filename)

        return full_features
