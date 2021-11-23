from .features_preprocessor import Preprocessor
from segmentation.downloader.cluster_model_downloader import ClusterDownloader
from .configuration_modeler import Modeller
import os
import re
import pandas as pd
import numpy as np
import pathlib
import glob
from collections import defaultdict
from sklearn.preprocessing import MinMaxScaler
import segmentation.cluster_model.utils as utils
from segmentation.config.load_config import load_config

# Config
config = load_config()


class Evaluator:
    # initialize the class
    def __init__(self, params, data_version, model_version):
        self._root_dir = config["root_dir"]
        self._params = params
        self._data_version = data_version
        self._model_version = model_version

    def extract_centroid_similarity(self, model_dir):

        # the first thing we need to do is extract the model paths for all models that share the same parameters as the one we are
        # currently working on
        model_dir_params = model_dir.split("/")
        model_dir_list = [
            "*" if item[:8] == "fit_for_" else item for item in model_dir_params
        ]
        model_dir_wildcard = "/".join(model_dir_list)
        # then put the wildcarded path in glob
        listing = glob.glob(model_dir_wildcard)
        paths_to_files = [f"{item}/unscaled_centroids.csv" for item in listing]
        # read each file - number of files is N in comments below
        all_files = [pd.read_csv(file, index_col=0) for file in paths_to_files]
        # also get the filename (period) that the model was fit on
        all_files_names = [
            path_to_file.split("fit_for_")[-1].split("/")[0]
            for path_to_file in paths_to_files
        ]
        # we come to an interesting predicament - some configurations will have only a single model
        # in those cases, we should replicate that, and the resulting similarities will be =1, so we will have to filter them later...
        if len(paths_to_files) == 1:
            all_files.append(all_files[0])
            all_files_names.append(f"{all_files_names[0]}_CLONE")
        # let's instantiate a dictionary
        # this dict will hold N keys - one for each of the fits with same parameters (ex. period for fit)
        # and N-1 values for each key - how different was each of these fits to the others
        best_similarities = defaultdict(list)
        # let's also instantiate another dict - to hold N keys and 1 value each, where
        # the keys are the fits and the values are the lists of similarities that each of the fit's clusters
        # have to the same clusters in the otehr fits
        cluster_similarities = {}
        # IMPORTANT: we should scale the centroid data before measuring cosine similarity
        centroid_scaler = MinMaxScaler()
        # let's fit on all of the provided files' data
        # IMPORTANT: these dataframes contain various number of features / columns (due to segments being filtered);
        # we should keep only those present in the least populous of these dataframes to move forward
        # we should address that by keeping only these features that are found in all dfs
        all_columns = list(set([item for sublist in all_files for item in sublist]))
        # having found all unique columns, let's leave only those found in each of the dataframes
        for df in all_files:
            all_columns = [item for item in all_columns if item in df.columns]
        # let's make sure all dataframes now have only these columns in their data
        all_files = [df[all_columns] for df in all_files]
        fit_df = pd.DataFrame()
        for dataframe in all_files:
            fit_df = fit_df.append(dataframe, ignore_index=True)
        centroid_scaler.fit(fit_df)
        # going over each of the files in question
        for centroid_file, centroid_file_name in zip(all_files, all_files_names):
            # scale the data
            curr_df = pd.DataFrame(centroid_scaler.transform(centroid_file))
            # lets also instantiate a dict to hold the SUM of cluster similarities of this file with the other files
            # this dict is like the one above, but contains only a single value per key: both N-1
            best_similarities_per_file = defaultdict(list)
            # for each cluster (row in the file read)
            for cluster in range(curr_df.shape[0]):
                # let's read the current cluster, and get it into array form for vector calculations
                curr_cluster_vector = np.array(curr_df.iloc[cluster, :])
                # now, let's read the 'other' files, using the names as guide
                other_files = [
                    (other_file, other_file_name)
                    for other_file, other_file_name in zip(all_files, all_files_names)
                    if centroid_file_name != other_file_name
                ]
                for other_file, other_file_name in other_files:
                    target_df = pd.DataFrame(centroid_scaler.transform(other_file))
                    # get all target vectors
                    all_target_vectors = [
                        np.array(target_df.iloc[cluster, :])
                        for cluster in range(target_df.shape[0])
                    ]
                    # calculate similarities with curr_vector
                    similarities = [
                        utils.calculate_cosine_similarity(curr_cluster_vector, tar_vec)
                        for tar_vec in all_target_vectors
                    ]
                    # get the closest vector's list index
                    closest_vector_index = np.argsort(similarities)[-1]
                    # let's append the similarities dict
                    # NOTE - we append once for each of the three keys, per 'for cluster in...' iteration
                    # result is dict with N-1 keys and 4-7 values (one for each cluster);
                    # TODO - we risk that some target clusters may be best fits for more than one curr_cluster
                    best_similarities_per_file[other_file_name].append(
                        similarities[closest_vector_index]
                    )
            # from the dict with N-1 keys and 4-7 values, we should sum along the keys
            # and append to the outside-of-the-loop dict to get a dict with N keys and N-1 values per key
            # i.e., a similarity metric from each of the OTHER three files
            for file in best_similarities_per_file.keys():
                best_similarities[file].append(np.sum(best_similarities_per_file[file]))
            # WIP BELOW ------
            # to get the average similarity per cluster for each fit, we should also average along the values of the
            # per-file dicts; first - calculate the max number of clusters being scanned
            max_n_clusters = max(len(v) for v in best_similarities_per_file.values())
            # let's instantiate a list to hold the average similarities per clusterF
            average_similarities_per_cluster = []
            # for each cluster in the range(max_n_clusters) we need to find the average similarity across files
            for cluster in range(max_n_clusters):
                # calculate the average - going over each value for each key, normallizing for number of clusters
                similarities_for_the_cluster = [
                    best_similarities_per_file[key][cluster]
                    if len(best_similarities_per_file[key]) >= max_n_clusters
                    else np.nan
                    for key in best_similarities_per_file.keys()
                ]
                # do the averaging
                avg_similarity = np.nanmean(similarities_for_the_cluster)
                # append to the list of similarities
                average_similarities_per_cluster.append(avg_similarity)
            # finally, append the list of similarities as an item to the outside-of-the-loop dict
            cluster_similarities[centroid_file_name] = average_similarities_per_cluster
        # lastly, let's average across the different files to get the average sim per cluster for the whole configuration
        cluster_similarities_list = []
        for cluster in range(len(list(cluster_similarities.values())[0])):
            all_sims_per_cluster_per_fit = [
                cluster_similarities[key][cluster]
                for key in cluster_similarities.keys()
            ]
            avg_sims_per_cluster_per_fit = np.nanmean(all_sims_per_cluster_per_fit)
            cluster_similarities_list.append(avg_sims_per_cluster_per_fit)
        # WIP ABOVE -----
        # now the 'best_similarities' disctionary contains N keys, each with N-1 values - how close was each of the files
        # to the each of the other files in vector space
        # we should do an average of these to get the 'final', though max is also good ()
        final_similarity = {}
        for k, v in best_similarities.items():
            # v is the list of grades for student k
            final_similarity[k] = np.mean(v)

        # now, lets construct the response of this method
        # first - lets order the dict and NORMALIZE to the number of clusters
        final_similarity = {
            k: v / all_files[0].shape[0]
            for k, v in sorted(final_similarity.items(), key=lambda x: x[1])
        }
        # finally, let's get as variables the ordered filenames, actual similarities,
        # as well as the average similarity for THE WHOLE CONFIGURATION - this is the nucleus of
        # the centroid distribution analysis
        worst_to_best_files = list(final_similarity.keys())
        worst_to_best_similarity = list(final_similarity.values())
        average_similarity = np.mean(list(final_similarity.values()))
        # uhhhm... let's aggregate the metrics into a single-row dataframe...
        centroid_distribution_df = pd.DataFrame()
        data = {}
        data["worst_to_best_files"] = worst_to_best_files
        data["worst_to_best_similarity"] = worst_to_best_similarity
        data["average_similarity"] = average_similarity
        data["cluster_similarities"] = cluster_similarities_list
        centroid_distribution_df = centroid_distribution_df.append(
            data, ignore_index=True
        )
        # persist the df in the model folder
        # but let's also hash the dates on which the similarity was calculated - even though it's visible within the file...
        dates_hash = utils.get_hash(all_files_names)
        centroid_distribution_df.to_csv(
            f"{model_dir}/centroid_similarity_{dates_hash}.csv"
        )
        return True

    def extract_single_cluster_distributions(self, model_dir, eval_dates):

        # to analyze the resuls of the grid search, we need to perform a couple of calculations and aggregations
        # let's start by defining a method to go through prediction results and extract distributions of clusters
        # by population for the distinct prediction files
        # first thing we extract the full paths and the corresponding dates for the eval files
        pred_files = [
            item
            for item in os.listdir(model_dir)
            if item.split("_")[1].split(".")[0] in eval_dates
        ]

        pred_paths = [f"{model_dir}/{item}" for item in pred_files]

        pred_dates = [item.split("_")[1].split(".")[0] for item in pred_files]

        # let's also add the fit_predictions files to the two lists, with a header - 'fit_data'
        pred_dates.append("fit_data")
        fit_pred_file = f"{model_dir}/fit_data_predictions.csv"
        pred_paths.append(fit_pred_file)

        # now, instantiate an empty dict, which would be filled with the calc results
        cluster_distr = {}
        for filename, date in zip(pred_paths, pred_dates):

            pred_df = pd.read_csv(filename)

            # group by the clusters and get the count
            cluster_counts = pred_df.groupby("cluster").count().iloc[:, 0]
            cluster_counts_list = cluster_counts.tolist()

            # Number of clusters
            # TODO: Not saying this is better necessarily...!
            model_filename_candidates = [
                re.findall(r"\d+", item)[0]
                for item in os.listdir(model_dir)
                if "clusters.sav" in item
            ]

            assert len(model_filename_candidates) == 1
            n_clusters = int(model_filename_candidates[0])

            if len(cluster_counts_list) < n_clusters:
                cluster_indexes_to_update = [
                    item
                    for item in range(n_clusters)
                    if item not in cluster_counts.index.tolist()
                ]
                for index_to_update in cluster_indexes_to_update:
                    cluster_counts_list.insert(index_to_update, 0)

            # get the proportion of each cluster
            cluster_prop = [
                item / sum(cluster_counts_list) for item in cluster_counts_list
            ]

            # populate the dict
            cluster_distr[date] = cluster_prop

        # parse the dict to df and persist
        analysis_df = pd.DataFrame.from_dict(cluster_distr)

        # a key moment here is that we need to know which dates does the distribution apply to
        # so that we can aggregate for the 'current' evaluation later on
        # to do that, let's get the hash of the dates, and add that to the filename
        dates_hash = utils.get_hash(eval_dates)
        analysis_df.to_csv(f"{model_dir}/cluster_distributions_{dates_hash}.csv")

        # ---------- NOTE - deleting evaluation pred files here...
        # NOTE having calculated the distriutions, let's remove the hefty prediction files
        # excl. the fit_prediction, which is last
        for filename in pred_paths[:-1]:
            pathlib.Path.unlink(pathlib.Path(filename))

        return True

    def evaluate_stability(self, params, evaluation_end_dates):

        print(f"Evaluation stability for {params['domains']}.")

        cols_to_trim_hash = utils.get_hash(params["cols_to_trim"])

        # now construct the dir with the parameters passed
        feature_selection_dir = (
            f"{self._root_dir}/models/v_{self._model_version}/{params['domains']}/"
            f"lookback_{params['lookback_period']}/fit_for_{params['end_date']}/"
            f"outliers_{cols_to_trim_hash}/zscore_{params['zscore_cutoff']}"
        )

        # above we have the folder containing all fits we are interested in
        # let's list the folders, and use that filtered list to replce the 'cols_to_keep' parameter
        cols_to_keep_hashes = [
            item.split("_")[-1]
            for item in os.listdir(feature_selection_dir)
            if "selected_features_" in item
        ]

        # let's run a for loop to save dev time...
        print(f"Calculating evaluation metrics at {feature_selection_dir}.")
        for cols_to_keep in cols_to_keep_hashes:

            # first, add the path to the models_dirs
            cluster_selection_dir = (
                f"{feature_selection_dir}/selected_features_{cols_to_keep}"
            )

            # now, we have a number of models here; since we do not pass a parameter to id which ones we want to look at,
            # let's predict with all of them - this should be always the case...
            model_dirs = [
                f"{cluster_selection_dir}/{item}"
                for item in os.listdir(cluster_selection_dir)
                if "_clusters" in item
            ]

            # before we ask for aggregation, though - we should see if this evaluation set hasn't
            # call the two evaluation methods for each of the dirs
            for model_dir in model_dirs:
                # extract temporal cluster distributions - how the proportion of clusters varies across eval_dates
                self.extract_single_cluster_distributions(
                    model_dir, evaluation_end_dates
                )

                # extract centroid similarities - whether fits with same parameters, but fit on different periods
                # have similar centroids - is it worth it?
                self.extract_centroid_similarity(model_dir)

        return True

    def preprocess_and_predict(self, eval_grid_params, preprocessor, modeller):

        print("--------------Pre-processing data.----------------")

        # Pre-process data
        preprocessed_data = preprocessor.preprocess_data(eval_grid_params)

        # now, get a list of combinations of parameters for modelling, which are in the second item in
        # the grid_search_params argument
        model_search_params_dict = {i: self._params[i] for i in ["clusters_centre"]}
        modelling_params_combinations = utils.get_param_combinations(
            model_search_params_dict
        )

        # create a list of params, one for each combination in modelling_params_combinations
        modelling_combinations = [
            {**eval_grid_params, **i} for i in modelling_params_combinations
        ]

        print("--------------Generate predictions.----------------")

        # now the eval_params_bundle is an iterator we can feed into a lambda to evaluate downstream models
        list(
            map(
                lambda x: self.generate_predictions(x, preprocessed_data, modeller),
                modelling_combinations,
            )
        )

        return True

    def generate_predictions(self, params, df, modeller):

        print(f"Making predictions for configuration for {params['domains']}.")

        # create bash
        cols_to_trim_hash = utils.get_hash(params["cols_to_trim"])

        # construct the dir - with parameters for versioning
        feature_selection_dir = (
            f"{self._root_dir}/models/v_{self._model_version}/{params['domains']}/"
            f"lookback_{params['lookback_period']}/fit_for_{params['end_date']}/"
            f"outliers_{cols_to_trim_hash}/"
            f"zscore_{params['zscore_cutoff']}"
        )

        # above we have the folder containing all fits we are interested in
        # let's list the folders, and use that filtered list to replace the 'cols_to_keep' parameter
        cols_to_keep_hashes = [
            item[18:]
            for item in os.listdir(feature_selection_dir)
            if "selected_features_" in item
        ]

        # let's run a for loop to save dev time...
        for cols_to_keep_hash in cols_to_keep_hashes:
            # first, add the path to the models_dirs
            cluster_selection_dir = (
                f"{feature_selection_dir}/selected_features_{cols_to_keep_hash}"
            )

            # now, we have a number of models here; since we do not pass a parameter to id which ones we want to look at,
            # let's predict with all of them - this should be always the case...
            model_dirs = [
                f"{cluster_selection_dir}/{item}"
                for item in os.listdir(cluster_selection_dir)
                if "_clusters" in item
            ]

            # call the predict method for each of the dirs
            for model_dir in model_dirs:
                # before we move on, let's right now check to see if this feature selection has been the result of
                # cutting some features, by inspecting the folder and check
                # to see if a "selection_for_{hash}.csv" file with the hash we are currently working for exists
                # if so - make sure only the features in that file are left in the dataframe
                selection_file = pathlib.Path(
                    f"{feature_selection_dir}/selection_for_{cols_to_keep_hash}.csv"
                )

                if selection_file.is_file():
                    cols_to_keep = pd.read_csv(selection_file)["feature"].tolist()
                    checked_df = df[cols_to_keep]
                else:
                    checked_df = df

                # Sort columns
                checked_df = checked_df[sorted(checked_df.columns)]

                # Predict
                modeller.predict(checked_df, model_dir, params["evaluation_end_date"])

        return True

    def rank_models(
        self,
        root_dir,
        models_version,
        domains,
        evaluation_end_dates,
        id_hash,
    ):

        # first step in ranking our models is to locate all of them
        # we do this easily by generating the hash of the evaluation dates we're currently evaluating for
        # and locating all files with that hash for each domain
        eval_dates_hash = utils.get_hash(evaluation_end_dates)

        for domain in domains:

            # for each domain, instantiate a dataframe...
            agg_df = pd.DataFrame()
            domain_cluster_distribution_filenames = f"{root_dir}/models/v_{models_version}/{domain}/**/cluster_distributions_{eval_dates_hash}.csv"
            listing = glob.glob(domain_cluster_distribution_filenames, recursive=True)

            for filename in listing:
                # read the cluster stability file, and get an average of the cluster stability (temporal variance)
                variance_df = pd.read_csv(filename, index_col=0)

                # n clusters
                n_clusters = variance_df.shape[0]

                # TODO: should this include the final column?
                cluster_variance = np.mean(variance_df.var(axis=1))

                # while we're here, let's also extract the centroid similarity from the already calculted df
                model_dir = filename.split("cluster_distributions")[0]

                centroid_dfs_filenames = [
                    f"{model_dir}{item}"
                    for item in os.listdir(model_dir)
                    if "centroid_similarity_" in item
                ]

                # let's read all centroid similarity files - could be multiple.. and get an average of that for our metric
                centroid_similarities = []
                for centroid_file in centroid_dfs_filenames:
                    df = pd.read_csv(centroid_file, index_col=0)
                    assert df.shape[0] == 1
                    avg_similarity = df.to_dict()["average_similarity"]
                    centroid_similarities.append(avg_similarity[0])
                centroid_similarity = np.mean(centroid_similarities)

                # append the two metrics to the dataframe, alongside the path to the model they are for
                # put results in a separate location - with hash?
                data = {
                    "model_dir": model_dir,
                    "cluster_variance": cluster_variance,
                    "centroid_similarity": centroid_similarity,
                    "n_clusters": n_clusters,
                    "lookback_period": re.split(
                        "/|\\\\", model_dir.split("lookback_")[-1]
                    )[0],
                }
                agg_df = agg_df.append(data, ignore_index=True)

            # for each domain, persist the reporting file with the metrics and eval dates hash?
            # ALSO - get a hash of the FIT dates & imput strategy? - we're evaling models only downstream from those
            stability_dir = f"{root_dir}/evaluation/v_{models_version}/{domain}"
            pathlib.Path(stability_dir).mkdir(parents=True, exist_ok=True)
            stability_filename = (
                f"{stability_dir}/{id_hash}_stability_{eval_dates_hash}.csv"
            )
            print(
                f"Cluster stability and centroid similarity for {domain} calculated and persisted at {stability_filename}."
            )
            agg_df.to_csv(stability_filename)

        # now we have a file in 'eval' for each domain, with the stability metrics for the current evaluation run
        # there is one more metric we need to add - industry_segregation_capacity - it measures how well do our models
        # segregate our userbase by industry - while the models are behavioural, the hypothesis is that professionals from
        # certain industries would behave somewhat similarly on our domains;
        # to measure that capacity, we need to download and process some data from bigquery
        # since that processing is costly, let's first vet our models, so that only a shortlist remain
        # first, let's select the 5 best by stability
        shortlist_models = []
        for domain in domains:
            # TODO: i guess this could be part of the above for loop to avoid re-reading in file?
            stability_file = pd.read_csv(
                f"{root_dir}/evaluation/v_{models_version}/{domain}/{id_hash}_stability_{eval_dates_hash}.csv",
                index_col=0,
            )

            # as it's coming first - this metric is most important
            # to inject a bit of variance in the downstream model zoo, let's pick the 2 best models for each n_clust
            stability_file.sort_values("cluster_variance", inplace=True)
            stability_shortlist = (
                stability_file.groupby(["n_clusters", "lookback_period"]).head(5).copy()
            )

            # from the stability shortlist, let's pick the most 'similar by centroid' model for each n_clusters
            # but first - let's replace the 1s for similarity (meaning there was no model to compare to, hence useless)
            stability_shortlist.loc[
                stability_shortlist["centroid_similarity"] > 0.9999,
                "centroid_similarity",
            ] = np.mean(stability_shortlist["centroid_similarity"])

            stability_shortlist.sort_values(
                "centroid_similarity", ascending=False, inplace=True
            )

            similarity_shortlist = stability_shortlist.groupby(
                ["n_clusters", "lookback_period"]
            ).head(3)

            similarity_shortlist_dict = {
                "domain": domain,
                "similarity_shortlist": similarity_shortlist,
            }

            # now we've got the 'most stable' model from each n_clusters configuration
            shortlist_models.append(similarity_shortlist_dict)

        # as there might be a lot of domains, let's use lambda, where our iterator is a list of the domains we evaluate
        # lambda > for loop since we're calling bigquery data in
        seg_scores_industry = list(
            map(
                lambda x: self.evaluate_segregation(x, self.score_by_industry),
                shortlist_models,
            )
        )
        seg_scores_jobs = list(
            map(
                lambda x: self.evaluate_segregation(x, self.score_by_job),
                shortlist_models,
            )
        )

        seg_scores_seniority = list(
            map(
                lambda x: self.evaluate_segregation(x, self.score_by_seniority),
                shortlist_models,
            )
        )

        seg_scores_ceos = list(
            map(
                lambda x: self.evaluate_segregation(x, self.score_by_ceos),
                shortlist_models,
            )
        )

        seg_ranking_score = self.score_by_ranking(
            seg_scores_industry, seg_scores_jobs, seg_scores_ceos, seg_scores_seniority
        )

        # the above contains a list (one for each domain) of lists (one for each model scored)
        # let's append these scores to the respective dataframes and models, and persist the aggregated
        # shortlist_evaluation dfs to disk
        for (
            shortlist_dict,
            seg_scores_industry,
            seg_scores_jobs,
            seg_scores_seniority,
            seg_scores_ceos,
            seg_ranking_score,
        ) in zip(
            shortlist_models,
            seg_scores_industry,
            seg_scores_jobs,
            seg_scores_seniority,
            seg_scores_ceos,
            seg_ranking_score,
        ):

            scores_series_industry = pd.Series(
                seg_scores_industry,
                index=shortlist_dict["similarity_shortlist"].index,
                name="weighted_profiling_score_industry",
            )
            scores_series_jobs = pd.Series(
                seg_scores_jobs,
                index=shortlist_dict["similarity_shortlist"].index,
                name="weighted_profiling_score_jobs",
            )
            scores_series_seniority = pd.Series(
                seg_scores_seniority,
                index=shortlist_dict["similarity_shortlist"].index,
                name="weighted_profiling_score_seniority",
            )
            scores_series_ceos = pd.Series(
                seg_scores_ceos,
                index=shortlist_dict["similarity_shortlist"].index,
                name="c_level_projection",
            )
            scores_series_ranking = pd.Series(
                seg_ranking_score,
                index=shortlist_dict["similarity_shortlist"].index,
                name="ranking_score",
            )

            shortlist_df_industry = pd.merge(
                shortlist_dict["similarity_shortlist"],
                scores_series_industry,
                how="left",
                left_index=True,
                right_index=True,
            )

            shortlist_df_jobs = pd.merge(
                shortlist_df_industry,
                scores_series_jobs,
                how="left",
                left_index=True,
                right_index=True,
            )

            shortlist_df_ceos = pd.merge(
                shortlist_df_jobs,
                scores_series_ceos,
                how="left",
                left_index=True,
                right_index=True,
            )

            shortlist_df_seniority = pd.merge(
                shortlist_df_ceos,
                scores_series_seniority,
                how="left",
                left_index=True,
                right_index=True,
            )

            shortlist_df = pd.merge(
                shortlist_df_seniority,
                scores_series_ranking,
                how="left",
                left_index=True,
                right_index=True,
            )

            # persist the file
            shortlist_dir = (
                f"{root_dir}/evaluation/v_{models_version}/{shortlist_dict['domain']}"
            )
            pathlib.Path(shortlist_dir).mkdir(parents=True, exist_ok=True)
            shortlist_filename = (
                f"{shortlist_dir}/{id_hash}_shortlist_{eval_dates_hash}.csv"
            )

            print(
                f"Profiling scores for shortlisted models for {shortlist_dict['domain']} "
                f"calculated and persisted at {stability_filename}."
            )

            shortlist_df.to_csv(shortlist_filename)

        return True

    @staticmethod
    def evaluate_segregation(models_list, func):
        # what needs to happen is that we need to
        # 1) download clearbit data for the end day of the period for which the model was fit
        # 2) for each user that we have a prediction for in the 'fit_data_predictions.csv', get their company and industry
        # 3) group users by cluster and by industry, industryGroup, subIndustry, sector
        # 4) calculate weight ratio per cluster
        # 5) do a weighted average of the weight ratio - weighing by cluster_total?
        # 6) write the weighted average of the weight ratio, alongside the other two metrics, to file
        # first step is to get the domain, and a list of the end_dates the models were fit for

        domain = models_list["domain"]
        model_dirs = models_list["similarity_shortlist"]["model_dir"].tolist()

        end_dates = [
            i.replace("fit_for_", "")
            for j in model_dirs
            for i in re.split("/|\\\\", j)
            if "fit_for_" in i
        ]

        lookback_periods = [
            i.replace("lookback_", "")
            for j in model_dirs
            for i in re.split("/|\\\\", j)
            if "lookback_" in i
        ]

        # finally, get a list of the fit_data_predictions.csv for the models
        pred_dfs = [
            pd.read_csv(f"{item}/fit_data_predictions.csv", index_col=0)
            for item in model_dirs
        ]

        date_and_predictions = [
            (end_date, lookback_period, predictions)
            for end_date, lookback_period, predictions in zip(
                end_dates, lookback_periods, pred_dfs
            )
        ]

        segregation_scores = list(
            map(
                lambda x: func(x, domain, transform_function=utils.transform_function),
                zip(date_and_predictions, model_dirs),
            )
        )

        # having calculated the segregation scores for all shortlisted models, let's return those to the ranking method where
        # we will persist them in the eval folder for the respective domain
        return segregation_scores

    @staticmethod
    def score_by_ranking(
        seg_scores_industry, seg_scores_jobs, seg_scores_ceos, seg_scores_seniority
    ):
        list_scores = np.array(
            [
                seg_scores_industry,
                seg_scores_jobs,
                seg_scores_ceos,
                seg_scores_seniority,
            ]
        )
        weights = np.array([0.4, 0.3, 0.2, 0.1])
        weighted_scores = [
            sum(x) for x in zip(*[a * b for a, b in zip(weights, list_scores)])
        ]
        return weighted_scores

    def score_by_job(
        self,
        dates_preds_dir,
        domain,
        transform_function=None,
    ):

        # Download data
        # send our iterator to a lambda, collecting responses
        # the model_type here is also the dimension we use for profiling...
        jobs_downloader = ClusterDownloader(
            model_type="jobs", data_version=self._data_version
        )
        dates_and_predictions = dates_preds_dir[0]
        profile_df = jobs_downloader.download_jobs_data(
            dates_and_predictions=dates_and_predictions,
            domain=domain,
        )

        model_dir = dates_preds_dir[1]
        # we need to persist the industry profile in the model's home dir, for future reference...
        profile_df.to_csv(f"{model_dir}/jobs_profiles_fit.csv")

        # Weighted mean
        x = profile_df["weight_ratio"] - 1

        if transform_function:
            w = transform_function(profile_df["jobs_total"])
        else:
            w = profile_df["jobs_total"]

        # Weighted mean
        weighted_score = np.sum(x * w) / np.sum(w)

        return weighted_score

    def score_by_seniority(
        self,
        dates_preds_dir,
        domain,
        transform_function=None,
    ):

        # Download data
        # send our iterator to a lambda, collecting responses
        # the model_type here is also the dimension we use for profiling...
        jobs_downloader = ClusterDownloader(
            model_type="seniority", data_version=self._data_version
        )
        dates_and_predictions = dates_preds_dir[0]
        profile_df = jobs_downloader.download_seniority_data(
            dates_and_predictions=dates_and_predictions,
            domain=domain,
        )

        model_dir = dates_preds_dir[1]
        # we need to persist the industry profile in the model's home dir, for future reference...
        profile_df.to_csv(f"{model_dir}/seniority_profiles_fit.csv")

        # Weighted mean
        x = profile_df["weight_ratio"] - 1

        if transform_function:
            w = transform_function(profile_df["seniority_total"])
        else:
            w = profile_df["seniority_total"]

        # Weighted mean
        weighted_score = np.sum(x * w) / np.sum(w)

        return weighted_score

    def score_by_ceos(
        self,
        dates_preds_dir,
        domain,
        transform_function=None,
    ):

        # Download data
        # send our iterator to a lambda, collecting responses
        # the model_type here is also the dimension we use for profiling...
        jobs_downloader = ClusterDownloader(
            model_type="seniority", data_version=self._data_version
        )
        dates_and_predictions = dates_preds_dir[0]
        profile_df = jobs_downloader.download_c_level_data(
            dates_and_predictions=dates_and_predictions,
            domain=domain,
        )

        model_dir = dates_preds_dir[1]
        # we need to persist the industry profile in the model's home dir, for future reference...
        profile_df.to_csv(f"{model_dir}/c_level_score.csv")

        return profile_df.iloc[0, 0]

    def score_by_industry(
        self,
        dates_preds_dir,
        domain,
        transform_function=None,
    ):

        # Download data
        # send our iterator to a lambda, collecting responses
        # the model_type here is also the dimension we use for profiling...
        industry_downloader = ClusterDownloader(
            model_type="industry", data_version=self._data_version
        )
        dates_and_predictions = dates_preds_dir[0]
        profile_df = industry_downloader.download_industry_data(
            dates_and_predictions=dates_and_predictions,
            domain=domain,
        )

        model_dir = dates_preds_dir[1]
        # we need to persist the industry profile in the model's home dir, for future reference...
        profile_df.to_csv(f"{model_dir}/industry_profiles_fit.csv")

        # Weighted mean
        x = profile_df["weight_ratio"] - 1

        if transform_function:
            w = transform_function(profile_df["industry_total"])
        else:
            w = profile_df["industry_total"]

        # Weighted mean
        weighted_score = np.sum(x * w) / np.sum(w)

        return weighted_score

    def evaluate_grid(self, evaluation_end_dates):

        # Clean domains
        self._params["domains"] = [
            utils.clean_domains(i) for i in self._params["domains"]
        ]

        n_combinations = utils.overall_combinations(
            {**self._params, **{"evaluation_end_date": evaluation_end_dates}}
        )

        print(
            f"---------Performing a grid search EVALUATION over {n_combinations} predictions.------------"
        )

        print("--------------Downloading data.----------------")

        # Create download params dict
        download_params = {
            "lookback_period": self._params["lookback_period"],
            "evaluation_end_date": evaluation_end_dates,
        }

        # Get download params combinations
        dl_params_combinations = utils.get_param_combinations(download_params)

        # Init downloader
        evaluation_data_downloader = ClusterDownloader(
            model_type="evaluation", data_version=self._data_version
        )

        # for each combination of the download parameters, perform the downloading
        list(
            map(
                lambda x: evaluation_data_downloader.get_files(
                    x, self._params["domains"]
                ),
                dl_params_combinations,
            )
        )

        print("--------------Generate search combinations.----------------")

        # having downloaded the data we need efficiently, let's start evaluating the models
        # now create combinations of parameters for the preprocessing
        # having downloaded the data we need efficiently, let's start fitting the models
        # now create combinations of parameters for the preprocessing
        preprocess_combinations_dict = {
            i: self._params[i]
            for i in [
                "domains",
                "lookback_period",
                "end_date",
                "cols_to_trim",
                "zscore_cutoff",
                "cols_to_keep",
            ]
        }

        preprocess_parameters_combinations = utils.get_param_combinations(
            {
                **preprocess_combinations_dict,
                **{"evaluation_end_date": evaluation_end_dates},
            }
        )

        # instantiate the preprocessor, modeller and feature_selector
        preprocessor = Preprocessor(
            model_type="evaluation",
            data_version=self._data_version,
            model_version=self._model_version,
        )
        modeller = Modeller(
            model_type="evaluation",
            data_version=self._data_version,
            model_version=self._model_version,
        )

        # we continue downstream - mapping the preprocessing to our preprocess combinations iterator
        # within that map, there will be another map to do the fitting with its further combinatorial paramaters
        # that we send as the second part of the lists within 'grid_search_params'
        list(
            map(
                lambda x: self.preprocess_and_predict(x, preprocessor, modeller),
                preprocess_parameters_combinations,
            )
        )

        # once the above has finished - we now should have a full set of predictions for each of our models
        # now, we will evaluate the models, based on these predictions
        # downstream we will be evaluating for ALL cols_to_keep variations,
        # so we should exclude them from the combination here
        params_ex_colstokeep = self._params.copy()
        params_ex_colstokeep.pop("cols_to_keep")
        params_ex_colstokeep.pop("fine_tune")
        model_combinations = utils.get_param_combinations(params_ex_colstokeep)
        list(
            map(
                lambda x: self.evaluate_stability(x, evaluation_end_dates),
                model_combinations,
            )
        )

        # having generated the metrics for each of our models, let's rank them by those metrics and persist a report
        # notably, the ranking of models should be earmarked by a few parameters
        id_hash = utils.get_hash(self._params)

        # finally, let's also instantiate the downloader credentials to pass for ranking
        self.rank_models(
            config["root_dir"],
            self._model_version,
            self._params["domains"],
            evaluation_end_dates,
            id_hash,
        )

        return True
