import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.impute import SimpleImputer
from pickle import dump, load
from scipy import stats
import pathlib
import segmentation.cluster_model.utils as utils
from segmentation.config.load_config import load_config

# Config
config = load_config()


class Preprocessor:
    # initialize the class
    def __init__(self, model_type, data_version, model_version):
        self._root_dir = config["root_dir"]
        self._model_type = model_type
        self._data_version = data_version
        self._model_version = model_version

    @staticmethod
    def load_and_clean(root_dir, data_version, domain, lookback_period, end_date):
        df = pd.read_csv(
            f"{root_dir}/training_data/v_{data_version}/{domain}/lookback_{lookback_period}/{end_date}.csv",
            index_col="user_id",
        )
        # let's drop the segments here and now... collinearity and insecurity there...
        df = df.drop(["Unnamed: 0", "segments"], axis=1)
        return df

    def impute_missing_data(self, df, params):

        # construct the dir - with parameters for versioning
        imputation_dir = (
            f"{self._root_dir}/models/v_{self._model_version}/{params['domains']}/"
            f"lookback_{params['lookback_period']}/fit_for_{params['end_date']}"
        )
        pathlib.Path(imputation_dir).mkdir(parents=True, exist_ok=True)
        imputer_filename = pathlib.Path(imputation_dir + "/imputer.pkl")

        if imputer_filename.is_file():
            imputer = load(open(imputer_filename, "rb"))
        else:
            imputer = SimpleImputer(strategy="median")
            imputer.fit(df)
            # after it's fit, save it so it can be re-used later
            dump(imputer, open(imputer_filename, "wb"))

        # report which cols will have data imputed, and how many rows
        nan_cols = [i for i in df.columns if df[i].isnull().any()]
        nan_rows = df[df.isnull().any(axis=1)].shape[0]
        print(
            f"Imputing for columns: {nan_cols}, across a total of {nan_rows} rows, out of {df.shape[0]} rows."
        )

        imputed_array = imputer.transform(df)
        imputed_df = pd.DataFrame(
            data=imputed_array, index=df.index, columns=df.columns
        )
        return imputed_df

    # kmeans is sensitive to outliers, so we should discard outliers from our training set
    # TODO: is there a scikit mod for this?
    # TODO: this seems pretty heavy for sessions and views, and probably not harsh enough for dwell
    def cut_outliers(self, df, params):
        # to construct the new dir, as we might add more columns to cut outliers along, let's hash the list of columns
        cols_to_trim_hash = utils.get_hash(params["cols_to_trim"])

        # construct the dir - with parameters for versioning
        outliers_dir = (
            f"{self._root_dir}/models/v_{self._model_version}/{params['domains']}/"
            f"lookback_{params['lookback_period']}/fit_for_{params['end_date']}/"
            f"outliers_{cols_to_trim_hash}/zscore_{params['zscore_cutoff']}"
        )

        pathlib.Path(outliers_dir).mkdir(parents=True, exist_ok=True)

        # to avoid view vs copy randomness...
        no_outliers_df = df.copy()

        # for each feature we want to remove outliers for...
        for col in params["cols_to_trim"]:

            new_col_name = "Z_" + col

            # create a new column with the zscore of the values of the feature
            no_outliers_df[new_col_name] = abs(
                stats.zscore(no_outliers_df[col], nan_policy="omit")
            )

            # report the number to be cut
            df_z_over = [
                item
                for item in no_outliers_df[new_col_name].tolist()
                if item >= params["zscore_cutoff"]
            ]

            print(
                f"Users with '{col}' zscore over {params['zscore_cutoff']}: {str(len(df_z_over))}"
            )

            # cut the outliers from the dataframe
            no_outliers_df = no_outliers_df[
                no_outliers_df[new_col_name] < params["zscore_cutoff"]
            ]

            # drop the helper column
            no_outliers_df = no_outliers_df.drop([new_col_name], axis=1)

        return no_outliers_df

    # the second-to-last step is to drop some features (that are found to give the least impact?)
    def select_features(self, df, params):
        # to construct the new dir, let's hash the list of features that we've determined to keep, to use
        # as id for this parametrization; sort lists
        cols_to_keep_hash = utils.get_hash(params["cols_to_keep"])

        # in order to id which outliers we've cut, let's also get the hash of the 'cols_to_trim' list as string
        cols_to_trim_hash = utils.get_hash(params["cols_to_trim"])

        # construct the dir - with parameters for versioning
        feature_selection_dir = (
            f"{self._root_dir}/models/v_{self._model_version}/{params['domains']}/lookback_{params['lookback_period']}/fit_for_{params['end_date']}/"
            f"outliers_{cols_to_trim_hash}/zscore_{params['zscore_cutoff']}"
        )
        selected_features_dir = (
            f"{feature_selection_dir}/selected_features_{cols_to_keep_hash}"
        )

        pathlib.Path(selected_features_dir).mkdir(parents=True, exist_ok=True)

        #  drop the features we've determined not to use
        if params["cols_to_keep"][0] == "All":
            cols_to_keep = df.columns.tolist()
        else:
            cols_to_keep = params["cols_to_keep"]
            # create a csv indicating the selection, if one doesn't exist
            selection_filename = pathlib.Path(
                f"{feature_selection_dir}/selection_for_{cols_to_keep_hash}.csv"
            )
            if not selection_filename.is_file():
                hardcoded_features_importances = {}
                for item in cols_to_keep:
                    hardcoded_features_importances[item] = "hardcoded"
                selection_df = pd.DataFrame.from_dict(
                    hardcoded_features_importances, orient="index"
                )
                selection_df.reset_index(inplace=True)
                selection_df.columns = ["feature", "average_importance"]
                selection_df.to_csv(selection_filename)

        df = df[cols_to_keep]

        return df.reindex(sorted(df.columns), axis=1)

    # the last step we have to perform is to scale the data so that all values are between 0 and 1
    def scale_data(self, df, params):

        # let's get the hashes of the 'cols_to_trim' and the 'cols_to_keep' parameters to id the model version; sort lists
        cols_to_keep_hash = utils.get_hash(params["cols_to_keep"])
        cols_to_trim_hash = utils.get_hash(params["cols_to_trim"])

        # construct the dir - with parameters for versioning
        scaler_dir = (
            f"{self._root_dir}/models/v_{self._model_version}/{params['domains']}/"
            f"lookback_{params['lookback_period']}/fit_for_{params['end_date']}"
            f"/outliers_{cols_to_trim_hash}/zscore_{params['zscore_cutoff']}/"
            f"selected_features_{cols_to_keep_hash}"
        )

        # check if a fit scaler already exists, and if so, use it
        scaler_filename = pathlib.Path(scaler_dir + "/scaler.pkl")

        if scaler_filename.is_file():
            scaler = load(open(scaler_filename, "rb"))
        else:
            # if not, then we fit the scaler utility - which forces all values to between 0 and 1
            # TODO: look at standardisation?
            scaler = MinMaxScaler()
            scaler.fit(df)

            # after it's fit, save it so it can be re-used later
            dump(scaler, open(scaler_filename, "wb"))

        # now transform the data
        scaled_array = scaler.transform(df)

        # this returns a numpy array; let's get it into a form we can read
        scaled_df = pd.DataFrame(data=scaled_array, index=df.index, columns=df.columns)

        return scaled_df

    def preprocess_data(self, params):

        # print a statement
        print(f"Preprocessing data for {params['domains']}.")

        # If we're evaluating use end dates
        if self._model_type == "evaluation":
            dates_key = "evaluation_end_date"
        else:
            dates_key = "end_date"

        # run the preprocessing steps
        df = self.load_and_clean(
            self._root_dir,
            self._data_version,
            params["domains"],
            params["lookback_period"],
            params[dates_key],
        )

        # no need to pass on eval date downstream - having loaded the data once, downstream components need to only take that up
        # and do the same thing they would do regardless if its training or infering - see if artefact exists (when evaluating it should)
        # and load and do processing (if fitting - some artefacts would not exist)
        imputed_df = self.impute_missing_data(df, params)

        # we should cut outliers ONLY WHEN fitting; otherwise (when we have 'evaluation_end_date', we should not
        if self._model_type == "training":
            no_outliers_df = self.cut_outliers(imputed_df, params)
        else:
            no_outliers_df = imputed_df.copy()

        # Select features
        selected_df = self.select_features(no_outliers_df, params)

        # Scale data
        scaled_df = self.scale_data(selected_df, params)

        return scaled_df
