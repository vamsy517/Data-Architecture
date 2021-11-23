from segmentation.downloader.probability_model_downloader import ProbDownloader
import segmentation.probabilistic_model.classifier_constants as constants
import pandas as pd
import numpy as np
import re
from datetime import datetime
from segmentation.config.load_config import load_config
import pprint

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, balanced_accuracy_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import RandomizedSearchCV
from sklearn.dummy import DummyClassifier
import json

# Config
config = load_config()


class ClassificationModel:
    """
    The `ClassificationModel()` class is for generating predictions for user attributes

    Args:
        params (dict): model parameters
        data_version (int): data version
        model_version (int): model version

    """

    # initialize the class
    def __init__(self, params: dict, data_version: int, model_version: int):
        """Initialise"""
        self.scaler = StandardScaler()
        self._root_dir = config["root_dir"]
        self._params = params
        self._data_version = data_version
        self._model_version = model_version

    def get_data(
        self,
        downloader: ProbDownloader,
        domain: str,
        attribute: str,
        end_date: str,
        lookback_period: int,
    ):
        """
        Gets data from BigQuery

        Args:
            downloader (ProbDownloader): downloader class
            domain (str): which domain
            attribute (str): which attribute
            end_date (str): date from which to look back
            lookback_period (int): look back days

         Returns:
            tuple: dataframes with knowns and unknowns, target variable lists and other info

        """

        print(f"----Generating {attribute} table for {domain}.----")

        # Generate table
        df = downloader.get_features_and_attributes(
            domain, attribute, end_date, lookback_period
        )

        if df.shape[0] == 0:
            raise ValueError("No rows returned.")

        print(f"----{df.shape[0]} rows returned.----")

        # Set index to user id
        df.set_index("user_id", inplace=True)

        # Extract recency and frequency
        extra_user_info_df = df[constants.EXTRA_USER_INFO]
        # extra_user_info_df = df[constants.EXTRA_USER_INFO].dropna(
        #     subset=constants.DROP_NA_USER_INFO_SUBSET
        # )

        # Drop rows with missing features
        # TODO: remove missing features values from query
        df = df.dropna(subset=constants.MODEL_FEATURES[attribute])

        print(f"----{df.shape[0]} rows with complete feature sets.----")

        # Split into knowns and unknowns
        knowns = ~df[constants.MODEL_TARGET[attribute]].isna()
        df_knowns = df[knowns]
        df_unknowns = df[~knowns]

        print(f"----{df_knowns.shape[0]} rows of knowns.----")
        print(f"----{df_unknowns.shape[0]} rows of unknowns.----")

        # Get target names
        df_knowns[constants.MODEL_TARGET[attribute]] = df_knowns[
            constants.MODEL_TARGET[attribute]
        ] = [
            re.sub("__+", "_", re.sub(" |,|&", "_", i.lower()))
            for i in df_knowns[constants.MODEL_TARGET[attribute]]
        ]

        # Remove small categories
        df_knowns = df_knowns.groupby(constants.MODEL_TARGET[attribute]).filter(
            lambda x: len(x) >= constants.MIN_INDUSTRY_N_THRESHOLD
        )
        tgt_names_clean = df_knowns[constants.MODEL_TARGET[attribute]].tolist()
        print(
            f"----{df_knowns.shape[0]} rows of knowns after remove small categories.----"
        )

        # Table of known targets
        known_targets = pd.get_dummies(tgt_names_clean)
        known_targets.index = df_knowns.index

        return (
            df_knowns[constants.MODEL_FEATURES[attribute]],
            df_unknowns[constants.MODEL_FEATURES[attribute]],
            tgt_names_clean,
            known_targets,
            extra_user_info_df,
        )

    def preprocess_data(self, df: pd.DataFrame) -> np.ndarray:
        """
        Scale data, remove outliers

        Args:
            df (pd.DataFrame): dataframe

        Returns:
            np.ndarray: The return value array
        """

        # Scale
        df = self.scaler.fit_transform(df)

        # Outliers etc

        return df

    def grid_search(self, x_df: pd.DataFrame, target: list) -> dict:

        # Number of trees in random forest
        n_estimators = [int(x) for x in np.linspace(start=100, stop=500, num=3)]

        # Number of features to consider at every split
        max_features = ["auto", "sqrt"]

        # Maximum number of levels in tree
        max_depth = [int(x) for x in np.linspace(10, 110, num=3)]

        # Minimum number of samples required to split a node
        min_samples_split = [2, 5, 10]

        # Minimum number of samples required at each leaf node
        min_samples_leaf = [1, 2, 4]

        # Method of selecting samples for training each tree
        bootstrap = [True, False]

        # Create the random grid
        random_grid = {
            "n_estimators": n_estimators,
            "max_features": max_features,
            "max_depth": max_depth,
            "min_samples_split": min_samples_split,
            "min_samples_leaf": min_samples_leaf,
            "bootstrap": bootstrap,
        }
        pprint.pprint(random_grid)

        # Train test split
        X_train, X_test, y_train, y_test = train_test_split(
            x_df, target, stratify=target, test_size=0.3, random_state=42
        )

        # Base model
        rf = RandomForestClassifier(class_weight="balanced")

        # Search
        rf_random = RandomizedSearchCV(
            estimator=rf,
            param_distributions=random_grid,
            n_iter=constants.N_ITERS,
            cv=constants.CV,
            n_jobs=constants.N_JOBS,
        )

        # Fit the random search model #TODO: note doesn't work properly in pycharm debug
        rf_random.fit(X_train, y_train)

        # Performance of best estimator
        best_random = rf_random.best_estimator_
        predictions = best_random.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        print("Accuracy = {:0.2f}%.".format(accuracy * 100))

        bal_accuracy = balanced_accuracy_score(y_test, predictions)
        print("Balanced accuracy = {:0.2f}%.".format(bal_accuracy * 100))

        # Dummy classifier for comparison
        dummy_clf = DummyClassifier(strategy="stratified")
        dummy_clf.fit(X_train, y_train)

        # Predictions
        y_dummy_pred = dummy_clf.predict(X_test)
        dummy_bal_accuracy = balanced_accuracy_score(y_test, y_dummy_pred)

        print(
            f"Relative to dummy classifier balanced accuracy: {bal_accuracy / dummy_bal_accuracy}"
        )

        # Create dictionary of metrics
        # Frequency counts in the test set
        y_test_freqs = pd.Series(y_test).value_counts()

        # Frequency counts in predictions
        y_pred_freqs = pd.Series(predictions).value_counts()

        metrics_dict = {
            "y_test_freqs": y_test_freqs.to_dict(),
            "y_pred_freqs": y_pred_freqs.to_dict(),
            "accuracy": accuracy,
            "balanced_accuracy": bal_accuracy,
            "dummy_balanced_accuracy": dummy_bal_accuracy,
            "relative_to_dummy_balanced_accuracy": bal_accuracy / dummy_bal_accuracy,
            "best_params": rf_random.best_params_,
        }

        # Save
        with open(
            f'{self._root_dir}/models/model_report_dv_{self._data_version}_mv_{self._model_version}_domain_{self._params["domain"]}.json',
            "w",
        ) as fp:
            json.dump(metrics_dict, fp, indent=4)

        return rf_random.best_params_

    def train(self, x_df: np.ndarray, target: list, grid_search=False) -> None:
        """
        Train model

        Args:
            x_df (np.ndarray): training data
            target (list): list of attribute names
            grid_search (bool): whether to perform grid search or not

        """

        if grid_search:
            print("doing grid search")
            grid_search_parameters = self.grid_search(x_df, target)
            self.rf_model = RandomForestClassifier(
                class_weight="balanced", **grid_search_parameters
            )
        else:
            self.rf_model = RandomForestClassifier(class_weight="balanced")

        # Fit model
        self.rf_model.fit(x_df, target)

    def make_predictions(
        self, df_unknowns: pd.DataFrame, probability: bool = True
    ) -> pd.DataFrame:
        """
        Make probability predictions

        Args:
            df_unknowns (pd.DataFrame): training data
            probability (bool): whether to return a binary or probabilistic prediction

        Returns:
            pd.DataFrame: The return value dataframe

        """

        df_pred = self.scaler.transform(df_unknowns)

        if probability:
            tgt_probs = self.rf_model.predict_proba(df_pred)
            output_table = pd.DataFrame(
                tgt_probs, index=df_unknowns.index, columns=self.rf_model.classes_
            )
        else:
            tgt_probs = self.rf_model.predict(df_pred)
            output_table = pd.DataFrame(
                tgt_probs, index=df_unknowns.index, columns=["predicted_class"]
            )
            output_table = output_table["predicted_class"].str.get_dummies()

        return output_table

    def add_extra_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        df["domain"] = self._params["domain"]
        df["end_date"] = self._params["end_date"]
        df["lookback_period"] = self._params["lookback_period"]
        df["timestamp"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        df["end_date"] = pd.to_datetime(df["end_date"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        return df

    def reshape_results(self, df: pd.DataFrame) -> pd.DataFrame:
        id_vars = [
            "user_id",
        ]
        df.reset_index(inplace=True)
        df = df.melt(id_vars=id_vars, var_name="attribute", value_name="probability")

        return df

    def compute(self, grid_search=False, persist_to_bq=True) -> None:
        """
        Make predictions

        Args:
            df_unknowns (pd.DataFrame): training data
            grid_search (bool): whether to perform grid search on model
            persist_to_bq (bool): persist to bq

        Example:

            Here is some example code:

        .. code-block:: python

            from segmentation.probabilistic_model import classification_model

            # Versions
            data_version = 1
            model_version = 1

            # Parameters
            params = {
                "attribute": "industry",
                "domain": "newstatesman.com",
                "end_date": "2021-04-28",
                "lookback_period": 60,
            }

            class_model = classification_model.ClassificationModel(
                params, data_version, model_version
            )
            class_model.compute()

        """

        # Inst downloader / uploader
        downloader = ProbDownloader(
            model_type="classification", data_version=self._data_version
        )

        # Get data
        (
            df_knowns,
            df_unknowns,
            tgt_names_clean,
            known_targets,
            extra_user_info_df,
        ) = self.get_data(
            downloader,
            self._params["domain"],
            self._params["attribute"],
            self._params["end_date"],
            self._params["lookback_period"],
        )

        # Pre-process knowns
        df_knowns = self.preprocess_data(df_knowns)

        # Train
        self.train(df_knowns, tgt_names_clean, grid_search)

        # Predict
        targets_preds = self.make_predictions(
            df_unknowns,
            probability=constants.PROBABILISTIC_PREDICTION[self._params["attribute"]],
        )

        # Reshape
        targets_preds = self.reshape_results(targets_preds)
        known_targets = self.reshape_results(known_targets)

        # Filter knowns
        known_targets = known_targets[known_targets["probability"] == 1]

        # Append knowns
        targets_preds = targets_preds.assign(known=0)
        known_targets = known_targets.assign(known=1)
        targets_preds = pd.concat([targets_preds, known_targets])

        # Add extra columns
        targets_preds = self.add_extra_cols(targets_preds)

        # Join recency and frequency
        targets_preds = (
            targets_preds.set_index("user_id").join(extra_user_info_df).reset_index()
        )

        # Persist
        print("----Save.----")

        # Push to BQ
        if persist_to_bq:
            downloader.upload_user_attribute_probabilities(
                targets_preds, self._params["attribute"]
            )
