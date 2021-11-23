from segmentation.downloader.probability_model_downloader import ProbDownloader
from segmentation.config.load_config import load_config
import pathlib
import pandas as pd

# Config
config = load_config()


class ProbModel:
    """
    The `ProbModel()` class is for generating predictions for cluster attributes

    Args:
        params (dict): model parameters
        data_version (int): data version
        model_version (int): model version

    """

    # initialize the class
    def __init__(self, params: dict, data_version: int, model_version: int):
        """Initialise"""
        self._root_dir = config["root_dir"]
        self._params = params
        self._data_version = data_version
        self._model_version = model_version

    # Calculate probabilities method
    def calculate_probabilities(self, df: pd.DataFrame, attribute: str) -> pd.DataFrame:
        """
        Calculates probabilities for each attribute for each cluster based on user counts and priors

        Args:
            df (pd.DataFrame): dataframe of counts
            attribute (str): which attribute

        Returns:
            pd.DataFrame: The return value dataframe

        """

        # Sort
        df = df.sort_values(by=["attribute", "cluster"])

        # Drop duplicates
        df = df.drop_duplicates(
            ignore_index=True,
            subset=["cluster", "attribute", "user_count", "cluster_total", "data_date"],
        )

        print(f"----New rows: {df.shape[0]}")

        # Expand to include all combos and fill missing with zeros
        cluster_totals = df[["cluster", "cluster_total", "data_date"]].drop_duplicates()
        df = (
            df.set_index(["cluster", "attribute"])["user_count"]
            .unstack(fill_value=0)
            .stack()
            .reset_index(name="user_count")
        )

        df = df.merge(cluster_totals, how="left", on=["cluster"])

        # Add attribute count totals
        df["user_count_total"] = df.groupby("attribute")["user_count"].transform("sum")

        # Get alphas
        alphas = config[f"{attribute}_alphas"]
        alphas = {i: [v] for i, v in alphas.items()}

        # Join alphas
        priors_df = pd.DataFrame.from_dict(alphas, orient="index").reset_index()
        priors_df.columns = ["attribute", "alpha"]
        df = df.merge(priors_df)

        # Get probabilities
        df["alpha_prime"] = (
            df["user_count"]
            + (df["user_count_total"] + df["alpha"]) * config[f"{attribute}_theta"]
        )
        df["alpha_prime_sum"] = df.groupby("cluster")["alpha_prime"].transform("sum")
        df["probability"] = df["alpha_prime"] / df["alpha_prime_sum"]

        return df

    # Compute method
    def compute(self) -> None:
        """
        Downloads data, calculates attribute probabilities, saves to BigQuery

        Example:

        Here is some example code:

        .. code-block:: python

            from segmentation.probabilistic_model import probabilistic_model

            # Versions
            data_version = 1
            model_version = 1

            # Parameters
            params = {
                "attribute": "industry",
                "domain": "energymonitor",
            }

            prob_model = probabilistic_model.ProbModel(params, data_version, model_version)
            prob_model.compute()

        """

        # Get data
        profile_downloader = ProbDownloader(
            model_type="probabilistic", data_version=self._data_version
        )

        print("----Getting profile data.----")

        profile_df, domain = profile_downloader.download_attribute_profiles(
            self._params["domain"], self._params["attribute"]
        )

        print(f"----{profile_df.shape[0]} rows; {profile_df.shape[1]} columns----")
        print("----Calculating probabilities.----")

        # Calculate probabilities
        probability_df = self.calculate_probabilities(
            profile_df, self._params["attribute"]
        )

        # Persist
        print("----Save.----")

        # Local save
        domain_dir = (
            f"{config['root_dir']}/probability_tables/v_{self._data_version}/"
            f"{domain}/{self._params['attribute']}"
        )
        pathlib.Path(domain_dir).mkdir(parents=True, exist_ok=True)
        data_filename = pathlib.Path(
            domain_dir + "/cluster_attribute_probability_table.csv"
        )
        probability_df.to_csv(data_filename, index=False)

        # Push to BQ
        profile_downloader.upload_attribute_probabilities(
            probability_df,
            domain,
            self._params["attribute"],
        )
