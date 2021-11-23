from .features_preprocessor import Preprocessor
from segmentation.downloader.cluster_model_downloader import ClusterDownloader
from .configuration_modeler import Modeller, Feature_Selector
import segmentation.cluster_model.segmentation_constants as constants
import segmentation.cluster_model.utils as utils
from segmentation.config.load_config import load_config

# Config
config = load_config()


class GridSearch:
    # initialize the class
    def __init__(self, params, data_version, model_version):
        self._root_dir = config["root_dir"]
        self._params = params
        self._data_version = data_version
        self._model_version = model_version

    def preprocess_and_fit(
        self, preprocessing_params, preprocessor, modeller, feature_selector
    ):

        print("--------------Pre-processing data.----------------")

        # Pre-process data
        preprocessed_data = preprocessor.preprocess_data(preprocessing_params)

        # now, get a list of combinations of parameters for modelling, which are in the second item in
        # the grid_search_params argument
        model_search_params_dict = {i: self._params[i] for i in ["clusters_centre"]}
        modelling_params_combinations = utils.get_param_combinations(
            model_search_params_dict
        )

        # create a list of params, one for each combination in modelling_params_combinations
        modelling_combinations = [
            {**preprocessing_params, **i} for i in modelling_params_combinations
        ]

        print("--------------Fit models.----------------")

        # now the fitting_parameters_bundle is an iterator we can feed into a lambda to fit models
        list(
            map(
                lambda x: self.fit(
                    preprocessed_data, x, preprocessor, modeller, feature_selector
                ),
                modelling_combinations,
            )
        )

        return True

    @staticmethod
    def fine_tune(n, params, keep_cols, feature_selector, preprocessor, modeller):

        # Get best features
        cols_to_keep_new = feature_selector.extract_best_features(params, keep_cols, n)

        # Update selection parameters
        params["cols_to_keep"] = cols_to_keep_new

        # Pre-process data
        preprocessed_data = preprocessor.preprocess_data(params)

        # Fit base model
        modeller.model_configuration(preprocessed_data, params)

    def fit(self, df, params, preprocessor, modeller, feature_selector):

        # Fit base model
        modeller.model_configuration(df, params)

        # Get keep columns
        list_of_features = constants.LIST_OF_MUSTHAVE_FEATURES

        if self._params["fine_tune"][0]:

            print("--------------Fine tuning model.----------------")

            list(
                map(
                    lambda x: self.fine_tune(
                        x,
                        params,
                        list_of_features,
                        feature_selector,
                        preprocessor,
                        modeller,
                    ),
                    self._params["fine_tune"],
                )
            )

        return True

    def perform_grid_search(
        self,
    ):

        # Clean domains
        self._params["domains"] = [
            utils.clean_domains(i) for i in self._params["domains"]
        ]

        # List of dicts of grid search parameter combinations
        n_combinations = utils.overall_combinations(self._params)

        print(
            f"--------------Performing a grid search over {n_combinations} combinations.----------------"
        )

        print("--------------Downloading data.----------------")

        # we do not use the above elsewhere
        # now, get a list of the DOWNLOAD parameters ONLY =- EXCLUDING THE DOMAINS
        # it is vastly more io-dwh-efficient to split the download from downstream ops
        download_params = {
            "lookback_period": self._params["lookback_period"],
            "end_date": self._params["end_date"],
        }

        dl_params_combinations = utils.get_param_combinations(download_params)

        training_data_downloader = ClusterDownloader(
            model_type="training", data_version=self._data_version
        )

        # for each combination of the download parameters, perform the downloading
        list(
            map(
                lambda x: training_data_downloader.get_files(
                    x, self._params["domains"]
                ),
                dl_params_combinations,
            )
        )

        print("--------------Generating search combinations.----------------")

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
            preprocess_combinations_dict
        )

        # instantiate the preprocessor, modeller and feature_selector
        preprocessor = Preprocessor(
            model_type="training",
            data_version=self._data_version,
            model_version=self._model_version,
        )
        modeller = Modeller(
            model_type="training",
            data_version=self._data_version,
            model_version=self._model_version,
        )
        feature_selector = Feature_Selector(
            model_type="training",
            data_version=self._data_version,
            model_version=self._model_version,
        )

        # we continue downstream - mapping the preprocessing to our preprocess combinations iterator
        # within that map, there will be another map to do the fitting with its further combinatorial paramaters
        # that we send as the second part of the lists within 'grid_search_params'
        list(
            map(
                lambda x: self.preprocess_and_fit(
                    x, preprocessor, modeller, feature_selector
                ),
                preprocess_parameters_combinations,
            )
        )

        print("--------------Complete.----------------")

        return True
