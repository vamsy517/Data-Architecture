from pickle import load
from sklearn.metrics import confusion_matrix, classification_report
from segmentation_report_engine.utils.utils import *
from segmentation_report_engine.utils.constants_queries import *
from ingest_utils.storage_gcp import download_file_from_storage
import pathlib


class RFEvaluationDomain:
    """
    Class, which performs RF evaluation for specific domain
    """
    
    def __init__(self, encoder_file: str, model_name: str, domain: str, dataframe: pd.DataFrame, date: str):
        """
        Initialize class
        :param encoder_file: path string to the encoder file
        :param model_name: path string to the model
        :param domain: specific domain, for which to perform RF evaluation
        :param dataframe: dataframe with test evaluation data
        :param date: date, for which to perform RF evaluation
        """
        self.domain = domain
        self.test_data = dataframe
        self.date = date
        self.encoder_file = encoder_file
        self.model_name = model_name
        self.test_y, self.test_x = self.preprocess_data()
        self.scoring_df, self.test_preds = self.classification_report_df()
        self.conf_mat_df = self.confusion_matrix_df()

    def preprocess_data(self) -> (pd.DataFrame, pd.DataFrame):
        """
        Preprocess input data to transform it to a state to be used by RF
        :return: DataFrames with test labels and test data
        """
        print(self.domain)

        df_test = self.test_data.drop(['rn', 'type'], axis=1)
        print(df_test.shape)
        # preprocess the 'time' field - it should contain only the 'hour' - 0:24
        df_test['hour_of_day_utc'] = df_test.time.dt.hour
        df_test['local_hour_proxy'] = df_test.apply(lambda x: localize_hour(x['hour_of_day_utc'], x['continent']), axis=1)
        # drop the original and helper column
        df_test = df_test.drop(['time', 'hour_of_day_utc'], axis=1)
        df_test = df_test.fillna('other')
        df_test['referrer_group'] = df_test.apply(lambda x: extract_referrer_group(x['referrer'], self.domain), axis=1)
        # drop the original column
        df_test = df_test.drop(['referrer'], axis=1)
        df_test['os_group'] = df_test.apply(lambda x: extract_os_group(x['user_agent']), axis=1)
        # drop the original column
        df_test = df_test.drop(['user_agent'], axis=1)
        df_test = df_test.drop(['url'], axis=1)
        df_test = df_test.set_index('user_id')
        # let's evaluate the fit
        encoder_loaded = load(open(self.encoder_file, 'rb'))
        categorical_features_df_test = df_test[['domain', 'continent', 'referrer_group', 'os_group']]
        ohe_array_test = encoder_loaded.transform(categorical_features_df_test)
        ohe_df_test = pd.DataFrame(ohe_array_test,
                                   index=categorical_features_df_test.index, columns=encoder_loaded.get_feature_names())
        # finally, let's add the two other columns to this df to have a complete, good-to-go df
        df_test_enc = ohe_df_test.join(df_test[['local_hour_proxy', 'cluster']])
        test_y, test_x = df_test_enc['cluster'], df_test_enc.loc[:, df_test_enc.columns != 'cluster']
        print(f'Test set size: {test_x.shape}, targets: {test_y.shape}')
        return test_y, test_x

    def classification_report_df(self) -> (pd.DataFrame, pd.DataFrame):
        """
        Perform classification report
        :return: dataframes with scores and predictions
        """
        clf_loaded = load(open(self.model_name, 'rb'))
        test_preds = clf_loaded.predict(self.test_x)
        report = classification_report(self.test_y, test_preds, output_dict=True)
        scoring_df = pd.DataFrame(report)
        return scoring_df, test_preds

    def confusion_matrix_df(self) -> pd.DataFrame:
        """
        Generate confusion matrix
        :return: confusion matrix dataframe
        """
        conf_mat = confusion_matrix(self.test_y, self.test_preds)
        conf_mat_norm = conf_mat.astype('float') / conf_mat.sum(axis=1)[:, np.newaxis]
        conf_mat_df = pd.DataFrame(conf_mat_norm)
        conf_mat_df = conf_mat_df.round(2)
        return conf_mat_df


class RFEvaluationReport:
    """
    Class, which generates the RF evaluation report for all domains
    """
    
    def __init__(self, domains_dict: dict, data: pd.DataFrame):
        """
        Initialize class
        :param domains_dict: dictionary of domains, on which to perform RF evaluation
        :param data: dataframe, containing all data, needed to perform RF evaluation
        """
        self.dates = DATES
        self.domains_dict = domains_dict
        self.data = data
        self.random_state = RANDOM_STATE
        
    def perform_analysis(self) -> pd.DataFrame:
        """
        Perform analysis on performance of Random Forest on all domains on specific dates
        :return: table with evaluation (precision, recall, f1-score, support,
                                   most_fn_to, most_fn_to_percentage,
                                   most_fp_from, most_fp_from_percentage)
        """
        # create empty dataframe to store results
        analysis_final_df = pd.DataFrame()
        # for each domain in predefined list of domains
        for domain_key, domain_value in self.domains_dict.items():
            # for each date in predefined list of dates
            for date in self.dates:
                # keep only the data, related to the specific domain
                dataframe = self.data[self.data.domain.isin(extend_domain(self.domains_dict, domain_key))]
                if dataframe.shape[0] == 0:
                    continue
                # get model_name and encoder file for specific domain
                domain = domain_value[0]
                # set the directory, where we will download the files
                segmentation_dir = f'{SEGMENTATION_FOLDER}'
                # create the directory, where we will download the files
                pathlib.Path(segmentation_dir + domain).mkdir(parents=True, exist_ok=True)
                # download model
                download_file_from_storage(PULLDATA_PROJECT_ID,
                                           f'{domain}/rf_classifier.sav',
                                           'nsmg-segmentation-2',
                                           segmentation_dir)
                # set model name
                model_name = f'{segmentation_dir}{domain}/rf_classifier.sav'
                # download encoder
                download_file_from_storage(PULLDATA_PROJECT_ID,
                                           f'{domain}/rf_ohe_encoder.pkl',
                                           'nsmg-segmentation-2',
                                           segmentation_dir)
                # set encoder file name
                encoder_file = f'{segmentation_dir}{domain}/rf_ohe_encoder.pkl'
                # create obj, containing the analysis for the specific domain
                domain_obj = RFEvaluationDomain(encoder_file, model_name, domain, dataframe, date)
                # for each cluster in predefined number of clusters
                for cluster in range(domain_obj.conf_mat_df.shape[0]):
                    # create empty dataframe to add key metrics
                    analysis_df = pd.DataFrame()
                    # add column date
                    analysis_df['date'] = [date]
                    # add column domain
                    analysis_df['domain'] = [domain]
                    # add column cluster
                    analysis_df['cluster'] = [cluster]
                    # add column precision
                    analysis_df['precision'] = domain_obj.scoring_df.loc['precision'][cluster]
                    # add column recall
                    analysis_df['recall'] = domain_obj.scoring_df.loc['recall'][cluster]
                    # add column f1_score
                    analysis_df['f1_score'] = domain_obj.scoring_df.loc['f1-score'][cluster]
                    # add column support
                    analysis_df['support'] = domain_obj.scoring_df.loc['support'][cluster]
                    # add column most_fn_to
                    analysis_df['most_fn_to'] =\
                        ','.join([str(elem)
                                  for elem in
                                  list(domain_obj.conf_mat_df.iloc[cluster][domain_obj.conf_mat_df.iloc[cluster] ==
                                                                            domain_obj.conf_mat_df.iloc[cluster].drop([cluster]).max()].index.values)])
                    # add column most_fn_to_percentage
                    analysis_df['most_fn_to_percentage'] = domain_obj.conf_mat_df.iloc[cluster].drop([cluster]).max()
                    # add column most_fp_from
                    analysis_df['most_fp_from'] = \
                        ','.join([str(elem)
                                  for elem in
                                  list(domain_obj.conf_mat_df.iloc[:, cluster][domain_obj.conf_mat_df.iloc[:, cluster] ==
                                                                               domain_obj.conf_mat_df.iloc[:, cluster].drop([cluster]).max()].index.values)])
                    # add column most_fp_from_percentage
                    analysis_df['most_fp_from_percentage'] = domain_obj.conf_mat_df[cluster].drop([cluster]).max()
                    # append the dataframe for metrics per cluster/domain/date to one global dataframe
                    analysis_final_df = analysis_final_df.append(analysis_df)
        # reset reindex
        analysis_final_df.reset_index(drop=True)
        return analysis_final_df
