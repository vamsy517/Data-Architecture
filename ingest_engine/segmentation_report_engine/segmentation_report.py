from segmentation_report_engine.utils.utils import *
from segmentation_report_engine.utils.constants_queries import *
from ingest_utils.constants import PULLDATA_PROJECT_ID
from ingest_utils.database_gbq import get_gbq_table
from statistics import mean


class SegmentationReport:
    """
    Class, which upon received dataframes, containing returning visitors data and RF Metrics Report,
    returns Segmentation_Engine_Report table for yesterday
    """
    
    def __init__(self, domains_dict: dict, dataset: str, df_returning_visitors: pd.DataFrame,
                 df_rf_metrics_report: pd.DataFrame,
                 project_id: str = PULLDATA_PROJECT_ID):
        """
        Initialize the class
        :param domains_dict: dictionary of domains, on which to perform RF evaluation
        :param dataset: dataset
        :param df_returning_visitors: DataFrame, containing returning visitors data
        :param df_rf_metrics_report: DataFrame, containing RF Metrics Report
        :param project_id: Project ID of project in GBQ
        """
        self.project_id = project_id
        self.domains_dict = domains_dict
        self.dataset = dataset
        self.date = DATES[0]
        self.dim_for_measuring_behaviour = DIM_FOR_MEASURING_BEHAVIOUR
        self.dim_for_measuring_industry = DIM_FOR_MEASURING_INDUSTRY
        self.dim_for_measuring_jobs = DIM_FOR_MEASURING_JOBS
        self.dim_for_measuring_seniority = DIM_FOR_MEASURING_SENIORITY
        self.df_returning_visitors = df_returning_visitors
        self.df_rf_metrics_report = df_rf_metrics_report

    def aggregate_clustering_metrics(self) -> pd.DataFrame:
        """
        Aggregate clustering metrics
        :return: DataFrame, containing aggregated clustering metrics
        """
        df_clustering_final = pd.DataFrame()
        for domain_key, domain_value in self.domains_dict.items():
            print(f'Starting aggregation for {domain_key}')
            df_clustering = pd.DataFrame()
            # set the queries, which will be used
            base_segmentation_data = BASE_SEGMENTATION_DATA_V2
            yesterday_segmentation_data = YESTERDAY_SEGMENTATION_DATA_V2
            domain = domain_value[0]
            # download base and new data
            df_behavioural_profiles_base = get_gbq_table(self.project_id,
                                                         base_segmentation_data.format(
                                                             domain=domain,
                                                             table='behavioural_profiles',
                                                             project=self.project_id,
                                                             dataset=self.dataset))
            df_behavioural_profiles = get_gbq_table(self.project_id,
                                                    yesterday_segmentation_data.format(
                                                        domain=domain,
                                                        table='behavioural_profiles',
                                                        project=self.project_id,
                                                        dataset=self.dataset,
                                                        yesterday=self.date))

            # make sure clusters are ordered correctly in both tables
            df_behavioural_profiles_base = df_behavioural_profiles_base.sort_values('cluster', ascending=True,
                                                                                    ignore_index=True)
            df_behavioural_profiles = df_behavioural_profiles.sort_values('cluster', ascending=True, ignore_index=True)
            # calculate average cluster distributions
            base_cluster_size_avg = df_behavioural_profiles_base[['cluster', 'count',
                                                                  'date']].groupby(['cluster'])[
                'count', 'cluster'].mean()
            base_cluster_size_avg = base_cluster_size_avg.reset_index(drop=True)
            cluster_size_avg = df_behavioural_profiles[['count', 'cluster']]
            df_cluster_size = base_cluster_size_avg.merge(cluster_size_avg, how='left', on='cluster').fillna(0).astype(
                int)
            base_cluster_size_avg = df_cluster_size.count_x.values
            cluster_size_avg = df_cluster_size.count_y.values
            # add new columns
            df_clustering['date'] = [self.date]
            df_clustering['domain'] = [domain]
            # calculate similarity between distributions
            df_clustering['cluster_size'] = [calculate_cosine_similarity(base_cluster_size_avg, cluster_size_avg)]
            # begin calculation for behaviour profiles
            # calculate global base averages
            df_centroid_similarity_base = df_behavioural_profiles_base.groupby(['cluster']).mean()
            # set index to cluster in order to keep same index values in both dataframes
            df_behavioural_profiles = df_behavioural_profiles.set_index('cluster')
            # select the dimensions to be measured
            behavioural_cols = self.dim_for_measuring_behaviour
            # convert dimensions to vector
            df_centroid_similarity_base['vector_1'] = df_centroid_similarity_base[behavioural_cols].values.tolist()
            df_centroid_similarity_base = df_centroid_similarity_base['vector_1']
            df_behavioural_profiles['vector_2'] = df_behavioural_profiles[behavioural_cols].values.tolist()
            df_centroid_similarity = df_behavioural_profiles['vector_2']
            # concat vectors
            df_centroid_similarity = pd.concat([df_centroid_similarity_base, df_centroid_similarity], axis=1)
            # calculate cosine similarity
            similarity = df_centroid_similarity.apply(lambda x: calculate_cosine_similarity(x['vector_1'],
                                                                                            x['vector_2']), axis=1)
            similarity = [0 if type(x) == np.ndarray else x for x in similarity]
            df_clustering['behaviour'] = mean(similarity)
            # select the identification columns for the industry aggregation
            group_cols = ['industry', 'cluster']
            # Generate industry data
            df_nsmg_industry_base = get_gbq_table(self.project_id,
                                                  base_segmentation_data.format(
                                                      domain=domain,
                                                      table='industry_profiles',
                                                      project=self.project_id,
                                                      dataset=self.dataset))
            df_nsmg_industry = get_gbq_table(self.project_id,
                                             yesterday_segmentation_data.format(
                                                 domain=domain,
                                                 table='industry_profiles',
                                                 project=self.project_id,
                                                 dataset=self.dataset,
                                                 yesterday=self.date))
            df_nsmg_industry_base = df_nsmg_industry_base.sort_values('cluster', ascending=True, ignore_index=True)
            df_nsmg_industry = df_nsmg_industry.sort_values('cluster', ascending=True, ignore_index=True)
            # generate clusters dictionary
            n_clusters = generate_clusters_dict('industry_profiles')[domain_value[0]]
            # calculate similarity
            df_clustering['industry'] = get_similarity(df_nsmg_industry_base, df_nsmg_industry,
                                                       self.dim_for_measuring_industry, group_cols, n_clusters,
                                                       'industry')

            df_wishlist_industry_base = get_gbq_table(self.project_id,
                                                      base_segmentation_data.format(
                                                          domain=domain,
                                                          table='wishlist_industry_profiles',
                                                          project=self.project_id,
                                                          dataset=self.dataset))
            df_wishlist_industry = get_gbq_table(self.project_id,
                                                 yesterday_segmentation_data.format(
                                                     domain=domain,
                                                     table='wishlist_industry_profiles',
                                                     project=self.project_id,
                                                     dataset=self.dataset,
                                                     yesterday=self.date))
            df_wishlist_industry_base = df_wishlist_industry_base.sort_values('cluster', ascending=True,
                                                                              ignore_index=True)
            df_wishlist_industry = df_wishlist_industry.sort_values('cluster', ascending=True, ignore_index=True)

            # rename columns
            df_wishlist_industry_base.rename(columns={'wishlist_industry': 'industry',
                                                      'wishlist_industry_per_cluster': 'industry_per_cluster',
                                                      'wishlist_industry_total': 'industry_total'},
                                             inplace=True)
            df_wishlist_industry.rename(columns={'wishlist_industry': 'industry',
                                                 'wishlist_industry_per_cluster': 'industry_per_cluster',
                                                 'wishlist_industry_total': 'industry_total'},
                                        inplace=True)
            df_clustering['wishlist_industry'] = get_similarity(df_wishlist_industry_base, df_wishlist_industry,
                                                                self.dim_for_measuring_industry, group_cols,
                                                                n_clusters, 'industry')

            # select the identification columns for the jobs aggregation
            group_cols = ['jobs', 'cluster']
            # Generate jobs data
            df_nsmg_jobs_base = get_gbq_table(self.project_id, base_segmentation_data.format(domain=domain,
                                                                                             table='jobs_profiles',
                                                                                             project=self.project_id,
                                                                                             dataset=self.dataset))
            df_nsmg_jobs = get_gbq_table(self.project_id, yesterday_segmentation_data.format(domain=domain,
                                                                                             table='jobs_profiles',
                                                                                             project=self.project_id,
                                                                                             dataset=self.dataset,
                                                                                             yesterday=self.date))
            df_nsmg_jobs_base = df_nsmg_jobs_base.sort_values('cluster', ascending=True, ignore_index=True)
            df_nsmg_jobs = df_nsmg_jobs.sort_values('cluster', ascending=True, ignore_index=True)
            # generate clusters dictionary
            n_clusters = generate_clusters_dict('jobs_profiles')[domain_value[0]]
            # calculate similarity
            df_clustering['jobs'] = get_similarity(df_nsmg_jobs_base, df_nsmg_jobs,
                                                   self.dim_for_measuring_jobs, group_cols, n_clusters, 'jobs')

            # select the identification columns for the seniority aggregation
            group_cols = ['seniority', 'cluster']
            # Generate jobs data
            df_nsmg_seniority_base = get_gbq_table(self.project_id,
                                                   base_segmentation_data.format(domain=domain,
                                                                                 table='seniority_profiles',
                                                                                 project=self.project_id,
                                                                                 dataset=self.dataset))
            df_nsmg_seniority = get_gbq_table(self.project_id,
                                              yesterday_segmentation_data.format(domain=domain,
                                                                                 table='seniority_profiles',
                                                                                 project=self.project_id,
                                                                                 dataset=self.dataset,
                                                                                 yesterday=self.date))
            df_nsmg_seniority_base = df_nsmg_seniority_base.sort_values('cluster', ascending=True, ignore_index=True)
            df_nsmg_seniority = df_nsmg_seniority.sort_values('cluster', ascending=True, ignore_index=True)
            # generate clusters dictionary
            n_clusters = generate_clusters_dict('seniority_profiles')[domain_value[0]]
            # calculate similarity
            df_clustering['seniority'] = get_similarity(df_nsmg_seniority_base, df_nsmg_seniority,
                                                        self.dim_for_measuring_seniority, group_cols, n_clusters,
                                                        'seniority')

            # append to master df
            df_clustering_final = df_clustering_final.append(df_clustering)
        # reset reindex
        df_clustering_final.reset_index(drop=True)
        return df_clustering_final

    def aggregate_returning_visitors_metrics(self) -> pd.DataFrame:
        """
        Aggregate returning visitors metrics
        :return: DataFrame, containing aggregated returning visitors metrics
        """
        # get base data from returning visitors
        df_returning_visitors_base = get_gbq_table(self.project_id, BASE_RETURNING_VISITORS_DATA_V2)
        # get yesterday data from returning visitors
        df_returning_visitors_new = self.df_returning_visitors
        df_returning_visitors_new = df_returning_visitors_new.drop(['cluster'], axis=1)
        df_returning_visitors_new = df_returning_visitors_new.groupby(['domain']).mean()
        df_returning_visitors_new.reset_index(inplace=True)
        df_returning_visitors_new.columns = ['domain', 'new_avg']
        # merge base and yesterday returning visitors data
        df_returning_visitors_final = df_returning_visitors_base.merge(df_returning_visitors_new, how='left',
                                                                       on='domain')
        # set returning_visitors_change column
        df_returning_visitors_final['returning_visitors_change'] = df_returning_visitors_final.apply(
            lambda x: x['new_avg'] - x['base_avg'], axis=1)
        # get actual domain without www. for V2
        df_returning_visitors_final['domain'] = df_returning_visitors_final['domain'].apply(
            lambda x: '.'.join(x.split('.')[1:]))
        # keep needed columns
        df_returning_visitors_final = df_returning_visitors_final[['domain', 'returning_visitors_change']]
        return df_returning_visitors_final

    def aggregate_classifier_metrics(self) -> pd.DataFrame:
        """
        Aggregate classifier metrics
        :return: DataFrame, containing aggregated classifier metrics
        """
        # get base classifier data for Segmentation V1 or V2
        df_classifier_base = get_gbq_table(self.project_id, BASE_CLASSIFIER_DATA_V2)
        # get yesterday classifier data
        df_classifier_new = self.df_rf_metrics_report[['domain', 'precision', 'support', 'recall', 'f1_score']]
        df_classifier_new['precision_support'] = df_classifier_new['precision']*df_classifier_new['support']
        df_classifier_new['recall_support'] = df_classifier_new['recall']*df_classifier_new['support']
        df_classifier_new['f1_score_support'] = df_classifier_new['f1_score']*df_classifier_new['support']
        df_classifier_new = df_classifier_new.groupby(['domain']).sum()
        df_classifier_new.reset_index(inplace=True)
        df_classifier_new['new_avg_precision'] = df_classifier_new['precision_support'] / df_classifier_new['support']
        df_classifier_new['new_avg_recall'] = df_classifier_new['recall_support'] / df_classifier_new['support']
        df_classifier_new['new_avg_f1_score'] = df_classifier_new['f1_score_support'] / df_classifier_new['support']
        df_classifier_new = df_classifier_new[['domain', 'new_avg_precision', 'new_avg_recall', 'new_avg_f1_score']]
        # set final dataframe
        df_classifier_final = df_classifier_base.merge(df_classifier_new, how='left', on='domain')
        df_classifier_final['precision_change'] = df_classifier_final.apply(
            lambda x: x['new_avg_precision'] - x['base_avg_precision'], axis=1)
        df_classifier_final['recall_change'] = df_classifier_final.apply(
            lambda x: x['new_avg_recall'] - x['base_avg_recall'], axis=1)
        df_classifier_final['f1_score_change'] = df_classifier_final.apply(
            lambda x: x['new_avg_f1_score'] - x['base_avg_f1_score'], axis=1)
        df_classifier_final = df_classifier_final[['domain', 'precision_change', 'recall_change', 'f1_score_change',
                                                   'base_avg_f1_score']]
        return df_classifier_final

    def generate_report(self) -> pd.DataFrame:
        """
        Generate Segmentation Engine Report
        :return: dataframe, containing table Segmentation_Engine_Report for yesterday
        """
        print('Generating clustering data')
        df_clustering_final = self.aggregate_clustering_metrics()
        print('Generating returning visitors data')
        df_returning_visitors_final = self.aggregate_returning_visitors_metrics()
        print('Generating RF classifier metrics')
        df_classifier_final = self.aggregate_classifier_metrics()
        # merge df_clustering_final and df_clustering_returning
        df_clustering_returning = df_clustering_final.merge(df_returning_visitors_final, how='left', on='domain')
        # merge df_clustering_returning and df_classifier_final
        df_clustering_returning_classifier = df_clustering_returning.merge(df_classifier_final, how='left', on='domain')
        return df_clustering_returning_classifier
