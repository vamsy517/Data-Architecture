# imports:
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

import pandas as pd
import numpy as np
import requests
import pandas_gbq
from google.oauth2 import service_account
import os
from sklearn.preprocessing import MultiLabelBinarizer, MinMaxScaler
from pickle import dump, load
from scipy import stats
import pathlib
from sklearn.cluster import KMeans
from matplotlib import pyplot as plt
import time
import math
from collections import defaultdict, OrderedDict
from google.cloud import bigquery
from google.cloud import bigquery_storage
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import precision_recall_fscore_support, confusion_matrix, classification_report
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from statistics import variance


# set up the credentials to access the GCP database
def set_credentials():
    keyfile='/home/ingest/credentials/gbq_credentials.json'
    project_id = 'project-pulldata'
    credentials = service_account.Credentials.from_service_account_file(
        keyfile,
    )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id

    # Initialize clients.
    bqclient = bigquery.Client(credentials=credentials, project=project_id)
    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
    return bqclient, bqstorageclient

def generate_base_data(domain, table):

    # initialize connection to GBQ
    bqclient, bqstorageclient = set_credentials()
    # fix domain with names that we used in GBQ 
    # set query to pull evaluation data
    query_string = f"""
        SELECT * FROM `project-pulldata.Segmentation_{domain}.{table}`
        WHERE EXTRACT(DATE FROM DATETIME (date)) <= '2021-01-31'
        AND EXTRACT(DATE FROM DATETIME (date)) >= '2020-12-24'
        """
    dataframe = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    return dataframe

def generate_data(domain, date, table):

    # initialize connection to GBQ
    bqclient, bqstorageclient = set_credentials()
    # fix domain with names that we used in GBQ 
    # set query to pull evaluation data
    query_string = f"""
        SELECT * FROM `project-pulldata.Segmentation_{domain}.{table}`
        WHERE EXTRACT(DATE FROM DATETIME (date)) = DATE_ADD('{date}', INTERVAL -1 DAY)
        """
    dataframe = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    return dataframe

def calculate_cosine_similarity(vector_1, vector_2):
    similarity = np.dot(vector_1, vector_2)/(np.linalg.norm(vector_1)*np.linalg.norm(vector_2))
    return similarity

def get_similarity(df_base, df_new, dim_for_measuring, group_cols, n_clusters):
    df_base = df_base.groupby(group_cols, as_index=False).mean()
    # create dataframe with unknown industry and 0 for all other columns for that clusters that not appear in database
    df_unknown = pd.DataFrame(columns=df_base.columns)
    df_unknown['industry'] = ['unknown' for x in range(n_clusters)]
    df_unknown['cluster'] = [x for x in range(n_clusters)]
    df_unknown = df_unknown.fillna(value=0)
    # append dataframe to base and new dataframes
    df_base = df_base.append(df_unknown).reset_index(drop=True)
    old_combinations = [(industry, cluster) for industry, cluster in zip(df_base['industry'], df_base['cluster'])]
    new_combinations = [(industry, cluster) for industry, cluster in zip(df_new['industry'], df_new['cluster'])]
    in_old_not_in_new = set(old_combinations) - set(new_combinations)
    # for combination in in_old_not_in_new
    # add combination to new dataframe with 0
    for industry, cluster in in_old_not_in_new:
        df_to_append = pd.DataFrame(columns = df_new.columns)
        df_to_append['industry'] = [industry]
        df_to_append['cluster'] = [cluster]
        df_to_append = df_to_append.fillna(value=0)
        df_new = df_new.append(df_to_append).reset_index(drop=True)
    # for combination in in_new_not_in_old
    # move this combinations to unknown (sum)
    in_new_not_in_old = set(new_combinations) - set(old_combinations)
    for industry, cluster in in_new_not_in_old:
        df_new['industry'][(df_new['industry'] == industry) & (df_new['cluster'] == cluster)] = 'unknown'
    df_new = df_new.groupby(group_cols, as_index=False).sum()
    # assert shapes are same
    print('Asserting industry dfs are in same shape.')
    assert df_new.shape == df_base.shape
    df_base['vector_base'] = df_base[dim_for_measuring].values.tolist()
    df_industry_similarity_base = df_base['vector_base']
    df_new['vector_new'] = df_new[dim_for_measuring].values.tolist()
    df_industry_similarity_new = df_new['vector_new']
    df_industry_similarity = pd.concat([df_industry_similarity_base, df_industry_similarity_new], axis=1)
    similarity = df_industry_similarity.apply(lambda x: calculate_cosine_similarity(x['vector_base'], x['vector_new']), axis=1)
    return similarity.mean()

def aggregate_clustering_metrics(DOMAINS, DATE, DIM_FOR_MEASURING_BEHAVIOUR, DIM_FOR_MEASURING_INDUSTRY, N_CLUSTERS):
    df_clustering_final = pd.DataFrame()
    for domain in DOMAINS:
        print(f'Starting aggregation for {domain}')
        df_clustering = pd.DataFrame()
        # download base and new data
        df_behavioural_profiles_base = generate_base_data(domain, 'behavioural_profiles')
        df_behavioural_profiles = generate_data(domain, DATE, 'behavioural_profiles')
        # make sure custers are orderd correctly in both tables
        df_behavioural_profiles_base = df_behavioural_profiles_base.sort_values('cluster', ascending=True, ignore_index=True)
        df_behavioural_profiles = df_behavioural_profiles.sort_values('cluster', ascending=True, ignore_index=True)
        # calculate avarage cluster distributions
        base_cluster_size_avg = df_behavioural_profiles_base[['cluster', 'count', 'date']].groupby(['cluster'])['count'].mean().values
        cluster_size_avg = df_behavioural_profiles['count'].values
        # add new columns
        df_clustering['date'] = [(datetime.strptime(DATE, '%Y-%m-%d') - timedelta(days=1))]
        df_clustering['domain'] = [domain]
        # calculate similarity between distributions
        df_clustering['cluster_size'] = [calculate_cosine_similarity(base_cluster_size_avg, cluster_size_avg)]
        # begin calculation for behaviour profiles
        # calculate global base averages
        df_centroid_similarity_base = df_behavioural_profiles_base.groupby(['cluster']).mean()
        # select the dimensions to be measured
        behavioural_cols = DIM_FOR_MEASURING_BEHAVIOUR
        # convert dimensions to vector
        df_centroid_similarity_base['vector_1'] = df_centroid_similarity_base[behavioural_cols].values.tolist()
        df_centroid_similarity_base = df_centroid_similarity_base['vector_1']
        df_behavioural_profiles['vector_2'] = df_behavioural_profiles[behavioural_cols].values.tolist()
        df_centroid_similarity = df_behavioural_profiles['vector_2']
        # concat vectors
        df_centroid_similarity = pd.concat([df_centroid_similarity_base, df_centroid_similarity], axis=1)
        # calculate cosine similarity
        similarity = df_centroid_similarity.apply(lambda x: calculate_cosine_similarity(x['vector_1'], x['vector_2']), axis=1)
        df_clustering['behaviour'] = similarity.mean()
        # select the identification columns for the industry aggregation
        group_cols = ['industry', 'cluster']
        # Generate industry data
        df_nsmg_industry_base = generate_base_data(domain, 'industry_profiles')
        df_nsmg_industry = generate_data(domain, DATE, 'industry_profiles')
        df_nsmg_industry_base = df_nsmg_industry_base.sort_values('cluster', ascending=True, ignore_index=True)
        df_nsmg_industry = df_nsmg_industry.sort_values('cluster', ascending=True, ignore_index=True)
        # calculate similarity
        df_clustering['industry'] = get_similarity(df_nsmg_industry_base, df_nsmg_industry, DIM_FOR_MEASURING_INDUSTRY, group_cols, N_CLUSTERS)
        df_wishlist_industry_base = generate_base_data(domain, 'wishlist_industry_profiles')
        df_wishlist_industry = generate_data(domain, DATE, 'wishlist_industry_profiles')
        df_wishlist_industry_base = df_wishlist_industry_base.sort_values('cluster', ascending=True, ignore_index=True)
        df_wishlist_industry = df_wishlist_industry.sort_values('cluster', ascending=True, ignore_index=True)
        df_clustering['wishlist_industry'] = get_similarity(df_wishlist_industry_base, df_wishlist_industry, DIM_FOR_MEASURING_INDUSTRY, group_cols, N_CLUSTERS)
        # append to master df
        df_clustering_final = df_clustering_final.append(df_clustering)
    df_clustering_final.reset_index(drop=True)
    return df_clustering_final

def aggregate_returning_visitors_metrics(date):
    bqclient, bqstorageclient = set_credentials()
    query_string_base = f"""
    SELECT domain, avg(percent_returning) as base_avg FROM `project-pulldata.Segmentation.percent_returning_visitors_per_day`
    WHERE day <= '2021-01-31'
    AND day >= '2020-12-24'
    group by domain
    """
    df_returning_visitors_base = (
        bqclient.query(query_string_base)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    query_string_new = f"""
    SELECT domain, avg(percent_returning) as new_avg FROM `project-pulldata.Segmentation.percent_returning_visitors_per_day`
    WHERE day = DATE_ADD('{date}', INTERVAL -1 DAY)
    group by domain
    """
    df_returning_visitors_new = (
        bqclient.query(query_string_new)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    
    df_returning_visitors_final = df_returning_visitors_base.merge(df_returning_visitors_new, how='left', on='domain')
    df_returning_visitors_final['returning_visitors_change'] = df_returning_visitors_final.apply(
        lambda x: x['new_avg'] - x['base_avg'], axis=1)
    df_returning_visitors_final['domain'] = df_returning_visitors_final['domain'].apply(
        lambda x: x.split('.')[1])
    df_returning_visitors_final = df_returning_visitors_final[['domain', 'returning_visitors_change']]
    return df_returning_visitors_final

def aggregate_classifier_metrics(date):
    yesterday = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    print(yesterday)
    bqclient, bqstorageclient = set_credentials()
    query_string_base = f"""
    SELECT domain, avg(weighted_avg_precision) as base_avg_precision,
    avg(weighted_avg_recall) as base_avg_recall,
    avg(weighted_avg_f1_score) as base_avg_f1_score
    FROM
    (SELECT date, domain, 
    sum(precision * support) / sum(support) as weighted_avg_precision,
    sum(recall * support) / sum(support) as weighted_avg_recall,
    sum(f1_score * support) / sum(support) as weighted_avg_f1_score
    FROM `project-pulldata.Segmentation.RF_metrics_report`
    WHERE date <= '2021-01-31'
    AND date >= '2020-12-24'
    group by date, domain)
    group by domain
    """
    df_classifier_base = (
        bqclient.query(query_string_base)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    query_string_new = f"""
    SELECT domain, 
    sum(precision * support) / sum(support) as new_avg_precision,
    sum(recall * support) / sum(support) as new_avg_recall,
    sum(f1_score * support) / sum(support) as new_avg_f1_score
    FROM `project-pulldata.Segmentation.RF_metrics_report`
    WHERE date = '{yesterday}'
    group by domain
    """
    df_classifier_new = (
        bqclient.query(query_string_new)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    df_classifier_final = df_classifier_base.merge(df_classifier_new, how='left', on='domain')
    df_classifier_final['precision_change'] = df_classifier_final.apply(
        lambda x: x['new_avg_precision'] - x['base_avg_precision'], axis=1)
    df_classifier_final['recall_change'] = df_classifier_final.apply(
        lambda x: x['new_avg_recall'] - x['base_avg_recall'], axis=1)
    df_classifier_final['f1_score_change'] = df_classifier_final.apply(
        lambda x: x['new_avg_f1_score'] - x['base_avg_f1_score'], axis=1)
    df_classifier_final = df_classifier_final[['domain', 'precision_change', 'recall_change', 'f1_score_change']]
    return df_classifier_final
    
def combine_and_upload(DOMAINS, DATE, DIM_FOR_MEASURING_BEHAVIOUR, DIM_FOR_MEASURING_INDUSTRY, N_CLUSTERS):
    project_id = "project-pulldata"
    set_credentials()
    print('Generating clustering data.')
    df_clustering_final = aggregate_clustering_metrics(DOMAINS, DATE, DIM_FOR_MEASURING_BEHAVIOUR, DIM_FOR_MEASURING_INDUSTRY, N_CLUSTERS)
    print('Downloading returning visitors data.')
    df_returning_visitors_final = aggregate_returning_visitors_metrics(DATE)
    df_clustering_returning = df_clustering_final.merge(df_returning_visitors_final, how='left', on='domain')
    print('Downloading RF classifier metrics.')
    df_classifier_final = aggregate_classifier_metrics(DATE)
    df_clustering_returning_clasifier = df_clustering_returning.merge(df_classifier_final, how='left', on='domain')
    print('Uploading to GBQ.')
    df_clustering_returning_clasifier.to_gbq(destination_table='Segmentation.Segmentation_Engine_Report',
                      project_id = project_id, if_exists = 'append')

    return df_clustering_returning_clasifier

def perform_aggregation():
    DOMAINS = ('techmonitor','investmentmonitor', 'citymonitor', 'energymonitor',
               'pressgazette', 'newstatesman', 'elitetraveler', 'monitors')
    DATE = datetime.strftime(datetime.now(), '%Y-%m-%d')
    DIM_FOR_MEASURING_BEHAVIOUR = ['number_of_views', 'visits_from_social', 'visits_from_search', 'visits_from_email', 'lead_or_not', 'avg_timestamp_for_continent', 'average_virality']
    DIM_FOR_MEASURING_INDUSTRY = ['industry_per_cluster', 'industry_total', 'weight_ratio']
    N_CLUSTERS = 8
    df_clustering_returning_clasifier = combine_and_upload(DOMAINS, DATE, DIM_FOR_MEASURING_BEHAVIOUR, DIM_FOR_MEASURING_INDUSTRY, N_CLUSTERS)
    df_clustering_returning_clasifier = df_clustering_returning_clasifier.round(3)
    email_response = '<div><p>' + df_clustering_returning_clasifier.to_html() + '</p></div>'
    return email_response


    
default_args = {
    'owner': 'dscoe_team',
    'start_date': datetime(2021, 1, 19, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('segmentation_engine_report',
     catchup=False,
     default_args=default_args,
     schedule_interval='20 7 * * *',
     ) as dag:
    
    
    perform_aggregation = PythonOperator(task_id='perform_aggregation',
                               python_callable=perform_aggregation)
    
    email = EmailOperator(
        task_id='send_email',
        to=['petyo.karatov@ns-mediagroup.com','veselin.valchev@ns-mediagroup.com','nina.nikolaeva@ns-mediagroup.com','yavor.vrachev@ns-mediagroup.com','Dzhumle.Mehmedova@ns-mediagroup.com','kamen.parushev@ns-mediagroup.com','emil.filipov@ns-mediagroup.com'],
        subject='Daily Segmentation Monitoring Report',
        html_content="""<h3>Daily Segmentation Monitoring Report:</h3><div> {{ti.xcom_pull(task_ids='perform_aggregation')}} </div>""" ,
        dag=dag
)

perform_aggregation >> email