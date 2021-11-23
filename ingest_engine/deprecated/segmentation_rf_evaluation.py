# danni ot 1 dekemvri

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

# let's start by fixing the domain - our sql queries are structured so that they require this
def fix_domain(domain):
    if domain == 'techmonitor':
        domain_url = ('techmonitor.ai', 'cbronline.com')
    elif domain == 'investmentmonitor':
        domain_url = ('investmentmonitor.ai',)
    elif domain == 'citymonitor':
        domain_url = ('citymonitor.ai',)
    elif domain == 'energymonitor':
        domain_url = ('energymonitor.ai',)
    elif domain == 'pressgazette':
        domain_url = ('pressgazette.co.uk',)
    elif domain == 'newstatesman':
        domain_url = ('newstatesman.com',)
    elif domain == 'elitetraveler':
        domain_url = ('elitetraveler.com',)
    elif domain == 'monitors':
        domain_url = ('investmentmonitor.ai', 'citymonitor.ai', 'energymonitor.ai', 'techmonitor.ai', 'cbronline.com')
    domain_url += tuple('www.' + domain for domain in domain_url)
    return str(domain_url)

def generate_test_data(domain, date):
    '''
    Get evaluation (test) data from GBQ.
    Input:
        domain: tuple with NSMG domains
        date: tuple with dates that we will test on
    Output:
        dataframe which will be used as input for RF evaluation
    
    '''
    # initialize connection to GBQ
    bqclient, bqstorageclient = set_credentials()
    # fix domain with names that we used in GBQ
    domain_url = fix_domain(domain) 
    # set query to pull evaluation data
    query_string = f"""
        select * from (select user_id, time, domain, type,
        referrer, url, continent,
        user_agent, cluster, row_number() over (partition by user_id order by time DESC) as rn
        FROM `project-pulldata.Segmentation.RF_kmeans_comparison_{date}`
        where domain in {domain_url} and cluster is not null)  where rn = 1
        """
    dataframe = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    return dataframe

def localize_hour(utc_hour, continent):
    '''
    Input:
        utc_hour: hour in utc
        continent: continent as string
    Output:
        local_hour: hour in user local time
    '''
    if continent == 'North America':
        local_hour = utc_hour - 6
    elif continent == 'South America':
        local_hour = utc_hour - 3
    elif continent == 'Europe':
        local_hour = utc_hour + 1
    elif continent == 'Africa':
        local_hour = utc_hour + 1
    elif continent == 'Asia':
        local_hour = utc_hour + 5
    else:
        local_hour = utc_hour
    return local_hour

def extract_referrer_group(referrer, domain):
    '''
    Extract refferer group based on predefined lists
    '''
    social_list = ['facebook', 'linkedin', 'twitter', 't.co/', 'youtube', 'instagram', 'reddit', 'pinterest']
    search_list = ['ask', 'baidu', 'bing', 'duckduckgo', 'google', 'yahoo', 'yandex']
    nsmg_network = ['energymonitor.ai', 'investmentmonitor.ai', 'citymonitor.ai', 'newstatesman.com',
                   'cbronline.com', 'techmonitor.ai', 'elitetraveler.com']
    own_website = [domain]
    email = ['Pardot', 'pardot']
    if any(wildcard in referrer for wildcard in social_list):
        referrer_group = 'social'
    elif any(wildcard in referrer for wildcard in search_list):
        referrer_group = 'search'
    elif any(wildcard in referrer for wildcard in nsmg_network):
        referrer_group = 'NSMG'
    elif any(wildcard in referrer for wildcard in own_website):
        referrer_group = 'own_website'
    elif any(wildcard in referrer for wildcard in email):
        referrer_group = 'email'
    else:
        referrer_group = 'other_referrer'
    return referrer_group

def extract_os_group(user_agent):
    '''
    Extract operation system
    '''
    if 'Mozilla/5.0 (Linux' in user_agent:
        os_group = 'linux'
    elif 'Mozilla/5.0 (Android' in user_agent:
        os_group = 'android'
    elif 'Mozilla/5.0 (Windows NT 10.0' in user_agent:
        os_group = 'windows10'
    elif 'Mozilla/5.0 (Windows' in user_agent and 'Mozilla/5.0 (Windows NT 10.0' not in user_agent:
        os_group = 'windows_older'
    elif 'Mozilla/5.0 (iPhone' in user_agent:
        os_group = 'iphone'
    elif 'Mozilla/5.0 (Macintosh' in user_agent:
        os_group = 'macintosh'
    elif 'Mozilla/5.0 AppleWebKit' in user_agent:
        os_group = 'apple_other'
    else:
        os_group = 'other_os'
    return os_group

# this function is not in use:
def extract_section(url):
    if len(url.split('/')) > 3:
        section = url.split('/')[3]
    else:
        section = 'none'
    return section

def preprocess_data(df_test, domain, date, random_state):
    '''
    Input:
        df_test: dataframe with test evaluation data
        domain: specific domain
        date: date on which evaluation is perform
        random_state: random state
    Output:
        test_y = test labels
        test_x = test data
    '''
    df_test = df_test.drop(['rn', 'type'], axis = 1)
    # preprocess the 'time' field - it should contain only the 'hour' - 0:24
#     df_test['hour_of_day_utc'] = df_test.time.dt.hour
    df_test['hour_of_day_utc'] = df_test.time.dt.hour
    df_test['local_hour_proxy'] = df_test.apply(lambda x: localize_hour(x['hour_of_day_utc'], x['continent']), axis = 1)
    # drop the original and helper column
    df_test = df_test.drop(['time', 'hour_of_day_utc'], axis = 1)
    df_test = df_test.fillna('other')
    df_test['referrer_group'] = df_test.apply(lambda x: extract_referrer_group(x['referrer'], domain), axis = 1)
    # drop the original column
    df_test = df_test.drop(['referrer'], axis = 1)
    df_test['os_group'] = df_test.apply(lambda x: extract_os_group(x['user_agent']), axis = 1)
    # drop the original column
    df_test = df_test.drop(['user_agent'], axis = 1)
    df_test['website_section'] = df_test.apply(lambda x: extract_section(x['url']), axis = 1)
    # TODO: since there are over 12k different strings after the third '/', this is no feasible way to extract sections
    # so we drop both the original column and this new one
#     assert df_test.groupby('website_section').count().shape[0] > 50
    df_test = df_test.drop(['url', 'website_section'], axis = 1)
    df_test = df_test.set_index('user_id')
    # let's evaluate the fit
    encoder_file = f'/home/airflow_gcp/segmentation/{domain}/{domain}_encoder_rs{random_state}.pkl'
    encoder_loaded = load(open(encoder_file, 'rb'))
    categorical_features_df_test = df_test[['domain', 'continent', 'referrer_group', 'os_group']]
    ohe_array_test = encoder_loaded.transform(categorical_features_df_test)
    
    ohe_df_test = pd.DataFrame(ohe_array_test,
                               index = categorical_features_df_test.index, columns = encoder_loaded.get_feature_names())
    # finally, let's add the two other columns to this df to have a compelete, good-to-go df
    df_test_enc = ohe_df_test.join(df_test[['local_hour_proxy', 'cluster']])
#     df_test_enc.to_csv(f'preprocessed_test_data_latest/preprocessed_{domain}_{date}.csv')
    
    test_y, test_x = df_test_enc['cluster'], df_test_enc.loc[:, df_test_enc.columns !='cluster']
    print(f'Test set size: {test_x.shape}, targets: {test_y.shape}')
    return test_y, test_x

def classification_report_df(test_x, test_y, domain, date, random_state):
    '''
    Perform classification report
    Input:
        test_y = test labels
        test_x = test data
        domain: specific domain
        date: date on which evaluation is perform
        random_state: random state
    '''
    model_name = f'/home/airflow_gcp/segmentation/{domain}/rf_classifier_{domain}_rs{random_state}.sav'
    clf_loaded = load(open(model_name, 'rb'))
    test_preds = clf_loaded.predict(test_x)
    report = classification_report(test_y, test_preds, output_dict=True)
    scoring_df = pd.DataFrame(report)
#     scoring_df.to_csv(f'classification_report_latest/classification_report_{domain}_{date}_rs{random_state}.csv')
    return scoring_df, test_preds

def confusion_matrix_df(test_y, test_preds, domain, date, random_state):
    '''
    Get confusion matrix dataframe
    Input:
        test_y = test labels
        test_preds = predictions of RF on the test data
        domain: specific domain
        date: date on which evaluation is perform
        random_state: random state
    Output:
        confusion matrix dataframe
    '''
    
#     TEXT_COLOR = 'k'
#     fig=plt.figure(figsize=(12, 10))
    conf_mat = confusion_matrix(test_y, test_preds)
    conf_mat_norm = conf_mat.astype('float') / conf_mat.sum(axis=1)[:, np.newaxis]
    conf_mat_df = pd.DataFrame(conf_mat_norm)
    conf_mat_df = conf_mat_df.round(2)

#     conf_mat_df.to_csv(f'confusion_matrix_data_latest/confusion_matrix_{domain}_{date}_rs{random_state}.csv')
    return conf_mat_df

def perform_analysis(domains, dates, random_state):
    '''
        Perfrom analysis on performarnce of Random Forest on all domain on spesific dates
        Input:
            domains: Tuple with domains that we will use in evaluation
            dates: Tuple with date that we will use in evaluation
            random_state: random state
        Output:
            upload to GBQ table with evaluation (precision, recall, f1-score, support,
                                   most_fn_to, most_fn_to_percentage, 
                                   most_fp_from, most_fp_from_percentage)
                                   
            store reports with key metrics as csv for daily email
    '''
    # set crecetials
    set_credentials()
    # create empty dataframe to store data for all domains and dates
    analysis_final_df = pd.DataFrame()
    # iterate over all domains and dates to performe evaluation
    for domain in domains:
        for date in dates:
            # generate test data from GBQ
            df = generate_test_data(domain, date)
            # preprocess the raw data to format that is used from the RF model
            test_y, test_x = preprocess_data(df, domain, date, random_state)
            # perform classification report and store the report and predicted values
            scoring_df, test_preds = classification_report_df(test_x, test_y, domain, date, random_state)
            # get confiusion matrix
            conf_mat_df = confusion_matrix_df(test_y, test_preds, domain, date, random_state)
            # for every cluster we get key metrixs
            for cluster in range(conf_mat_df.shape[0]):
                # create empty dataframe to add key metrix
                analysis_df = pd.DataFrame()
                analysis_df['date'] = [date]
                analysis_df['domain'] = [domain]
                analysis_df['cluster'] = [cluster]
                analysis_df['precision'] = scoring_df.loc['precision'][cluster]
                analysis_df['recall'] = scoring_df.loc['recall'][cluster]
                analysis_df['f1_score'] = scoring_df.loc['f1-score'][cluster]
                analysis_df['support'] = scoring_df.loc['support'][cluster]
                analysis_df['most_fn_to'] = ','.join([str(elem) for elem in list(conf_mat_df.iloc[cluster][conf_mat_df.iloc[cluster] == conf_mat_df.iloc[cluster].drop([cluster]).max()].index.values)])
                analysis_df['most_fn_to_percentage'] = conf_mat_df.iloc[cluster].drop([cluster]).max()
                analysis_df['most_fp_from'] = ','.join([str(elem) for elem in list(conf_mat_df.iloc[:,cluster][conf_mat_df.iloc[:,cluster] == conf_mat_df.iloc[:,cluster].drop([cluster]).max()].index.values)])
                analysis_df['most_fp_from_percentage'] = conf_mat_df[cluster].drop([cluster]).max()
                # append the dataframe for metrics per cluster/domain/date to one global dataframe
                analysis_final_df = analysis_final_df.append(analysis_df)

    analysis_final_df.reset_index(drop=True)
    
    # upload metadata to GBQ
    analysis_final_df.to_gbq(destination_table='Segmentation.RF_metrics_report',
                      project_id = 'project-pulldata', if_exists = 'append')
    
    # prepare report for each domain which have clusters with f1 < 30% and support > 1
    report_f1_score = analysis_final_df[(analysis_final_df['f1_score']<0.3) & (analysis_final_df['support']>0)][['date','domain','cluster','f1_score','support']].groupby(['date','domain'])[['cluster','f1_score','support']].apply(lambda x: x.values.tolist()).to_frame().reset_index()
    report_f1_score.rename(columns={0:'f1_score'}, inplace=True)
    report_f1_score['f1_score'] = report_f1_score['f1_score'].apply(lambda table: [('cluster:' + str(int(x)), 'f1_score:' + str(round(y, 2)), 'support:'+str(int(z))) for x,y,z in table])
    report_f1_score.to_csv('/home/airflow_gcp/segmentation/reports/report_f1_score.csv')
    return True

def evaluate_rf():
    # main function which is called by the dag to perform the analysis with parameters
    RANDOM_STATE = 0
    DOMAINS = ('techmonitor','investmentmonitor', 'citymonitor', 'energymonitor',
               'pressgazette', 'newstatesman', 'elitetraveler', 'monitors') 
    DATES = (datetime.strftime((datetime.now() - timedelta(1)), '%Y-%m-%d'),)
    perform_analysis(DOMAINS, DATES, RANDOM_STATE)
    return True


default_args = {
    'owner': 'segmentation_rf_evaluation',
    'start_date': datetime(2021, 1, 19, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('segmentation_rf_evaluation',
     catchup=False,
     default_args=default_args,
     schedule_interval='15 7 * * *',
     ) as dag:
    
    
    evaluate_rf_data = PythonOperator(task_id='evaluate_rf',
                               python_callable=evaluate_rf)

evaluate_rf_data