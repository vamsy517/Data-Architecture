from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import datetime
from ingest_utils.constants import PULLDATA_PROJECT_ID, POSTGRE_AUTH
from ingest_utils.database_gbq import upload_table_to_gbq
from ingest_utils.metadata import update_metadata


def update_campaigns_list() -> bool:
    """
    Replace table CampaignsList_DailyUpdated in dataset CampaignTracker in GBQ from provided Google Sheet
    Update metadata for this table
    :return: True in case all operations are successful
    """
    # read campaigns list
    print('Download table CampaignsList_DailyUpdated from Google Sheet')
    df = pd.read_csv('https://docs.google.com/spreadsheets/d/111A0ToYthfN_aHC5AUplvRctB0seanTII82rZtFDsH8/export?gid=0&format=csv')
    df.rename(columns={'Campaign name': 'campaign_name', 'URL': 'url'}, inplace=True)
    df=df[['campaign_name', 'url', 'Title']]
    print('Replace table CampaignsList_DailyUpdated in GBQ')
    upload_table_to_gbq(PULLDATA_PROJECT_ID, df, 'CampaignTracker', 'CampaignsList_DailyUpdated', method='replace')
    print('Update metadata for table CampaignsList_DailyUpdated')
    update_metadata(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df, 'CampaignTracker', 'CampaignsList_DailyUpdated')
    # read VMS campaigns list
    print('Download table CampaignsList_VMS_DailyUpdated from Google Sheet')
    df = pd.read_csv('https://docs.google.com/spreadsheets/d/193IYD1gdBUqpvNPA_e_iSemG1FxtfLgTlsfLfoVm4NU/export?gid=0&format=csv')
    df.rename(columns={'Campaign name': 'campaign_name', 'URL': 'url'}, inplace=True)
    df=df[['campaign_name', 'url', 'Title']]
    print('Replace table CampaignsList_VMS_DailyUpdated in GBQ')
    upload_table_to_gbq(PULLDATA_PROJECT_ID, df, 'CampaignTracker', 'CampaignsList_VMS_DailyUpdated', method='replace')
    print('Update metadata for table CampaignsList_VMS_DailyUpdated')
    update_metadata(PULLDATA_PROJECT_ID, POSTGRE_AUTH, df, 'CampaignTracker', 'CampaignsList_VMS_DailyUpdated')
    return True

default_args = {
    'owner': 'project-pulldata',
    'start_date': datetime.datetime(2020, 10, 15, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_campaignslist_dailyupdate',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 4 * * *',
     ) as dag:
    
    
    update_campaigns_data = PythonOperator(task_id='update_campaigns_list',
                               python_callable=update_campaigns_list)

update_campaigns_data