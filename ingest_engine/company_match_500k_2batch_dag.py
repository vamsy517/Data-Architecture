from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import datetime
from pathlib import Path
from company_match_500k.company_match import CompanyMatch
import numpy as np


def get_match():
    file = 'split2.xlsx'
    n = 10000
    n_closest = 7
    threshold = 'None'
    df_all = pd.read_excel('/home/ingest/company_match_500k_materials/splited_data/'+file , index_col=0)
    df_all.rename(columns={'company_name':'name'}, inplace=True)
    company_match = CompanyMatch()
    for g, df in df_all.groupby(np.arange(len(df_all)) // n):
        file_name = f"{file.split('.')[0]}_batch{str(g)}.xlsx"
        if Path(f"/home/ingest/company_match_500k_materials/500k_results/{file.split('.')[0]}/"+file_name).exists():
            print(f'Batch {str(g)} already exist. Continue on next.')
            continue
        print('Start matching for batch '+str(g))
        matched_df = company_match.get_distance(df, n_closest, threshold)
        matched_df.to_excel(f"/home/ingest/company_match_500k_materials/500k_results/{file.split('.')[0]}/"+file_name)
        print('file is saved as '+ file_name)
    return True


default_args = {
    'owner': 'company_match_500k',
    'start_date': datetime.datetime(2021, 6, 8, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('company_match_500k_2batch_dag',
         catchup=False,
         default_args=default_args,
         schedule_interval=None,
         ) as dag:

    get_match = PythonOperator(task_id='get_match', python_callable=get_match)

get_match