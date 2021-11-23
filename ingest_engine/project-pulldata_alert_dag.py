from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime

from ingest_utils.database_gbq import get_gbq_table


def flag_alert(**context):
    flag_file = '/home/ingest/logs/flag.csv'
    email_date_file = '/home/ingest/logs/last_email_date.csv'
    query_string = 'SELECT count(*) FROM `permutive-1258.global_data.{table}` WHERE time >= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -30 MINUTE)'
    clearbit_events_total = get_gbq_table('project-pulldata', query_string.format(table='clearbit_events')).iloc[0][0]
    pageview_events_total = get_gbq_table('project-pulldata', query_string.format(table='pageview_events')).iloc[0][0]
    with open(flag_file, "r") as f:
        flag = f.read()
    with open(email_date_file, "r") as f:
        last_mail_time = f.read()
    if last_mail_time != '':
        timedelta = datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S') - datetime.strptime(last_mail_time, '%Y-%m-%d %H:%M:%S')
        timedelta = int(timedelta.total_seconds() // 3600)

    else:
        timedelta = 42
    if clearbit_events_total == 0 or pageview_events_total == 0:
        if flag == 'yellow' or flag == 'red':
            flag = 'red'
        else:
            flag = 'yellow'
            print('flag is yellow no data in the last 30 min')
    else:
        flag = 'green'
        print('all good flag is green')

    with open(flag_file, 'w') as fl:
        fl.write(flag)
    if flag == 'red' and (timedelta > 24 or timedelta == 42):
        if timedelta != 42:
            message = '24 hours'
            print(f"""No Data in the last {message} for pageview and clearbit. Email sent""")

        else:
            message = '1 hours'
            print(f"""No Data in the last {message} pageview and clearbit. Email sent""")
        email = EmailOperator(task_id="email_task",
                              to=['petyo.karatov@ns-mediagroup.com', 'veselin.valchev@ns-mediagroup.com',
                                  'nina.nikolaeva@ns-mediagroup.com', 'yavor.vrachev@ns-mediagroup.com',
                                  'Dzhumle.Mehmedova@ns-mediagroup.com', 'kamen.parushev@ns-mediagroup.com',
                                  'emil.filipov@ns-mediagroup.com',
                                  'Kristiyan.Stanishev@ns-mediagroup.com', 'Aleksandar.NGeorgiev@ns-mediagroup.com'],
                              subject='Daily Segmentation Monitoring Report',
                              html_content=f"""No Data in the last {message}""",
                              dag=context.get("dag"))
        email.execute(context=context)
        with open(email_date_file, 'w') as fl:
            fl.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    if timedelta > 24 and timedelta != 42:
        with open(email_date_file, 'w') as fl:
            fl.write('')
    return True


default_args = {
    'owner': 'project-pulldata_Pardot_Prospects_data_update_B2C',
    'start_date': datetime(2020, 11, 8, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_alert_dag',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/30 * * * *',
         ) as dag:
    update_prospects = PythonOperator(task_id='flag_alert', python_callable=flag_alert)

update_prospects
