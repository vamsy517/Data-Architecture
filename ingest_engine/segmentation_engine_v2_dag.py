from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import datetime
import requests
from segmentation.cluster_model import daily_prediction
from users_jobs_update.utils import update_users_jobs
from segmentation_report_engine.generate_segmentation_engine_report import generate_segmentation_engine_report

# Versions
data_version = 1


def run_daily_prediction():
    predictor = daily_prediction.Predictor(
        data_version,
    )
    predictor.predict_and_profile()


def reset_api_memory():
    api_key = Variable.get("API_UPDATE_KEY")
    api_address = "https://nsmg.nsmgendpoints.co.uk/segmentation"
    request_url = f"{api_address}/init?api_key={api_key}&load_models=True"
    response = requests.post(request_url)
    print(response)
    json_response = response.json()
    print(json_response)


default_args = {
    'owner': 'project-pulldata',
    'start_date': datetime.datetime(2021, 4, 25, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('segmentation_engine_v2',
         catchup=False,
         default_args=default_args,
         schedule_interval='15 5 * * *',
         ) as dag:
    
    users_jobs_update = PythonOperator(task_id='update_users_jobs', python_callable=update_users_jobs)
    predict_and_profile = PythonOperator(task_id='run_daily_prediction', python_callable=run_daily_prediction)
    reload_api = PythonOperator(task_id='reset_api_memory', python_callable=reset_api_memory)
    generate_report = PythonOperator(task_id='generate_segmentation_engine_report',
                                     python_callable=generate_segmentation_engine_report)
    email = EmailOperator(
        task_id='send_email',
        to=['petyo.karatov@ns-mediagroup.com', 'veselin.valchev@ns-mediagroup.com',
            'nina.nikolaeva@ns-mediagroup.com', 'yavor.vrachev@ns-mediagroup.com',
            'Dzhumle.Mehmedova@ns-mediagroup.com', 'emil.filipov@ns-mediagroup.com',
            'Kristiyan.Stanishev@ns-mediagroup.com', 'Aleksandar.NGeorgiev@ns-mediagroup.com'],
        subject='Daily Segmentation Monitoring Report',
        html_content="""<h3>Daily Segmentation Monitoring Report:
            </h3><div> {{ti.xcom_pull(task_ids='generate_segmentation_engine_report')}} </div>""",
        dag=dag
    )


users_jobs_update >> predict_and_profile >> [reload_api, generate_report]
generate_report >> email