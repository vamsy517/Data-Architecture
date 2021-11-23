from segmentation_report_engine.utils.gbq_queries import GBQQueries
from segmentation_report_engine.segmentation_rf_evaluation import *
from segmentation_report_engine.segmentation_report import *
import pandas as pd
from airflow import DAG
from google.cloud import bigquery
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from ingest_utils.database_gbq import upload_table_to_gbq


def generate_segmentation_engine_report() -> str:
    """
    Generate Segmentation Engine Report
    :return: email report if all operations are successful
    """
    # define a list, containing the Segmentation versions
    versions = ['V1', 'V2']
    # define list, containing resulting dataframes
    result_list = []
    # for each version
    for version in versions:
        # assign the variables for Segmentation V1
        if version == 'V1':
            main_query_list = MAIN_QUERY_LIST_V1
            dataset = DATASET_V1
            actual_domains = ACTUAL_DOMAINS_V1
        # assign the variables for Segmentation V2
        if version == 'V2':
            main_query_list = MAIN_QUERY_LIST_V2
            dataset = DATASET_V2
            actual_domains = ACTUAL_DOMAINS_V2

        print(f'SEGMENTATION {version}')
        # create object, containing percent_returning_visitors_per_day data
        # print('Generate percent_returning_visitors_per_day dataframe')
        obj_percent_returning_per_day = GBQQueries(main_query_list + [CALCULATE_RETURNING, CALCULATE_TOTAL,
                                                                      CALCULATE_PERCENT_RETURNING_VISITORS,
                                                                      FINAL_PERCENT_RETURNING_PER_DAY])
        # get generated percent_returning_visitors_per_day dataframe
        df_percent_returning_per_day = obj_percent_returning_per_day.requested_table
        # change type of day to date
        df_percent_returning_per_day['day'] = pd.to_datetime(df_percent_returning_per_day['day'],
                                                             format='%Y-%m-%d').dt.date
        # upload obj_percent_returning_per_day.requested_table to percent_returning_visitors_per_day
        upload_table_to_gbq(PULLDATA_PROJECT_ID, df_percent_returning_per_day, dataset, TB_PERCENT_RETURNING,
                            schema=[bigquery.SchemaField("day", "DATE"), ])
        # create object, containing RF_kmeans_comparison data
        print('Generate rf_kmeans_comparison dataframe')
        obj_rf_kmeans_comparison = GBQQueries(main_query_list + [FINAL_RF_METRICS_REPORT])
        # create object, which will be used to perform analysis
        print('Generate rf_evaluation_report dataframe')
        obj_rf_evaluation_report = RFEvaluationReport(version, actual_domains, obj_rf_kmeans_comparison.requested_table)
        # generate RF_metrics_report
        print('Generate rf_metrics_report dataframe')
        df_rf_metrics_report = obj_rf_evaluation_report.perform_analysis()
        # upload df_rf_metrics_report to RF_metrics_report
        upload_table_to_gbq(PULLDATA_PROJECT_ID, df_rf_metrics_report, dataset, TB_RF_METRICS_REPORT)
        # create object, containing Segmentation_Engine_Report
        print('Generate Segmentation_Engine_Report dataframe')
        obj_report = SegmentationReport(version, actual_domains, dataset, obj_percent_returning_per_day.requested_table,
                                        df_rf_metrics_report)
        # generate Segmentation_Engine_Report
        df_report = obj_report.generate_report()
        # change type of day to date
        df_report['date'] = pd.to_datetime(df_report['date'])
        # upload df_rf_metrics_report to Segmentation_Engine_Report
        upload_table_to_gbq(PULLDATA_PROJECT_ID, df_report, dataset, TB_ENGINE_REPORT)
        # append the result to a list
        result_list.append(df_report)

    # prepare email message
    print('Generate email message')
    report_email_list = [result.round(3) for result in result_list]
    email_response = '<br>'.join(['<div><p>' + result.to_html() + '</p></div>' for result in report_email_list])
    return email_response


default_args = {
    'owner': 'dscoe_team',
    'start_date': datetime(2021, 4, 5, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('segmentation_engine_report_v1',
         catchup=False,
         default_args=default_args,
         schedule_interval='20 7 * * *',
         ) as dag:

    generate_segmentation_engine_report = PythonOperator(task_id='generate_segmentation_engine_report',
                                                         python_callable=generate_segmentation_engine_report)
    email = EmailOperator(
        task_id='send_email',
        to=['petyo.karatov@ns-mediagroup.com', 'veselin.valchev@ns-mediagroup.com',
            'nina.nikolaeva@ns-mediagroup.com', 'yavor.vrachev@ns-mediagroup.com',
            'Dzhumle.Mehmedova@ns-mediagroup.com', 'kamen.parushev@ns-mediagroup.com', 'emil.filipov@ns-mediagroup.com',
            'Kristiyan.Stanishev@ns-mediagroup.com', 'Aleksandar.NGeorgiev@ns-mediagroup.com'],
        subject='Daily Segmentation Monitoring Report',
        html_content="""<h3>Daily Segmentation Monitoring Report:
        </h3><div> {{ti.xcom_pull(task_ids='generate_segmentation_engine_report')}} </div>""",
        dag=dag
    )

generate_segmentation_engine_report >> email
