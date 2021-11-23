from segmentation_report_engine.utils.gbq_queries import GBQQueries
from segmentation_report_engine.segmentation_rf_evaluation import *
from segmentation_report_engine.segmentation_report import *
import pandas as pd
from google.cloud import bigquery
from ingest_utils.database_gbq import upload_table_to_gbq


def generate_segmentation_engine_report() -> str:
    """
    Generate Segmentation Engine Report
    :return: email report if all operations are successful
    """
    print(f'Generate Segmentation Engine Report')
    # create object, containing percent_returning_visitors_per_day data
    # print('Generate percent_returning_visitors_per_day dataframe')
    obj_percent_returning_per_day = GBQQueries(MAIN_QUERY_LIST_V2 + [CALCULATE_RETURNING, CALCULATE_TOTAL,
                                                                     CALCULATE_PERCENT_RETURNING_VISITORS,
                                                                     FINAL_PERCENT_RETURNING_PER_DAY])
    # get generated percent_returning_visitors_per_day dataframe
    df_percent_returning_per_day = obj_percent_returning_per_day.requested_table
    # change type of day to date
    df_percent_returning_per_day['day'] = pd.to_datetime(df_percent_returning_per_day['day'],
                                                         format='%Y-%m-%d').dt.date
    # upload obj_percent_returning_per_day.requested_table to percent_returning_visitors_per_day
    upload_table_to_gbq(PULLDATA_PROJECT_ID, df_percent_returning_per_day, DATASET_V2, TB_PERCENT_RETURNING,
                        schema=[bigquery.SchemaField("day", "DATE"), ])
    # create object, containing RF_kmeans_comparison data
    print('Generate rf_kmeans_comparison dataframe')
    obj_rf_kmeans_comparison = GBQQueries(MAIN_QUERY_LIST_V2 + [FINAL_RF_METRICS_REPORT])
    # create object, which will be used to perform analysis
    print('Generate rf_evaluation_report dataframe')
    obj_rf_evaluation_report = RFEvaluationReport(ACTUAL_DOMAINS_V2, obj_rf_kmeans_comparison.requested_table)
    # generate RF_metrics_report
    print('Generate rf_metrics_report dataframe')
    df_rf_metrics_report = obj_rf_evaluation_report.perform_analysis()
    # upload df_rf_metrics_report to RF_metrics_report
    upload_table_to_gbq(PULLDATA_PROJECT_ID, df_rf_metrics_report, DATASET_V2, TB_RF_METRICS_REPORT)
    # create object, containing Segmentation_Engine_Report
    print('Generate Segmentation_Engine_Report dataframe')
    obj_report = SegmentationReport(ACTUAL_DOMAINS_V2, DATASET_V2, obj_percent_returning_per_day.requested_table,
                                    df_rf_metrics_report)
    # generate Segmentation_Engine_Report
    df_report = obj_report.generate_report()
    # change type of day to date
    df_report['date'] = pd.to_datetime(df_report['date'])
    # upload df_rf_metrics_report to Segmentation_Engine_Report
    upload_table_to_gbq(PULLDATA_PROJECT_ID, df_report, DATASET_V2, TB_ENGINE_REPORT)
    # prepare email message
    print('Generate email message')
    df_report_rounded = df_report.round(3)
    email_response = '<div><p>' + df_report_rounded.to_html() + '</p></div>'
    return email_response









