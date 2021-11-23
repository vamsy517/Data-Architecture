import pandas as pd
import datetime
from ingest_utils.constants import POSTGRE_AUTH, PULLDATA_PROJECT_ID, LOGS_FOLDER, PROCESSING_RESULTS, ALREADY_EXISTS_SKIP_UPLOAD
from ingest_utils.database_gbq import get_gbq_table, upload_table_to_gbq
from ingest_utils.database_postgre import upload_table_to_postgre
from ingest_utils.metadata import update_metadata
from sql_queries.gbq_queries import QUERY_DATASET_INFORMATION_SCHEMA
from ingest_utils.logging import write_to_log


class Uploader:
    """
    This class uploads tables and metadata data to Postgre and GBQ
    """
    
    # initialize the class with the provided porject_id and get credentials for BigQuery
    def __init__(self, project_id: str = PULLDATA_PROJECT_ID):
        self.project_id = project_id
        self.postgre_auth = POSTGRE_AUTH

    # uploads to BigQuery tables, provided in CSV or EXCEL format
    @staticmethod
    def read_file(filepath: str) -> (str, dict):
        """
        Read the file from filepath
        :param filepath: Full path to the file to be processed
        :return: dataset and dataframe containing the file content
        """

        # check if the file is in CSV format
        # CSV file naming convention is as follows: <dataset>__<table>.csv
        if filepath[-3:] == 'csv':
            # get the dataset name from filename (convention is 'dataset__tablename_timestamp.csv')
            dataset = filepath.split('/')[-1].split('__')[0]
            # get the table name from filename
            table = filepath.split('/')[-1].split('__')[1].split('_')[0]
            # read the table
            dataframe_to_upload = pd.read_csv(filepath)
            # remove Unnamed if exists
            dataframe_to_upload = dataframe_to_upload.loc[:, ~dataframe_to_upload.columns.str.contains('^Unnamed')]
            dataframes = {table: dataframe_to_upload}

        # check if the file is in EXCEL format
        # EXCEL file naming convention is as follows: <dataset>.xlsx;
        # sheetname is the name of the table, which will to be uploaded to Bigquery
        elif filepath[-4:] == 'xlsx':
            # get the dataset name from filename (convention is 'dataset_timestamp.xlsx')
            dataset = filepath.split('/')[-1].split('_')[0]
            # read the EXCEL file
            dataframes = pd.read_excel(filepath, None)

        else:
            # report that the provided input file is not in agreed format
            print('File is not in .xlsx or .csv format. Please make sure input files are in either of those formats.')
            # log the event in the processing results log file
            with open(f'{LOGS_FOLDER}{PROCESSING_RESULTS}', 'a') as fl:
                curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                file = str(filepath).split('/')[-1]
                fl.write(str(curr_time) + '|' + file + '|' + str('File not in .csv or .xlsx format.') + '\n')
            dataframes = {}
            dataset = ''
        return dataset, dataframes

    def process_file(self, filepath: str) -> bool:
        """
        Get file from process folder and upload the tables to GBQ and Postgre
        And update metadata
        @param filepath:  Full path to the file to be processed
        @return: True if success to upload the files
        """
        # log the file as being processed
        with open(f'{LOGS_FOLDER}in_progress.csv', 'a') as file:
            file.write(str(filepath).split('/')[-1] + '\n')
        # get dataset and file content
        dataset, dataframes = self.read_file(filepath)
        # get list of existing tables in the specific dataset from BigQuery
        tables_list = get_gbq_table(self.project_id, QUERY_DATASET_INFORMATION_SCHEMA.format(dataset=dataset))
        # check whether each sheet of the EXCEL file exists as a table in BigQuery
        for table in dataframes.keys():
            if table in tables_list:
                # in case the sheet already exists in BigQuery, skip upload
                write_to_log(f'{LOGS_FOLDER}{PROCESSING_RESULTS}', dataset, table, ALREADY_EXISTS_SKIP_UPLOAD)
            else:
                # in case the sheet does not exist in BigQuery, create a new dataframe
                dataframe_to_upload = dataframes[table]
                # upload the new table in the specific dataset in BigQuery and Postgre
                upload_table_to_gbq(self.project_id, dataframe_to_upload, dataset, table)
                upload_table_to_postgre(self.postgre_auth, dataframe_to_upload, dataset, table)
                # update the metadata of the specific table
                update_metadata(self.project_id, self.postgre_auth, dataframe_to_upload, dataset, table)

        # log the file as processed
        with open(f'{LOGS_FOLDER}processed.csv', 'a') as fl:
            fl.write(str(filepath).split('/')[-1] + '\n')
        # TODO not correct logic for in_progress...
        # clean the log, containing in progress files
        with open(f'{LOGS_FOLDER}in_progress.csv', 'w') as fl:
            fl.write('\n')
        return True
