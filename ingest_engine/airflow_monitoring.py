import pandas_gbq
import pandas as pd
import datetime
from google.oauth2 import service_account
from sqlalchemy import create_engine


class GBQ_Uploader():
    """
    This class uploads tables and metadata data to project-pulldata
    Parameters: project_id
    """
    
    # initialize the class with the provided porject_id and get credentials for BigQuery
    def __init__(self, project_id):
        self.project_id = project_id
        self.get_credentials(self.project_id)
        self.engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')

    
    # get credentials for BigQuery
    def get_credentials(self, project_id):
        keyfile='/home/ingest/credentials/gbq_credentials.json'
        credentials = service_account.Credentials.from_service_account_file(
            keyfile,
        )
        pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = project_id
        return True
    
    # checks whether the specific table already exists in the specific dataset
    # if yes, skip upload and log the event in the processing results log file
    def skipping_upload(self, dataset, table):
        print(f'This table ({dataset}.{table}) already exists. Skipping upload.')
        with open ('/home/ingest/logs/processing_results.csv', 'a') as fl:
            curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fl.write(str(curr_time) + '|' + f'{dataset}.{table}' + '|' + str('Already exists. Skipping upload.') + '\n')
        return True

    # upload the specific table to the specific dataset in BigQuery
    # log the event in the processing results log file
    def upload(self, dataframe, dataset, table, project_id, engine):
        dataframe.to_gbq(destination_table=dataset+'.'+str(table), project_id = project_id, if_exists = 'fail')
        print(f'Table ({dataset}.{table}) uploaded successfully to GBQ.')
        dataframe.to_sql(table,schema=dataset.lower(), con=engine,method='multi', chunksize = 100000)
        print(f'Table ({dataset}.{table}) uploaded successfully to Postgre.')
        with open ('/home/ingest/logs/processing_results.csv', 'a') as fl:
            curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fl.write(str(curr_time) + '|' + f'{dataset}.{table}' + '|' + str('Uploaded successfully.') + '\n')    
        return True
    
    # get list of existing tables from BigQuery
    def get_tables(self, dataset):
        sql = 'SELECT * FROM '+dataset+'.INFORMATION_SCHEMA.TABLES'
        tables_list = pd.read_gbq(sql)['table_name'].tolist()
        return tables_list
    
    # update MetaData.MetaData_V1 table with the metadata of the specific table and dataset
    def update_metadata(self,dataset, table, dataframe_to_upload,project_id, engine):
        sql = 'SELECT * FROM MetaData.MetaData_V1'
        # get the metadata table
        current_meta_df = pd.read_gbq(sql)
        # create id by concatenating the dataset, table and column names
        current_meta_df['dt_tb_col'] = current_meta_df['dataset'] + current_meta_df['table'] + current_meta_df['column']
        # create a list of ids for the existing sets of datasets,tables and columns
        id_list = current_meta_df['dt_tb_col'].tolist()
        # create an empty dataframe, which will contain the new metadata
        meta_to_upload_df=pd.DataFrame()
        # check each column of the table, which will be uploaded to BigQuery
        for column in dataframe_to_upload.columns.tolist():
            # create id for the new table by concatenating dataset, table and column names
            id_to_check = dataset+table+column
            # check if the id for the new table already exists in the list of current ids 
            if id_to_check in id_list:
                pass
                # todo: update only Update_time in current metadata
            else:
                # create a new dataframe, containing the metadata of the new table
                meta_to_upload_df = pd.concat([meta_to_upload_df,pd.DataFrame({'dataset':[dataset], 'table':[table], 
                            'column' :[column],
                            'create_date' : [datetime.datetime.utcnow()],
                            'update_date' : [datetime.datetime.utcnow()],
                            'deleted' : [False]})],ignore_index=True)
        # upload the metadata dataframe to BigQuery
        meta_to_upload_df.to_gbq(destination_table='MetaData.MetaData_V1', project_id = project_id, if_exists = 'append')
        # upload the metadata dataframe to Postgre
        meta_to_upload_df.to_sql('MetaData_V1', schema= 'metadata', con=engine,method='multi', chunksize = 100000, if_exists = 'append') 
        return True

    # uploads to BigQuery tables, provided in CSV or EXCEL format
    def process_file(self, filepath):
        # get the project_id
        project_id = self.project_id
        # get connection to Postgre
        engine = self.engine
        # log the file as being processed
        with open ('/home/ingest/logs/in_progress.csv', 'a') as fl:
            fl.write(str(filepath).split('/')[-1] + '\n')
        
        # check if the file is in CSV format
        # CSV file naming convention is as follows: <dataset>__<table>.csv
        if filepath[-3:] == 'csv':
        	# get the dataset name from filename (convention is 'dataset__tablename_timestamp.csv')
            dataset = filepath.split('/')[-1].split('__')[0]
            # get the table name from filename
            table = filepath.split('/')[-1].split('__')[1].split('_')[0]
            # get list of existing tables in the specific dataset from BigQuery
            tables_list = self.get_tables(dataset)
            # check whether the specific table already exists
            if table in tables_list:
                # skip upload of the specific table, in case it already exists
                self.skipping_upload(dataset, table)
            else:
                # read the table
                dataframe_to_upload = pd.read_csv(filepath)
                # drop unnecessary column
                if dataframe_to_upload.columns.tolist()[0] == 'Unnamed: 0':
                    dataframe_to_upload = dataframe_to_upload.drop(['Unnamed: 0'], axis = 1)
                # upload the specific table to the specific dataset in BigQuery    
                self.upload(dataframe_to_upload, dataset, table, project_id, engine)
                # update the metadata of the specific table
                self.update_metadata(dataset,table,dataframe_to_upload,project_id, engine)
        # check if the file is in EXCEL format
        # EXCEL file naming convention is as follows: <dataset>.xlsx; sheetname is the name of the table, which will to be uploaded to Bigquery
        elif filepath[-4:] == 'xlsx':
            # get the dataset name from filename (convention is 'dataset_timestamp.xlsx')
            dataset = filepath.split('/')[-1].split('_')[0]
            # read the EXCEL file
            dataframes = pd.read_excel(filepath, None)
            # get list of existing tables in the specific dataset from BigQuery
            tables_list = self.get_tables(dataset)
            # check whether each sheet of the EXCEL file exists as a table in BigQuery
            for table in dataframes.keys():
                if table in tables_list:
                    # in case the sheet already exists in BigQuery, skip upload
                    self.skipping_upload(dataset, table)
                else:
                    # in case the sheet does not exist in BigQuery, create a new dataframe
                    dataframe_to_upload = dataframes[table]
                    # upload the new table in the specific dataset in BigQuery
                    self.upload(dataframe_to_upload, dataset, table, project_id, engine)
                    # update the metadata of the specific table
                    self.update_metadata(dataset,table,dataframe_to_upload,project_id, engine)

        else:
            # report that the provided input file is not in agreed format
            print('File is not in .xlsx or .csv format. Please make sure input files are in either of those formats.')
            # log the event in the processing results log file
            with open ('/home/ingest/logs/processing_results.csv', 'a') as fl:
                curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                file = str(filepath).split('/')[-1]
                fl.write(str(curr_time) + '|' + file + '|' + str('File not in .csv or .xlsx format.') + '\n')
        # log the file as processed
        with open ('/home/ingest/logs/processed.csv', 'a') as fl:
            fl.write(str(filepath).split('/')[-1] + '\n')
        # TODO not correct logic for in_progress...
        # clean the log, containing in progress files
        with open ('/home/ingest/logs/in_progress.csv', 'w') as fl:
            fl.write('\n')
