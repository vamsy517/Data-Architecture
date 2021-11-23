import pandas_gbq
import pandas as pd
import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import bigquery_storage
from sqlalchemy import create_engine 
import numpy as np
import pytz

class Updater():
    """
    This class updates tables and metadata data to project-pulldata
    Parameters: 
        project_id: project id of the project in GBQ
        dataset: name of the dataset
        table: name of the table
        df: dataframe with updated data
        entity_id_col: entity indicator column
        ids_datapoints: datapoint id columns
        ids_entity: list of ids for entities which will be updated
    """
    
    # initialize the class
    def __init__(self, project_id, dataset, table, df, entity_id_col, ids_datapoints, ids_entity):
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.df = df
        self.entity_id_col = entity_id_col
        self.ids_datapoints = ids_datapoints
        self.ids_entity = ids_entity
        # create connection to GBQ and Postgre
        self.postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
        self.gbq_client, self.bqstorageclient = self.get_credentials(self.project_id)
        

    def get_credentials(self, project_id):
        ''' 
            Create a BigQuery client and authorize the connection to BigQuery
            Input:  
                    project_id: project-pulldata
            Output:
                    return big query client
        '''
        keyfile='/home/ingest/credentials/gbq_credentials.json'
        credentials = service_account.Credentials.from_service_account_file(
            keyfile,
        )
        pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = project_id

        # Make clients.
        self.bqclient = bigquery.Client(credentials=credentials, project=project_id,)
        self.bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
        return self.bqclient, self.bqstorageclient
    
    def download_existing_data(self, dataset, table, entity_id_col, ids_entity, gbq_client, bqstorageclient):
        ''' 
            Input: 
                dataset: dataset which contains the table 
                table: table with the existing data
                entity_id_col: entity indicator column
                ids_entity: list of ids for entities which will be updated
            Output:
                    return a dataframe with the existing data for the specified table
        '''
        # check list items type
        if (type(ids_entity[0]) == str):
            # cast the list as a string to pass to a sql statement
            ids_entity_str = ','.join([f"'{str(x)}'" for x in ids_entity])
        else:
            # cast the list as a string to pass to a sql statement
            ids_entity_str = ','.join([str(x) for x in ids_entity])
        query_string = f"SELECT * FROM {dataset}.{table} where {entity_id_col} in ({ids_entity_str})"
        existing_data_df = (
        	gbq_client.query(query_string)
        	.result()
        	.to_dataframe(bqstorage_client=bqstorageclient)
        )
        return existing_data_df
    
    def updated_or_not(self, old_values, new_values, last_updated_date):
        ''' 
        This function is used only as a lambda for a dataframe
            Input:
                    old_values: list of columns in the existing data
                    new_values: list of column in the updated data
                    last_updated_date: existing last updated date
            Output:
                    return update date or NaT in case the values are not changed
        ''' 
        #fix for NaT and np.nan values to be equal
        old_values = old_values.values.tolist()
        new_values = new_values.values.tolist()
        # return None if the value is NULL
        old_values_fix = [None if pd.isnull(x) else x for x in old_values]
        # The lists in GBQ are represented as string, thus in order to compare the data from GBQ and the API, we need to set the lists, coming from the API to string
        # this is done only for the comparison
        new_values_fix_list = [str(x) if type(x) is list else x for x in new_values]
        new_values_fix = [None if pd.isnull(x) else x for x in new_values_fix_list]
        
        if old_values_fix == new_values_fix:
            updated_or_not = last_updated_date
        else:
            updated_or_not = datetime.datetime.now(pytz.utc)
        return updated_or_not
    
    def get_new_data(self, df_existing_data, df_api_data, ids_datapoints):
        ''' 
        This function checks for changes in values and returns a dataframe with the updated data
            Input: 
                    df_existing_data: dataframe with the existing data
                    df_api_data: dataframe with the new data from the API
                    ids_datapoints: datapoint id columns
            Output:
                    return dataframe with the updated data
        '''
        df_merged_data = pd.merge(df_api_data, df_existing_data,
                            on=ids_datapoints, how='outer', validate="one_to_many")
        # Pandas merge has a bug where merging two integer columns results in float columns
        # To address that we find the column indexes for the columns that contains integers
        # after merge this columns will be dtype float and we will force them to int
        # base columns indexes to fix on new data to detect any intended changes in dtype
        indexes_to_fix = [i for i,item in enumerate(df_api_data.dtypes) if str(item).lower() == 'int64']
        # from the indexes get the columns names
        columns_to_fix = [column for i,column in enumerate(df_api_data.columns) if i in indexes_to_fix]
        # First dataframe in pandas merge gets suffix '_x', second gets '_y'
        new_value_cols = [item for item in df_merged_data.columns if item[-2:] == "_x"]
        new_value_cols.sort()
        old_value_cols = [item for item in df_merged_data.columns if item[-2:] == "_y"]
        old_value_cols.sort()
        # copy dataframe for transformation process
        df_merged_renamed = df_merged_data.copy()
        # ----PARTIAL UPDATE FIX----
        # make sure that if the 'new_value' value is NULL - it should be the same as the old value
        for old_val, new_val in zip(old_value_cols, new_value_cols):
            df_merged_renamed[new_val] = df_merged_renamed.apply(lambda x: x[old_val] if pd.isnull(x[new_val]) else x[new_val], axis = 1)
        # ----
        # rename only the last_updated_date column
        df_merged_renamed.columns = [item if item != 'last_updated_date' else 'last_updated_date_old' for item in df_merged_data.columns]
        df_merged_renamed['last_updated_date'] = df_merged_renamed.apply(
            lambda x : self.updated_or_not(x[old_value_cols], x[new_value_cols],
                                          x['last_updated_date_old']), axis=1)
        # drop the old columns
        df_merged_renamed = df_merged_renamed.drop(old_value_cols, axis=1)
        df_merged_renamed = df_merged_renamed.drop('last_updated_date_old', axis=1)
        # rename new columns so that they conform to existing schema
        df_merged_renamed.columns = [item if item[-2:] != "_x" else item[:-2] for item in df_merged_renamed.columns]
        # Apply the fix to the int to float bug
        for column_name in columns_to_fix:
            df_merged_renamed[column_name] = df_merged_renamed[column_name].apply(lambda x: int(np.nan_to_num(x)) if x != np.nan else x)
        return df_merged_renamed
    	
    def delete_data(self, id_list, dataset, table, entity_id_col, postgre_engine, gbq_client):
        '''
            Delete data from existing tables for specific list of IDs
            Input:
                id_list: list with updated IDs
                dataset: dataset which contains the table 
                table: table with the existing data
                entity_id_col: entity indicator column 
                postgre_engine: connection to postgree
                gbq_client: connection to gbq
            Output:
                return True if data is deleted
        '''
        # convert set of IDs to string
        converted_id_list = [element for element in list(id_list)]
        # check list items type
        if (type(converted_id_list[0]) == str):
            # cast the list as a string to pass to a sql statement
            inds_str = ','.join([f"'{str(x)}'" for x in converted_id_list])
        else:
            inds_str = ",".join([str(x) for x in converted_id_list])
        # delete rows from postgre for the input id_list:
        sql_postgre = f'delete from {dataset}."{table}" where "{entity_id_col}" in ({inds_str})'
        postgre_engine.execute(sql_postgre)
        print(f"Delete rows from {dataset}.{table} data table from Postgre")
        # delete rows from GBQ for the input id_list:
        sql_gbq = (f'delete from `{dataset}.{table}` where {entity_id_col} in ({inds_str})')
        query_job = gbq_client.query(sql_gbq)  # API request
        query_job.result()  # Waits for statement to finish
        print(f"Delete rows from {dataset}.{table} data table from GBQ")      
        return True
    
    def upload_data(self, dataframe, dataset, table, postgre_engine, project_id):
        ''' 
            Upload dataframe to database (postgre/GBQ)
            Input:
                dataframe: dataframe to upload
                dataset: dataset which contains the table 
                table: table with the existing data
                postgre_engine: connection to Postgre
                project_id: project ID
            Output:
                return True if the upload is successfull
        '''
        dataframe.to_sql(f'{table}',schema=dataset.lower(),
                          con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
        print(f"Table {dataset}.{table} is successfully updated in Postgre")
        dataframe.to_gbq(destination_table=f'{dataset}.{table}',
                          project_id = project_id, if_exists = 'append') 
        print(f"Table {dataset}.{table} is successfully updated in GBQ")
        return True
         
    def delete_metadata(self, dataset, table, postgre_engine, gbq_client):
        ''' 
            Delete data for specific dataset and table from Metadata table in Postgre and BigQuery
            Input:
                dataset: specific dataset
                table: specific table
                postgre_engine: connection to Postgre
                gbq_client: connection to GBQ
            Output:
                return True if the data for specific dataset and table from Metadata table in Postgre and GBQ is deleted
        '''
        dataset_s = "'" + dataset + "'"
        table_s = "'" + table + "'"
        # delete rows from Postgre for the specific dataset and table:
        sql_meta = f'delete from metadata."MetaData_V1" where "dataset" = {dataset_s} and "table" = {table_s}'
        postgre_engine.execute(sql_meta)
        print(f"Delete rows from MetaData.MetaData_V1 table from Postgre for table {table} in dataset {dataset}")
        # delete rows from GBQ for the specific dataset and table:
        sql_gbq_meta = (f'delete from `MetaData.MetaData_V1` where dataset = "{dataset}" and table = "{table}"')
        query_job = gbq_client.query(sql_gbq_meta)  # API request
        query_job.result()  # Waits for statement to finish
        print(f"Delete rows from MetaData.MetaData_V1 table from GBQ for table {table} in dataset {dataset}")
        return True         
        
    def update_metadata(self, df_updated, dataset, table, postgre_engine, gbq_client, project_id):
        ''' 
            Update data for specific dataset and table from Metadata table in Postgre and BigQuery
            Input:
                df_updated: dataframe, containing the updated data
                dataset: specific dataset
                table: specific table
                postgre_engine: connection to Postgre
                gbq_client: connection to GBQ
                project_id: Project ID in GBQ
            Output:
                return True if the data for specific dataset and table from Metadata table in Postgre and GBQ is updated
        '''
        # sql statement, which selects everything from the Metadata table in GBQ
        sql = "select * from `project-pulldata.MetaData.MetaData_V1`"
        # create a dataframe, containing the metadata table
        df_meta = pandas_gbq.read_gbq(query = sql)
        # create a dataframe, containing data only for the specific dataset table
        df_selected = df_meta[(df_meta['dataset'] == dataset) & (df_meta['table'] == table)]
        # get set of deleted columns
        deleted_columns = set(df_selected['column']) - set(df_updated.columns)
        # create a copy of the dataframe
        df_meta_updated = df_selected.copy()
        # for each deleted column
        for column in deleted_columns:
            # update deleted column
            df_meta_updated.loc[df_meta_updated['column'] == column, 'deleted'] = True
        # update update_date with current date
        df_meta_updated[['update_date']] = datetime.datetime.now(pytz.utc)
        # check if there are any new columns in the updated data
        new_columns = set(df_updated.columns) - set(df_meta_updated['column'])
        # for each new column
        for column in new_columns:
            # create a new row for the new column
            df_new_row = pd.DataFrame({'dataset':[dataset], 'table':[table], 
                                       'column' :[column],
                                       'create_date' : [datetime.datetime.now(pytz.utc)],
                                       'update_date' : [datetime.datetime.now(pytz.utc)],
                                       'deleted' : [False]})
            df_meta_updated = pd.concat([df_meta_updated,df_new_row])
        # reset the index
        df_meta_updated.reset_index()
        # delete the current metadata for the specific dataset and table
        self.delete_metadata(dataset, table, postgre_engine, gbq_client)
        # upload metadata to Postgre
        df_meta_updated.to_sql('MetaData_V1',schema='metadata',
                          con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
        print(f"Metadata for dataset {dataset} and table {table} is successfully updated in Postgre")
        # upload metadata to GBQ
        df_meta_updated.to_gbq(destination_table='MetaData.MetaData_V1',
                          project_id = project_id, if_exists = 'append')
        print(f"Metadata for dataset {dataset} and table {table} is successfully updated in GBQ")
        return True
    
    def update_data(self):
        ''' 
            Main function to execute update
            Output:
                return True if table is updated
        '''
        postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
        gbq_client, bqstorageclient = self.get_credentials(self.project_id)
        # Download existing table
        df_existing = self.download_existing_data(self.dataset, self.table, self.entity_id_col, self.ids_entity, gbq_client, bqstorageclient)
        # Perform checks and get dataframe to upload
        df_to_upload = self.get_new_data(df_existing, self.df, self.ids_datapoints)
        # reinitialize postgre and gbq connections, in case they died
        postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
        gbq_client, bqstorageclient = self.get_credentials(self.project_id)
        self.delete_data(self.ids_entity, self.dataset, self.table, self.entity_id_col, postgre_engine, gbq_client)
        self.upload_data(df_to_upload, self.dataset, self.table, postgre_engine, self.project_id)
        self.update_metadata(df_to_upload, self.dataset, self.table, postgre_engine, gbq_client, self.project_id)
        return True