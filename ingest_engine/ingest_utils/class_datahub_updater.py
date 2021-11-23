import pandas as pd
import datetime
from sqlalchemy import create_engine
import numpy as np
import pytz
from ingest_utils.database_gbq import get_gbq_table, get_gbq_clients, upload_table_to_gbq
from ingest_utils.database_postgre import upload_table_to_postgre
from ingest_utils.constants import POSTGRE_AUTH, PULLDATA_PROJECT_ID
from ingest_utils.metadata import update_metadata


class Updater:
    """
    This class updates tables and metadata data to Postgre and GBQ
    """
    # initialize the class
    def __init__(self, dataset: str, table: str, df: pd.DataFrame, entity_id_col: str, ids_datapoints: list,
                 ids_entity: list, upload_to_postgre: bool = True, project_id: str = PULLDATA_PROJECT_ID):
        """
        Class initialization
        :param dataset: name of the dataset
        :param table: name of the table
        :param df: dataframe with updated data
        :param entity_id_col: entity indicator column
        :param ids_datapoints: datapoint id columns
        :param ids_entity: list of ids for entities which will be updated
        :param upload_to_postgre: True if data needs to be uploaded to postgre
        :param project_id: project id of the project in GBQ
        """
        self.postgre_auth = POSTGRE_AUTH
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.df = df
        self.entity_id_col = entity_id_col
        self.ids_datapoints = ids_datapoints
        self.ids_entity = ids_entity
        self.upload_to_postgre = upload_to_postgre

    def download_existing_data(self) -> pd.DataFrame:
        """
        Download data from existing table in GBQ
        :return: a dataframe with the existing data for the specified table
        """
        # check list items type
        if type(self.ids_entity[0]) == str:
            # cast the list as a string to pass to a sql statement
            ids_entity_str = ','.join([f"'{str(x)}'" for x in self.ids_entity])
        else:
            # cast the list as a string to pass to a sql statement
            ids_entity_str = ','.join([str(x) for x in self.ids_entity])
        query_string = f"SELECT * FROM {self.dataset}.{self.table} where {self.entity_id_col} in ({ids_entity_str})"
        df_existing_data = get_gbq_table(self.project_id, query_string)
        return df_existing_data

    @staticmethod
    def updated_or_not(old_values: pd.Series, new_values: pd.Series, last_updated_date: datetime) -> datetime:
        """
        This function is used only as a lambda for a dataframe
        :param old_values: list of columns in the existing data
        :param new_values: list of column in the updated data
        :param last_updated_date: existing last updated date
        :return: update date or NaT in case the values are not changed
        """
        # fix for NaT and np.nan values to be equal
        old_values = old_values.values.tolist()
        new_values = new_values.values.tolist()
        # return None if the value is NULL
        old_values_fix = [None if pd.isnull(x) else x for x in old_values]
        # The lists in GBQ are represented as string, thus in order to compare the data from GBQ and the API, we need
        # to set the lists, coming from the API to string
        # this is done only for the comparison
        new_values_fix_list = [str(x) if type(x) is list else x for x in new_values]
        new_values_fix = [None if pd.isnull(x) else x for x in new_values_fix_list]
        if old_values_fix == new_values_fix:
            updated_or_not = last_updated_date
        else:
            updated_or_not = datetime.datetime.now(pytz.utc)
        return updated_or_not

    def get_new_data(self, df_existing_data: pd.DataFrame) -> pd.DataFrame:
        """
        This function checks for changes in values and returns a dataframe with the updated data
        :param df_existing_data: dataframe with the existing data
        :return: datapoint id columns
        """
        df_merged_data = pd.merge(self.df, df_existing_data,
                                  on=self.ids_datapoints, how='outer', validate="one_to_many")
        # Pandas merge has a bug where merging two integer columns results in float columns
        # To address that we find the column indexes for the columns that contains integers
        # after merge this columns will be dtype float and we will force them to int
        # base columns indexes to fix on new data to detect any intended changes in dtype
        indexes_to_fix = [i for i, item in enumerate(self.df.dtypes) if str(item).lower() == 'int64']
        # from the indexes get the columns names
        columns_to_fix = [column for i, column in enumerate(self.df.columns) if i in indexes_to_fix]
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
            df_merged_renamed[new_val] = df_merged_renamed.apply(
                lambda x: x[old_val] if pd.isnull(x[new_val]) else x[new_val], axis=1)
        # ----
        # rename only the last_updated_date column
        df_merged_renamed.columns = [item if item != 'last_updated_date' else 'last_updated_date_old' for item in
                                     df_merged_data.columns]
        df_merged_renamed['last_updated_date'] = df_merged_renamed.apply(
            lambda x: self.updated_or_not(x[old_value_cols], x[new_value_cols],
                                          x['last_updated_date_old']), axis=1)
        # drop the old columns
        df_merged_renamed = df_merged_renamed.drop(old_value_cols, axis=1)
        df_merged_renamed = df_merged_renamed.drop('last_updated_date_old', axis=1)
        # rename new columns so that they conform to existing schema
        df_merged_renamed.columns = [item if item[-2:] != "_x" else item[:-2] for item in df_merged_renamed.columns]
        # Apply the fix to the int to float bug
        for column_name in columns_to_fix:
            df_merged_renamed[column_name] = df_merged_renamed[column_name].apply(
                lambda x: int(np.nan_to_num(x)) if x != np.nan else x)
        return df_merged_renamed

    def delete_data(self) -> bool:
        """
        Delete data from existing tables for specific list of IDs
        :return: True if data is deleted
        """
        postgre_engine = create_engine(self.postgre_auth)
        gbq_client, gbq_storage_client = get_gbq_clients(self.project_id)
        # convert set of IDs to string
        converted_id_list = [element for element in list(self.ids_entity)]
        # check list items type
        if type(converted_id_list[0]) == str:
            # cast the list as a string to pass to a sql statement
            inds_str = ','.join([f"'{str(x)}'" for x in converted_id_list])
        else:
            inds_str = ",".join([str(x) for x in converted_id_list])
        # delete rows from postgre for the input id_list:
        if self.upload_to_postgre:
            sql_postgre = f'delete from {self.dataset}."{self.table}" where "{self.entity_id_col}" in ({inds_str})'
            postgre_engine.execute(sql_postgre)
            print(f"Delete rows from {self.dataset}.{self.table} data table from Postgre")
        # delete rows from GBQ for the input id_list:
        sql_gbq = f'delete from `{self.dataset}.{self.table}` where {self.entity_id_col} in ({inds_str})'
        query_job = gbq_client.query(sql_gbq)  # API request
        query_job.result()  # Waits for statement to finish
        print(f"Delete rows from {self.dataset}.{self.table} data table from GBQ")
        return True

    def update_data(self) -> bool:
        """
        Main function to execute update
        :return: True if table is updated
        """
        # Download existing table
        df_existing = self.download_existing_data()
        # Perform checks and get dataframe to upload
        df_to_upload = self.get_new_data(df_existing)
        self.delete_data()
        upload_table_to_gbq(self.project_id, df_to_upload, self.dataset, self.table)
        if self.upload_to_postgre:
            upload_table_to_postgre(self.postgre_auth, df_to_upload, self.dataset, self.table)
        update_metadata(self.project_id, self.postgre_auth, df_to_upload, self.dataset, self.table)
        return True
