from ingest_utils.database_gbq import get_gbq_clients, get_gbq_table, upload_table_to_gbq
from ingest_utils.database_postgre import upload_table_to_postgre
from sqlalchemy import create_engine
import pandas as pd
import datetime
import pytz


METADATA_DATASET = 'MetaData'
METADATA_TABLE = 'MetaData_V1'
TODAY_DATE = datetime.datetime.now(pytz.utc)


def delete_rows_from_meta_table(project_id: str, postgre_auth: str,
                                meta_table: str, dataset: str, table: str) -> bool:
    """
    Delete rows from metadata table for specific dataset and table both in GBQ and Postgre
    :param project_id: Project ID
    :param postgre_auth: Postgre Authentication string
    :param meta_table: Table from which we will remove rows
    :param dataset: dataset which specify the deleted rows
    :param table: table which specify the deleted rows
    :return: True if specified rows are deleted successfully
    """

    gbq_client, _ = get_gbq_clients(project_id)
    postgre_engine = create_engine(postgre_auth)
    dataset_s = "'" + dataset + "'"
    table_s = "'" + table + "'"
    # we set correct names of columns because there is difference between HealthCheck and MetaData
    dataset_sql, table_sql = ('Dataset', 'Table') if meta_table == 'HealthCheck' else ('dataset', 'table')
    # delete rows from Postgre for the specific dataset and table:
    sql_meta = f'delete from {METADATA_DATASET.lower()}."{meta_table}"' \
               f'where "{dataset_sql}" = {dataset_s} and "{table_sql}" = {table_s}'
    postgre_engine.execute(sql_meta)
    print(f"Delete rows from {METADATA_DATASET}.{meta_table} table from postgre for table {table} in dataset {dataset}")
    # delete rows from GBQ for the specific dataset and table:
    sql_gbq_meta = f'delete from `{METADATA_DATASET}.{meta_table}`' \
                   f'where {dataset_sql} = "{dataset}" and {table_sql} = "{table}"'
    query_job = gbq_client.query(sql_gbq_meta)  # API request
    query_job.result()  # Waits for statement to finish
    print(f"Delete rows from {METADATA_DATASET}.{meta_table} table from GBQ for table {table} in dataset {dataset}")
    return True


def update_metadata(project_id: str, postgre_auth: str, df_updated: pd.DataFrame, dataset: str, table: str) -> bool:
    """
    Update data for specific dataset and table in Metadata table in Postgre and BigQuery
    :param project_id: Project ID
    :param postgre_auth:  Postgre Authentication string
    :param df_updated: DataFrame for which will create metadata
    :param dataset: Specific dataset for which we will create metadata
    :param table: Specific table for which we will create metadata
    :return: True if metadata is updated successfully in GBQ and Postgre
    """
    # sql statement, which selects everything from the Metadata table in GBQ
    sql = f"""select * from `{project_id}.{METADATA_DATASET}.{METADATA_TABLE}`
          where dataset = '{dataset}' and table = '{table}'"""
    # create a dataframe, containing the metadata table
    df_selected = get_gbq_table(project_id, sql)
    # get set of deleted columns
    deleted_columns = set(df_selected['column']) - set(df_updated.columns)
    # create a copy of the dataframe
    df_meta_updated = df_selected.copy()
    # for each deleted column
    for column in deleted_columns:
        # update deleted column
        df_meta_updated.loc[df_meta_updated['column'] == column, 'deleted'] = True
    # update update_date with current date
    df_meta_updated[['update_date']] = TODAY_DATE
    # check if there are any new columns in the updated data
    new_columns = set(df_updated.columns) - set(df_meta_updated['column'])
    # for each new column
    for column in new_columns:
        # create a new row for the new column
        df_new_row = pd.DataFrame({'dataset': [dataset], 'table': [table],
                                   'column': [column],
                                   'create_date': [TODAY_DATE],
                                   'update_date': [TODAY_DATE],
                                   'deleted': [False]})
        df_meta_updated = pd.concat([df_meta_updated, df_new_row])
    # reset the index
    df_meta_updated.reset_index(drop=True, inplace=True)
    # delete the current metadata for the specific dataset and table
    delete_rows_from_meta_table(project_id, postgre_auth, METADATA_TABLE, dataset, table)
    # upload metadata to Postgre
    upload_table_to_postgre(postgre_auth, df_meta_updated, METADATA_DATASET, METADATA_TABLE)
    # upload metadata to GBQ
    upload_table_to_gbq(project_id, df_meta_updated, METADATA_DATASET, METADATA_TABLE)
    return True

