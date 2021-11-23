from sqlalchemy import create_engine
import pandas as pd


def delete_table_from_postgre(postgre_auth: str, dataset: str, table: str) -> bool:
    """
    Delete table from Postgre
    :param postgre_auth: Postgre Authentication string
    :param dataset: dataset from which to delete the table
    :param table: table to delete
    :return: True if the table is deleted
    """
    sql = f'drop table {dataset.lower()}."{table}"'
    postgre_engine = create_engine(postgre_auth)
    postgre_engine.execute(sql)
    print(f'Table {table} is deleted from dataset {dataset} from Postgre')
    return True


def upload_table_to_postgre(postgre_auth: str, df: pd.DataFrame, dataset: str,
                            table: str, method: str = 'append') -> bool:

    """
    Upload DataFrame to Postgre table
    :param postgre_auth: Postgre Authentication string
    :param df: DataFrame to upload
    :param dataset: dataset where to upload the DataFrame in Postgre
    :param table: table where to upload the DataFrame in Postgre
    :param method: append or replace (default: append)
    :return: True if the dataframe is uploaded to GBQ
    """
    assert method in ('append', 'replace'), 'Method should be "append" or "replace"'
    postgre_engine = create_engine(postgre_auth)
    df.to_sql(table, schema=dataset.lower(),
              con=postgre_engine, method='multi', chunksize=20000, if_exists=method)
    print(f'Table {dataset}.{table} is uploaded/updated to Postgre')
    return True









