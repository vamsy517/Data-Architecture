import pandas as pd


def fix_dataframe_columns(df_to_fix: pd.DataFrame) -> pd.DataFrame:
    """
    Clean all columns names to be compatible with postgres and gbq column name convention
    :param df_to_fix: DataFrame that we need to fix the column names
    :return: DataFrame with fixed column names
    """
    df = df_to_fix.copy()
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('/', '_')
    df.columns = df.columns.str.replace('-', '_')
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('[^A-Za-z0-9_]+', '')
    df.columns = ['_' + x if x[0].isdigit() else x for x in df.columns]
    return df


def iter_by_group(df: pd.DataFrame, column: str, chunk_size: int) -> pd.DataFrame:
    """
    Split DataFrame to chunks grouped by column
    :param df: DataFrame to split into chunks
    :param column: name of the column used to group by the records into chunks
    :param chunk_size: size of each chunk
    :return: generator for each chunks
    """
    chunk = pd.DataFrame()
    for _, group in df.groupby(column):
        if (chunk.shape[0] + group.shape[0]) > chunk_size:
            yield chunk
            chunk = group
        else:
            chunk = pd.concat([chunk, group])
    if not chunk.empty:
        yield chunk


