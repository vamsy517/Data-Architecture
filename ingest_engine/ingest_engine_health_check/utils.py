import pandas as pd
import scipy.stats as stats
import numpy as np
import os
from ingest_utils.database_gbq import get_gbq_table
from sql_queries.gbq_queries import QUERY_WHOLE_TABLE
from ingest_utils.constants import LOGS_FOLDER, HEALTH_CHECK_LOG, UPDATE_PROCESSING_RESULTS


def generate_column_metadata(dataset: str, table: str, df_to_check: pd.DataFrame, nr_samples: int) -> pd.DataFrame:
    """
    Create a DataFrame containing dataset, table, column, column type and sample values for this column
    for each column in a provided DataFrame
    :param dataset: specific dataset, containing the table
    :param table: specific table
    :param df_to_check: DataFrame, containing the specific table
    :param nr_samples: number of sample values
    :return: DataFrame, containing dataset, table
             and column name, column type and sample values for each column in the provided table
    """
    # Construct an empty dataframe with the following columns:
    # Dataset, Table , Column Name, Column Type, Sample Values
    df_types = pd.DataFrame(columns=('Dataset', 'Table', 'Column_Name', 'Column_Type', 'Sample_Values'))
    for (columnName, columnData) in df_to_check.iteritems():
        sample_values_list = df_to_check[columnName].sample(n=nr_samples).to_list()
        new_row = pd.Series({'Dataset': dataset,
                             'Table': table,
                             'Column_Name': columnName,
                             'Column_Type': df_to_check.dtypes[columnName],
                             'Sample_Values': "|".join([str(elem) for elem in sample_values_list])})
        df_types = df_types.append(new_row, ignore_index=True)
    return df_types


def calculate_coverage(df_table: pd.DataFrame, df_types: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate coverage for all columns in a provided table
    :param df_table: table, for which to calculate coverage
    :param df_types: table, containing column metadata
    :return: DataFrame, containing dataset, table
             and column name, column type and sample values for each column in the provided table
             and coverage for all columns
    """
    # create a copy of the summary df
    df_coverage = df_types.copy()
    # create a new column for Coverage
    df_coverage['Coverage'] = 0.0
    # for each column in the provided table
    for col in df_table.columns:
        # calculate coverage by dividing number of non-null values to number of all values
        # NOTE: Postgre not working with numpy
        coverage = df_table[col].count()/df_table[col].shape[0]
        # get the index of the column in the summary dataframe
        index = df_types.Column_Name[df_types.Column_Name == col].index[0]
        # set the calculated coverage in the summary table
        df_coverage.loc[index, 'Coverage'] = coverage.item()
    return df_coverage


def calculate_z_score_column(df: pd.DataFrame, column: str, nr_samples: int) -> list:
    """
    Calculate Z Score for specific column in the provided DataFrame
    :param df: DataFrame, containing the column, for which to calculate Z Score
    :param column: specific column, for which to calculate Z Score
    :param nr_samples: number of Z Score values to keep
    :return: returns a list of TOP number of samples tuples(value:corresponding Z-Score)
    """
    # create a dataframe with the specific column
    df_new = pd.DataFrame(df[column], columns=[column])
    # calculate Z-Score for the specific column
    df_new['Z'] = abs(stats.zscore(df[column], nan_policy='omit'))
    # sort the new dataframe by Z-Score
    df_new.sort_values(by=['Z'], inplace=True, ascending=False)
    lst = []
    # for each row from TOP number of samples
    for index in df_new.head(nr_samples).index:
        # append value : Z-Score to a list
        lst.append(str(df_new.loc[index, column]) + ' : ' + str(df_new.loc[index, 'Z']))
    return lst


def calculate_z_score_table(df_table: pd.DataFrame, df_types: pd.DataFrame, nr_samples: int) -> pd.DataFrame:
    """
    Calculate Z Score for all numeric columns in a provided DataFrame
    :param df_table: DataFrame, for which to calculate Z Score
    :param df_types: table, containing column metadata
    :param nr_samples: number of preferred sample values for Z Score
    :return: DataFrame, containing column metadata and Z score for each numeric column
    """
    # create a copy of the summary df
    df_z_score = df_types.copy()
    # create a new column for Coverage
    df_z_score['Z_Score'] = np.nan
    # get a list of all numeric columns
    numeric_columns = df_table.select_dtypes([np.number]).columns
    # for each numeric column in the table
    for col in numeric_columns:
        # calculate Z score for this column
        z_score_list = calculate_z_score_column(df_table, col, nr_samples)
        # get the index of the column in the summary dataframe
        index = df_types.Column_Name[df_types.Column_Name == col].index[0]
        # set the calculated coverage in the summary table
        df_z_score.loc[index, 'Z_Score'] = "  |  ".join(z_score_list)
    return df_z_score


def generate_health_check_table(project_id: str, dataset: str, table: str,
                         nr_sample_values: int, nr_sample_z_score_values: int) -> pd.DataFrame:
    """
    Perform health check on a specific table from a specific dataset
    :param project_id: Project ID
    :param dataset: specific dataset, which contains the table
    :param table: specific table, on which to perform health check
    :param nr_sample_values: number of preferred sample values
    :param nr_sample_z_score_values: number of preferred sample values for Z Score
    :return: a summary DataFrame, which contains dataset, table name, column name, column type,
             sample values for each column, coverage score for all columns and Z-Score for all numeric columns
    """
    # get the table, for which health check will be performed
    df_table = get_gbq_table(project_id, QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset, table=table))
    # check if the downloaded table is empty
    if df_table.shape[0] == 0:
        # create empty dataframe to return
        df_result = pd.DataFrame()
    else:
        # create a summary table for the specific table
        df_types = generate_column_metadata(dataset, table, df_table, nr_sample_values)
        # calculate coverage for all columns
        df_coverage = calculate_coverage(df_table, df_types)
        # calculate Z-Score for all numeric columns
        df_result = calculate_z_score_table(df_table, df_coverage, nr_sample_z_score_values)
    return df_result


def generate_list_of_tables() -> np.array:
    """
    Generate a list of tables, for which to perform health check using the log files, generated by each dag
    :return: a list with tables, for which to perform health check
    """
    # read health check log to find the last health check
    df_time = pd.read_csv(f'{LOGS_FOLDER}{HEALTH_CHECK_LOG}', header=None)
    df_time[0] = pd.to_datetime(df_time[0]).dt.date
    # list all log files in ingest logs folder
    lst_log_files = os.listdir(LOGS_FOLDER)
    # keep those logs, which should be processed by the health check dag
    health_check_file_list = [list_item for list_item in lst_log_files if list_item.endswith('processing_results.csv')]
    # create new df to store the final results
    df_to_check = pd.DataFrame()
    # for each file
    for file in health_check_file_list:
        # read the file with | separator
        df_log = pd.read_csv(LOGS_FOLDER + file, header=None, sep='|')
        # for the log file from commercial update, remove the unnecessary column
        if UPDATE_PROCESSING_RESULTS in file:
            df_log = df_log.drop(1, axis=1)
        # rename all columns
        df_log.columns = ['Timestamp', 'Filename', 'Message']
        # convert Timestamp string to Timestamp object
        df_log['Timestamp'] = pd.to_datetime(df_log['Timestamp'], infer_datetime_format=True)
        # get date from Timestamp object
        df_log['Date'] = df_log['Timestamp'].dt.date
        # keep only valid entries
        df_valid_log_files = df_log[(df_log['Date'] > df_time.iloc[0, 0]) &
                                    (df_log['Message'].str.endswith('successfully.'))]
        # concat all into one df
        df_to_check = pd.concat([df_to_check, df_valid_log_files])
    if df_to_check.shape[0] == 0:
        return np.array([])
    else:
        # return unique list of tables, for which to perform health check
        return df_to_check.Filename.unique()
