from datetime import datetime


def log_update(log_file: str, dataset: str, table: str, process: bool, lst_ids=None) -> bool:
    """
    Log the update/replacement of table in dataset
    :param log_file: destination log file
    :param dataset: specific dataset
    :param table: specific table
    :param process: True: if there is new data to update/replace
                    False: if there is no new data
    :param lst_ids: list of survey IDs, for which there is either no report or statistics
    :return: True if the logging is successful
    """
    with open(log_file, 'a') as fl:
        curr_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # set message to be empty for all tables except Reports and Statistics
        message = ""
        # set message to hold message for missing reports
        if table == 'Reports':
            message = 'There are no reports for the following ids: '
        # set message to hold message for missing statistics
        elif table == 'Statistics':
            message = 'There are no statistics for the following ids: '
        # check if the table is uploaded successfully
        if process:
            # check if there is non-empty list of IDs
            if lst_ids is not None:
                # log list of ids
                converted_id_list = [str(element) for element in lst_ids]
                ids_str = ",".join(converted_id_list)
                fl.write(str(curr_time) + '|' + dataset + '.' + table + '|' + message + ids_str +
                         '. Replaced successfully.\n')
            else:
                # log successful replacement
                fl.write(str(curr_time) + '|' + dataset + '.' + table + '|Replaced successfully.\n')
        else:
            fl.write(str(curr_time) + '|' + dataset + '.' + table + '|There is no data, skipping replacement.\n')
    print(f'Processing for table {table} in dataset {dataset} logged successfully.')
    return True
