from datetime import date, datetime, timedelta


def log_last_run_date(log_file: str) -> bool:
    """
    Write in the log file the date for the last successful DAG run
    :param log_file: full path to the log file
           Example: '/home/ingest/logs/health_check_log.csv'
    :return: True when the log is updated
    """
    with open(log_file, 'w') as fl:
        curr_time = date.today() - timedelta(days=1)
        fl.write(str(curr_time))
    return True


def write_to_log(log_file: str, dataset: str, table: str, message: str) -> bool:
    """
    Write the provided message into the specified log file
    :param log_file: destination log file
    :param dataset: specific dataset where the table already exists
    :param table: specific table which already exists
    :param message: message to log
    :return: True if logging is successful
    """
    with open(log_file, 'a') as fl:
        curr_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fl.write(str(curr_time) + '|' + f'{dataset}.{table}' + '|' + str(message) + '\n')
    print(f'Processing for table {table} in dataset {dataset} logged successfully.')
    return True


