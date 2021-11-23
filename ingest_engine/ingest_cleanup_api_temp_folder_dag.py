from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
import os
import glob
import math
import subprocess


def convert_size(size_bytes: int) -> str:
    """
    Convert bytes to readable format.
    :param size_bytes: Integer number for bytes
    :return: Converted bytes in format ("B", "KB", "MB", "GB" or "TB")
    """
    # if 0 bytes return "0B"
    if size_bytes == 0:
        return "0B"
    # set with size suffix
    size_name = ("B", "KB", "MB", "GB", "TB")
    # calculation to convert bytes to bigger size format("KB", "MB" ...)
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    # return formatted result
    return "%s %s" % (s, size_name[i])


def delete_files() -> bool:
    """
    Delete files that are stored for download from API from temp files folder
    :return: True if successful
    """
    # add chmod 777 to folder 
    command = 'sudo chmod -R 777 /home/data_hub_api/Postgre_API/result_files/'
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    # get all files in the folder
    directory = '/home/data_hub_api/Postgre_API/result_files/'
    files = glob.glob(directory + '*')
    # create a log with information about the deleted files size and files number with time on delete operation
    with open('/home/ingest/logs/cleanup_log.csv','a') as f:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        mb_clean = convert_size(sum(os.path.getsize(directory + f)
                                    for f in os.listdir(directory)
                                    if os.path.isfile(directory + f)))
        f.write(str(curr_time) + '|' + mb_clean + ' were deleted successfully.|' + str(len(files)) +
                ' file(s) were deleted successfully.\n')
    for f in files:
        os.remove(f)
    print('Temporary files were removed successfully from /home/data_hub_api/Postgre_API/result_files/.')
    return True


default_args = {
        'owner': 'postgre_api',
        'start_date': datetime.datetime(2020, 10, 8, 5, 1, 00),
        'concurrency': 1,
        'retries': 0
}
dag = DAG('delete_files_postgre_api', description='Delete temporary files from API temp folder',
          schedule_interval='0 1 * * *',
          default_args=default_args, catchup=False)
cleanup = PythonOperator(task_id='delete_files', python_callable=delete_files, dag=dag)
cleanup
