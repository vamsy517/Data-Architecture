# list of file extensions which we want to skip copying
EXCLUDED_EXTENSIONS = ['.md']
# list of files/folders which we dont need to copy to dags folder
EXCLUDED_ENTITIES = ['gd_apis_notebooks', 'deprecated']
# set the source path to the repo folder
GITHUB_PATH = '/home/NSMG-DataArchitecture/'
# Airflow DAGs folder
DAGS_FOLDER = '/home/airflow_gcp/airflow/dags/'
# Postgre API folder in Github
GITHUB_POSTGRE_API_FOLDER = 'APIs/flaskrestplus_postgre_api/'
# Airflow Postgre API folder
AIRFLOW_POSTGRE_API_FOLDER = '/home/data_hub_api/Postgre_API/'
