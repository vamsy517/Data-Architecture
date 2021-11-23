from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
import os
from pickle import load
import pathlib
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.cloud import storage

# download the latest data - from yesterday
def download_raw_data(root_dir, bqclient, bqstorageclient):
    # let's first do some housekeeping - 
    # get yesterday's date
    yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    yesterday_filename = pathlib.Path(root_dir + f'raw_data/users_and_features_{yesterday}.csv')
    # for reruns - check if file already exists and don't download it again
    if yesterday_filename.is_file():
        print(f"Using already downloaded data for {yesterday}")
    else:
        print(f"Starting download for data for {yesterday}")
        table = bigquery.TableReference.from_string(f"project-pulldata.Segmentation.Users_and_features_{yesterday}")
        rows = bqclient.list_rows(table)
        yesterday_df = rows.to_dataframe(bqstorage_client=bqstorageclient)
        yesterday_df.to_csv(yesterday_filename)
    # finally, let's do some housekeeping and delete any files other than the one downloaded for today
    # also filter for any non-csv files (ipynb checkpoints for some reason...)
    csvs_to_delete = [pathlib.Path(root_dir + 'raw_data/' + item) for item in os.listdir(root_dir + 'raw_data/') if
                       item[-4:] == '.csv']
    files_to_delete = [item for item in csvs_to_delete if item != yesterday_filename]
    print(f'Deleting files: {files_to_delete}')
    for path in files_to_delete:
        pathlib.Path.unlink(path)
    return yesterday

# start by creating methods for preprocessing data
# first one loads the table generated in bigquery and downloaded to the specified folder
def get_raw_data(root_dir, date):
    # read the csv and drop the unnamed col
    raw_df = pd.read_csv(root_dir + f'raw_data/users_and_features_{date}.csv', index_col = 'user_id')
    raw_df = raw_df.drop(['Unnamed: 0', 'user_id_1', 'user_id_2', 'user_id_3', 'user_id_4',
                         'user_id_5', 'user_id_6', 'user_id_7', 'user_id_8', 'user_id_9', ], axis = 1)
    return raw_df

# decide whether we keep all domains, or filter only for a specific group
def get_single_domain_only(df, domain):
    # if we want to keep all domains in the source file - leave df as it is
    if domain == None:
        df = df
    # otherwise, filter so that only users with visits to the required domain are left
    # with a special case for the 'monitors' group - where all monitor domains are together
    elif domain == 'monitors':
        df = df[(df['visits_to_energymonitor'] > 0) |
               (df['visits_to_investmentmonitor'] > 0) |
               (df['visits_to_citymonitor'] > 0) |
               (df['visits_to_techmonitor'] > 0)]
    else:
        col_to_check = 'visits_to_' + domain
        df = df[df[col_to_check] > 0]
    return df

# let's process the 'segments' (or any other array/list) data to workable format
# the next two methods are used together to generate that;
# 'segments_to_list' is used as a lambda within the other function below
def segments_to_list(segments_str):
    # for each string provided (containing all permutive segments),
    # remove bracket symbols and split into a list
    list_of_segs = segments_str.strip('[]').split(', ')
    # and specify the empty string as a distinct 'no_segments' segment
    list_of_segs = [item if item != '' else 'no_segments' for item in list_of_segs]
    return list_of_segs

# load and apply the binerizer, alongside the lambda above
def get_final_binarization(orig_df, root_dir, domain):
    # feed the lambda with the segmetns string and the list of segments to keep
    # to avoid view vs copy randomness...
    df = orig_df.copy()
    df['list_of_segments'] = df.apply(lambda x: segments_to_list(x['segments']), axis = 1)
    # check if a fit final binerizer already exists, and if so, use it:
    binerizer_name = pathlib.Path(root_dir + domain + '/final_binerizer.pkl')
    binerizer = load(open(binerizer_name, 'rb'))
    # let's add a prefix to the column names after binarization - vanilla classes are unreadable
    col_names = ['Permutive_seg_' + class_seg for class_seg in binerizer.classes_]
    # and create the dataframe with the correct indices
    ohe_df = pd.DataFrame(binerizer.transform(df['list_of_segments']),
                       columns=col_names,
                       index=df['list_of_segments'].index)
    # drop redundant cols and concatenate one-hot encoded data to the original dataframe
    df = df.drop(['segments', 'list_of_segments'], axis= 1)
    encoded_df = pd.concat([df, ohe_df], axis = 1)
    return encoded_df

# the 'average_virality' column contains some nans as we discard home pages, about us pages, etc.
# we should impute those - replacing nans with 1 (meaning 'average' page)
# TODO: this should work as a column-agnostic function, not only for virality and provided replacement
def impute_average_virality(df, root_dir, virality_placeholder):
    # report how many rows you're imputing
    rows = df[df.isnull().any(axis=1)].shape[0]
    print(f"Changing 'average_virality' values from 'nan' to '{virality_placeholder}' for a total of {rows} rows.")
    # replace nans with the provided parameter
    df = df.fillna(virality_placeholder)
    return df

# the second-to-last step is to drop some features (that are found to give the least impact?)
def select_features(df, root_dir, domain, features_to_keep):
    # first we drop the featuers we've determined not to use
    if features_to_keep is None:
        selected_df = df
    else:
        selected_df = df[features_to_keep]
    # return the data without the dropped features
    return selected_df

# the last step we have to perform is to scale the data so that all values are between 0 and 1
def get_scaled_data(df, root_dir, domain):
    scaler_name = pathlib.Path(root_dir + domain + '/scaler.pkl')
    scaler = load(open(scaler_name, 'rb'))
    # now transform the data
    scaled_array = scaler.transform(df)
    # this returns a numpy array; let's get it into a form we can read
    scaled_df = pd.DataFrame(data = scaled_array, index = df.index, columns = df.columns)
    return scaled_df

def upload_to_storage(project_id, domain, file):
    client = storage.Client(project=project_id)
    bucket = client.bucket('nsmg-segmentation')
    blob = bucket.blob(f'{domain}/latest_pred.csv')
    with open(file, 'rb') as my_file:
        blob.upload_from_file(my_file)

def predict_clusters(root_dir, domain, virality_placeholder, cols_to_drop, project_id, bqclient, bqstorageclient):
    # download the raw data
    yesterday_date = download_raw_data(root_dir, bqclient, bqstorageclient)
    print(f'Predicting for {domain} for the date of {yesterday_date}')
    # load the data frame
    raw_df = get_raw_data(root_dir, yesterday_date)
    # get the domain you want
    single_domain_df = get_single_domain_only(raw_df, domain)
    # get final binarization
    binarized_df = get_final_binarization(single_domain_df, root_dir, domain)
    # impute virality
    imputed_df = impute_average_virality(binarized_df, root_dir, virality_placeholder)
    # we skip cutting outliers - we want to predict every single user
    # select only specified features
    selected_df = select_features(imputed_df, root_dir, domain, cols_to_drop)
    # scale the data
    scaled_df = get_scaled_data(selected_df, root_dir, domain)

    # load the model
    print('Loading model')
    domain_folder_list = os.listdir(root_dir + domain)
    model_filenames = [item for item in domain_folder_list if item[-12:] == 'clusters.sav']
    assert len(model_filenames) == 1
    model_filename = model_filenames[0]
    model = pathlib.Path(root_dir + domain + '/' + model_filename)
    loaded_model = load(open(model, 'rb'))
    # predict the clusters, and add the predictions as a column to the original data
    print('Predicting data')
    predicted_clusters = loaded_model.predict(scaled_df)
    predicted_df = single_domain_df.copy()
    predicted_df['cluster'] = predicted_clusters
    predicted_df.reset_index(inplace=True)
    # leave only the user_id and predictions
    predicted_df_clean = predicted_df.loc[:, ['user_id','cluster']]
    # add the date as a datetime column
    predicted_df_clean['date'] = datetime.strptime(yesterday_date, '%Y-%m-%d')
    print(f'Appending to existing "Segmentation_{domain}.Users_Clusters" table.')
    # finally, save the latest predictions to csv to be queried by the API service
    predicted_df_clean.to_gbq(destination_table=f"Segmentation_{domain}.Users_Clusters", project_id = project_id, if_exists = 'append')
    print(f'Saving latest predictions for {domain} to /segmentation/latest_segments/{domain}_latest_pred_{yesterday_date}.csv.')
    predicted_df_clean.to_csv(f'{root_dir}latest_segments/{domain}_latest_pred.csv')
    # lastly, upload to google cloud storage:
    print(f'Uploading latest predictions for {domain} to Google Cloud Storage at /nsmg-segmentation/{domain}/latest_pred.csv.')
    upload_to_storage(project_id, domain, f'{root_dir}latest_segments/{domain}_latest_pred.csv')

    return True

def perform_clustering():
    # set up the credentials to access the GCP database
    keyfile='/home/ingest/credentials/gbq_credentials.json'
    project_id = 'project-pulldata'
    credentials = service_account.Credentials.from_service_account_file(keyfile)
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id
    # Make clients.
    bqclient = bigquery.Client(credentials=credentials, project=project_id,)
    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
    root_dir = '/home/airflow_gcp/segmentation/'
    domains = ['newstatesman', 'pressgazette', 'elitetraveler', 'monitors', 'citymonitor', 'investmentmonitor', 'energymonitor', 'techmonitor']
    # TODO - programatically extract these
    virality_placeholder = 1
    # cols to keep should be found in the file generated at previous iteration
    cols_to_keep_file = pd.read_csv(root_dir + 'features_to_keep_iter1.csv')
    # the list should itself be a list - it being a parameter to search through
    cols_to_keep = cols_to_keep_file['features_to_keep'].tolist()
    for domain in domains:
        predict_clusters(root_dir, domain, virality_placeholder, cols_to_keep, project_id, bqclient, bqstorageclient)

default_args = {
        'owner': 'airflow_gcp',
        'start_date':datetime(2020, 12, 1, 5, 4, 00),
        'concurrency': 1,
        'retries': 0
}
dag = DAG('segmentation_all_domains', description='Perform segmentation on a daily basis, uploading results to BigQuery',
          schedule_interval='30 6 * * *',
          default_args = default_args, catchup=False)
clustering = PythonOperator(task_id='perform_clustering', python_callable=perform_clustering, dag=dag)
clustering