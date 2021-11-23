from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
import pathlib
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import bigquery_storage

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
    return yesterday

# start by creating methods for preprocessing data
# first one loads the table generated in bigquery and downloaded to the specified folder
def get_raw_data(root_dir, date):
    # read the csv and drop the unnamed col
    raw_df = pd.read_csv(root_dir + f'raw_data/users_and_features_{date}.csv')
    raw_df = raw_df.drop(['Unnamed: 0', 'user_id_1', 'user_id_2', 'user_id_3', 'user_id_4',
                         'user_id_5', 'user_id_6', 'user_id_7', 'user_id_8', 'user_id_9', 'segments'], axis = 1)
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

def load_predictions(root_dir, domain, yesterday_date):
    preds_file = f'{root_dir}latest_segments/{domain}_latest_pred.csv'
    predictions_df = pd.read_csv(preds_file)
    # assert that the file has been created today
    print(f'Asserting that the predictions file for {domain} contains predictions for yesterday.')
    assert len(predictions_df['date'].unique())
    assert predictions_df['date'].unique()[0] == yesterday_date
    predictions_df = predictions_df.drop(['Unnamed: 0'], axis = 1)
    return predictions_df

def aggregate_by_cluster(features_df, predictions_df, yesterday_date):
    joined_df = features_df.merge(predictions_df, how='left', on = 'user_id')
    aggregated_df_mean = joined_df.groupby('cluster').mean()
    aggregated_df_count = joined_df.groupby('cluster').count().iloc[:,1]
    aggregated_df_count.rename("count", inplace = True)
    aggregated_df = pd.DataFrame(aggregated_df_count).merge(aggregated_df_mean, how='left', on = 'cluster')
    aggregated_df['date'] = datetime.strptime(yesterday_date, '%Y-%m-%d')
    return aggregated_df

def get_industry_data(root_dir, domain, yesterday_date, bqclient, bqstorageclient):
    # get the dat of 1 month ago, so that we can download correctly time-framed data
    month_ago = datetime.strftime(datetime.now() - timedelta(31), '%Y-%m-%d')
    # download the clearbit data
    clearbit_filename = pathlib.Path(root_dir + f'raw_data/Raw_Clearbit_Data_{yesterday_date}.csv')
    # for reruns - check if file already exists and don't download it again
    if clearbit_filename.is_file():
        print(f"Using already downloaded Raw_Clearbit_Data data for {yesterday_date}")
    else:
        print(f"Starting download for Raw_Clearbit_Data data for {yesterday_date}")
        clearbit_query_string = f"""
SELECT * from (
SELECT
user_id,
properties.company.name,
properties.company.category.industry,
properties.company.category.industryGroup,
properties.company.category.subIndustry,
properties.company.category.sector,
# get only the MOST RECENT industry / company for a user
ROW_NUMBER() OVER (partition by user_id ORDER BY time DESC) AS rownum
from Segmentation.Raw_Clearbit_Data
WHERE DATE_TRUNC(CAST(time AS DATE),DAY) >= "{month_ago}" and DATE_TRUNC(CAST(time AS DATE),DAY) <= "{yesterday_date}"
# exclude nulls so that WHEN WE DO have a company at least once per person, we don't ALSO count the null as a new company
and properties.company.name is not null
# also exclude where the industry is null
and properties.company.category.industry is not null
and properties.client.user_agent not like '%bot%'
and properties.client.user_agent not like '%Google Web Preview%')
where rownum = 1
"""
        clearbit_df = (
            bqclient.query(clearbit_query_string)
            .result()
            .to_dataframe(bqstorage_client=bqstorageclient)
        )
        clearbit_df = clearbit_df.drop(['rownum'], axis = 1)
        clearbit_df.to_csv(clearbit_filename)
    # also download the nsmg_industries data
    nsmg_industries_filename = pathlib.Path(root_dir + f'raw_data/NSMGIndustryCompany.csv')
    if nsmg_industries_filename.is_file():
        print(f"Using already downloaded NSMGIndustryCompany data.")
    else:
        print(f"Starting download for NSMGIndustryCompany data.")
        nsmg_table = bigquery.TableReference.from_string(f"project-pulldata.Audience.NSMGIndustryCompany")
        nsmg_rows = bqclient.list_rows(nsmg_table)
        nsmg_df = nsmg_rows.to_dataframe(bqstorage_client=bqstorageclient)
        nsmg_df.to_csv(nsmg_industries_filename)
    # lastly, repeat for wishlist industries
    wishlist_industries_filename = pathlib.Path(root_dir + f'raw_data/wishlist_companies.csv')
    if wishlist_industries_filename.is_file():
        print(f"Using already downloaded wishlist_companies data.")
    else:
        print(f"Starting download for wishlist_companies data.")
        wishlist_table = bigquery.TableReference.from_string(f"project-pulldata.Segmentation.wishlist_companies")
        wishlist_rows = bqclient.list_rows(wishlist_table)
        wishlist_df = wishlist_rows.to_dataframe(bqstorage_client=bqstorageclient)
        wishlist_df.to_csv(wishlist_industries_filename)
    return True

def extract_industry_cluster_proportions(raw_df, industry_column, yesterday_date):
    # start by grouping by the requied industry column and by cluster - that way get get number of 
    # occurances per industry per cluster
    grouped_df = raw_df.groupby([industry_column, 'cluster']).count()
    # reset the index so that the grouping columns are returned to columns, instead of being multi-level indices
    grouped_df.reset_index(inplace = True)
    # get these two columns and only one of the aggregation columns - the others are redundant (same)
    grouped_df = grouped_df.iloc[:,:3]
    # rename propperly
    grouped_df.columns = ['industry','cluster', 'industry_per_cluster']
    # let's also extract the 'cluster_total' - the original data by cluster, and the 'domain_total'
    cluster_totals = raw_df.groupby('cluster').count()
    cluster_totals.reset_index(inplace = True)
    cluster_totals = cluster_totals.iloc[:,:2]
    cluster_totals.columns = ['cluster', 'cluster_total']
    # merge with the main dataframe
    grouped_df = grouped_df.merge(cluster_totals, how='left', on = 'cluster')
    # also calculate the 'domain_total' - same for every row
    grouped_df['domain_total'] = raw_df.shape[0]
    # also calculate the 'industry_per_domain' column - in a separate group by
    industry_totals = raw_df.groupby(industry_column).count()
    industry_totals.reset_index(inplace = True)
    industry_totals = industry_totals.iloc[:,:2]
    industry_totals.columns = ['industry', 'industry_total']
    # merge with the main df
    grouped_df = grouped_df.merge(industry_totals, how='left', on = 'industry')
    # finally, we can calculate the proportions
    grouped_df['industryCluster_of_cluster'] = grouped_df.apply(lambda x: x['industry_per_cluster'] / x['cluster_total'],
                                                               axis = 1)
    grouped_df['industryCluster_of_industry'] = grouped_df.apply(lambda x: x['industry_per_cluster'] / x['industry_total'],
                                                                axis = 1)
    grouped_df['cluster_of_domain'] = grouped_df.apply(lambda x: x['cluster_total'] / x['domain_total'], axis = 1)
    # finally, the weight_ratio is calculated as industryCluster_of_industry / cluster_of_domain
    # in simple terms, the ratio is 1 when we see as many members of an industry as the size of the cluster would suggest
    # if there is nothing special about the cluster; when
    # when the weight ratio is higher than 1, it means that a cluster has a higher-than-expected proportion of users from
    # the respective industry
    grouped_df['weight_ratio'] = grouped_df.apply(lambda x: x['industryCluster_of_industry'] / x['cluster_of_domain'],
                                                  axis = 1)
    # add the date as a timestamp
    grouped_df['date'] = datetime.strptime(yesterday_date, '%Y-%m-%d')
    return grouped_df

def profile_by_industry(root_dir, domain, yesterday_date, wishlist):
    # start the industry profiling by getting the list of per-user predictions for the domain
    predictions_df = load_predictions(root_dir, domain, yesterday_date)
    # and loading the raw clearbit data
    raw_clearbit = pd.read_csv(root_dir + f'raw_data/Raw_Clearbit_Data_{yesterday_date}.csv')
    raw_clearbit = raw_clearbit.drop(['Unnamed: 0'], axis = 1)
    # finally, let's join the two - we'll need that downstream
    preds_and_clearbit = predictions_df.merge(raw_clearbit, how='left', on = 'user_id')
    # now, we must diverge on the industry data that we are to join to the predictions, depending on whether we're
    # profiling for nsmg industries or for wishlist industries
    if wishlist:
        # if we are profiling for wishlist industries, we first need to read the downloaded wishlist industries file
        wishlist_taxonomy = pd.read_csv(root_dir + f'raw_data/wishlist_companies.csv')
        # then we need to set the 'organisation' parameter; the wishlist industries are split in 2 groups
        # one group for newstatesman exclusively, and another group for all others
        if domain == 'newstatesman':
            organisation = 'New Statesman'
        else:
            organisation = 'Others'
        preds_and_clearbit_and_wishlist = True
        # using the organisation parameter, we must filter the wishlist so that only the taxonomy for the required
        # domain is left
        wishlist_taxonomy = wishlist_taxonomy[wishlist_taxonomy['Organisation'] == organisation]
        # for the next step, we need to take the full clearbit+preds data, and join the filtered mvp data on that
        preds_and_clearbit_and_wishlist = preds_and_clearbit.merge(wishlist_taxonomy, how='left',
                                                                  left_on = 'name', right_on = 'name_to_seek_in_clearbit')
        
        proportions_df = extract_industry_cluster_proportions(preds_and_clearbit_and_wishlist,
                                                              'Industry_and_tier', yesterday_date)
    else:
        # for the NSMG profiling, add the NSMG taxonomy, and follow the same steps as above
        nsmg_taxonomy = pd.read_csv(root_dir + f'raw_data/NSMGIndustryCompany.csv')
        nsmg_taxonomy = nsmg_taxonomy.drop(['Unnamed: 0'], axis = 1)
        preds_and_clearbit_and_nsmg = preds_and_clearbit.merge(nsmg_taxonomy, how='left',
                                                              left_on = ['industry', 'industryGroup',
                                                                         'subIndustry', 'sector'],
                                                      right_on = ['CB_Industry', 'CB_IndustryGroup',
                                                                  'CB_subIndustry', 'CB_sector'])
        proportions_df = extract_industry_cluster_proportions(preds_and_clearbit_and_nsmg, 'NSMG_Industry', yesterday_date)
    return proportions_df

def profile_clusters(root_dir, domain, project_id, bqclient, bqstorageclient):
    yesterday_date = download_raw_data(root_dir, bqclient, bqstorageclient)
    print(f'Profiling for {domain} for the date of {yesterday_date}')
    # load the data frame
    raw_df = get_raw_data(root_dir, yesterday_date)
    # get the domain you want
    single_domain_df = get_single_domain_only(raw_df, domain)
    # now load the predictions - which should have been created a few minutes ago...
    predictions_df = load_predictions(root_dir, domain, yesterday_date)
    # join the two dataframes and perform the grouping
    behaviour_profiles = aggregate_by_cluster(single_domain_df, predictions_df, yesterday_date)
    # reset the index so that cluster is selectable easily
    behaviour_profiles.reset_index(inplace = True)
    # finally, append the table to GBQ
    print(f'Appending {domain} behavioural profile data to GBQ table.')
    # finally, save the latest predictions to csv to be queried by the API service
    behaviour_profiles.to_gbq(destination_table=f"Segmentation_{domain}.behavioural_profiles", project_id = project_id, if_exists = 'append')
    # now let's start generating the industry profiles
    # start by downloading the data - we need to download the raw clearbit data, and the NSMG and WishList taxonomies
    get_industry_data(root_dir, domain, yesterday_date, bqclient, bqstorageclient)
    # having downloaded the raw data, let's calculate the profiles
    industry_profiles = profile_by_industry(root_dir, domain, yesterday_date, wishlist = False)
    # now repeat, but for wishlist industries
    wishlist_industry_profiles = profile_by_industry(root_dir, domain, yesterday_date, wishlist = True)
    # finally, append the two industry profiles tables to GBQ
    print(f'Appending {domain} industry profile data to GBQ table.')
    industry_profiles.to_gbq(destination_table=f"Segmentation_{domain}.industry_profiles", project_id = project_id, if_exists = 'append')
    print(f'Appending {domain} wishlist industry profile data to GBQ table.')
    wishlist_industry_profiles.to_gbq(destination_table=f"Segmentation_{domain}.wishlist_industry_profiles", project_id = project_id, if_exists = 'append')
    return True

def perform_profiling():
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
    domains = ['newstatesman', 'pressgazette', 'monitors', 'citymonitor', 'investmentmonitor', 'energymonitor', 'techmonitor', 'elitetraveler']
    for domain in domains:
        profile_clusters(root_dir, domain, project_id, bqclient, bqstorageclient)

default_args = {
        'owner': 'airflow_gcp',
        'start_date':datetime(2020, 12, 1, 5, 4, 00),
        'concurrency': 1,
        'retries': 0
}
dag = DAG('segmentation_profiling', description='Perform profiling of segments on a daily basis, uploading results to BigQuery',
          schedule_interval='55 6 * * *',
          default_args = default_args, catchup=False)
profiling = PythonOperator(task_id='perform_profiling', python_callable=perform_profiling, dag=dag)
profiling