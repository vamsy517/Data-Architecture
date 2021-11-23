import datetime
import pytz

import pandas_gbq
import pandas as pd

from google.oauth2 import service_account
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from sqlalchemy import create_engine 

import requests
from pandas.io.json import json_normalize

import datahub_updater

################################# Deprecated DAG ##################################

def get_credentials(project_id):
    ''' 
        Create a BigQuery client and authorize the connection to BigQuery
        Input:  
                project_id: project-pulldata
        Output:
                return big query client
    '''
    keyfile= '/home/ingest/credentials/gbq_credentials.json'
    credentials = service_account.Credentials.from_service_account_file(
        keyfile,
    )
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id

    client = bigquery.Client.from_service_account_json(keyfile)
    return client


def delete_table(postgre_engine, gbq_client, dataset,table):
    '''
        Delete specific table from specific dataset in Postgre and BigQuery
        Input:
            postgre_engine: connection to postgree
            gbq_client: connection to gbq
            dataset: dataset which contains the table
            table: specific table to be deleted
        Output:
            return True if table is deleted successfully
    '''
    # Deleting table from postgre
    sql = f'drop table {dataset.lower()}."{table}"'
    postgre_engine.execute(sql)
    print(f'Table {table} is deleted from dataset {dataset} from Postgre') 
    # Deleting table from BQ
    table_ref = gbq_client.dataset(dataset).table(table)
    gbq_client.delete_table(table_ref)  
    print(f'Table {table} is deleted from dataset {dataset} from BigQuery')
    return True

def upload_data(postgre_engine, project_id, dataset, table, df):
    ''' 
        Upload df data to database (postgre/GBQ) 
        Input:
            postgre_engine: connection to Postgre
            project_id: project ID
            dataset: dataset to which we upload the df
            table: specific table which will contain the df
            dataframe: dataframe to upload
        Output:
            return True if upload is successfull
    '''
    # Upload to postgre
    df.to_sql(table,schema=dataset.lower(),
                          con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
    print(f'Table {table} is successfully uploaded in the {dataset} dataset to Postgre')
    # Upload to BQ
    df.to_gbq(destination_table=f'{dataset}.{table}',project_id = project_id, if_exists = 'append')
    print(f'Table {table} is successfully uploaded in the {dataset} dataset to BigQuery')
    return True

def delete_metadata(dataset, table, postgre_engine, gbq_client):
    ''' 
        Delete data for specific dataset and table from Metadata table in Postgre and BigQuery
        Input:
            dataset: specific dataset
            table: specific table
            postgre_engine: connection to Postgre
            gbq_client: connection to GBQ
        Output:
            return True if the data for specific dataset and table from Metadata table in Postgre and GBQ is deleted
    '''
    dataset_s = "'" + dataset + "'"
    table_s = "'" + table + "'"
    # delete rows from Postgre for the specific dataset and table:
    sql_meta = f'delete from metadata."MetaData_V1" where "dataset" = {dataset_s} and "table" = {table_s}'
    postgre_engine.execute(sql_meta)
    print(f"Delete rows from MetaData.MetaData_V1 table from Postgre for table {table} in dataset {dataset}")
    # delete rows from GBQ for the specific dataset and table:
    sql_gbq_meta = (f'delete from `MetaData.MetaData_V1` where dataset = "{dataset}" and table = "{table}"')
    query_job = gbq_client.query(sql_gbq_meta)  # API request
    query_job.result()  # Waits for statement to finish
    print(f"Delete rows from MetaData.MetaData_V1 table from GBQ for table {table} in dataset {dataset}")
    return True

def update_metadata(df_updated, dataset, table, postgre_engine, gbq_client, project_id):
    ''' 
        Update data for specific dataset and table from Metadata table in Postgre and BigQuery
        Input:
            df_updated: dataframe, containing the new data
            dataset: specific dataset
            table: specific table
            postgre_engine: connection to Postgre
            gbq_client: connection to GBQ
            project_id: Project ID in GBQ
        Output:
            return True if the data for specific dataset and table from Metadata table in Postgre and GBQ is updated
    '''
    # sql statement, which selects everything from the Metadata table in GBQ
    sql = "select * from `project-pulldata.MetaData.MetaData_V1`"
    # create a dataframe, containing the metadata table
    df_meta = pandas_gbq.read_gbq(query = sql)
    # create a dataframe, containing data only for the specific dataset table
    df_selected = df_meta[(df_meta['dataset'] == dataset) & (df_meta['table'] == table)]
    # get set of deleted columns
    deleted_columns = set(df_selected['column']) - set(df_updated.columns)
    # create a copy of the dataframe
    df_meta_updated = df_selected.copy()
    # for each deleted column
    for column in deleted_columns:
        # update deleted column
        df_meta_updated.loc[df_meta_updated['column'] == column, 'deleted'] = True
    # update update_date with current date
    df_meta_updated[['update_date']] = datetime.datetime.now(pytz.utc)
    # check if there are any new columns in the updated data
    new_columns = set(df_updated.columns) - set(df_meta_updated['column'])
    # for each new column
    for column in new_columns:
        # create a new row for the new column
        df_new_row = pd.DataFrame({'dataset':[dataset], 'table':[table], 
                                   'column' :[column],
                                   'create_date' : [datetime.datetime.now(pytz.utc)],
                                   'update_date' : [datetime.datetime.now(pytz.utc)],
                                   'deleted' : [False]})
        df_meta_updated = pd.concat([df_meta_updated,df_new_row])
    # reset the index
    df_meta_updated.reset_index()
    # delete the current metadata for the specific dataset and table
    delete_metadata(dataset, table, postgre_engine, gbq_client)
    # upload metadata to Postgre
    df_meta_updated.to_sql('MetaData_V1',schema='metadata',
                      con=postgre_engine,method='multi', chunksize = 100000, if_exists = 'append')
    print(f"Metadata for dataset {dataset} and table {table} is successfully updated in Postgre")
    # upload metadata to GBQ
    df_meta_updated.to_gbq(destination_table='MetaData.MetaData_V1',
                      project_id = project_id, if_exists = 'append')
    print(f"Metadata for dataset {dataset} and table {table} is successfully updated in GBQ")

    return True


def replace_data(df, dataset, table, postgre_engine, gbq_client, project_id):
    ''' 
        Delete specific table from postgre and bq
        Upload the new df to postgre and bq
        Update metadata
        Log results
        Input:
            df: dataframe to upload
            dataset: dataset to which we upload the df
            table: specific table which will contain the df
            postgre_engine: connection to Postgre
            gbq_client: connection to BigQuery
            project_id: project ID            
        Output:
            return True if all operations are successfull
    '''
    # check if dataframe is empty
    if df.empty:
        print(f'There is no data for {dataset}.{table}. Skipping replacement.')
        log_update(dataset, table, False)
    else:
        # check if table exists in Postgre and BigQuery, if not skip deleting
        sql = 'SELECT * FROM '+ dataset+'.INFORMATION_SCHEMA.TABLES' 
        tables_list = pd.read_gbq(sql)['table_name'].tolist()
        if table in tables_list:
            # delete table CompanyListing from postgre and BigQuery
            delete_table(postgre_engine, gbq_client, dataset,table)
        # upload new table CompanyListing to postgre and bq
        upload_data(postgre_engine, project_id, dataset, table, df)
        # update the metadata for CompanyListing table
        update_metadata(df, dataset, table, postgre_engine, gbq_client, project_id)
        # log updated IDs and Tables
        log_update(dataset, table, True)
    return True


################################ End of Deprecated DAG ############################

def getCompanyListingOrDetails(api, token):
    ''' 
        Input:  
                api: GetCompanyListing or GetCompanyDetails
                token: token for the api
        Output:
                return dataframe containing the company listing or the company details
    '''
    page = 1
    url = f'http://apidata.globaldata.com/CompanyIntegratedViewNSMG/api/Content/{api}?TokenID={token}%3D&PageNumber={page}'
    response = requests.get(url).json()
    # assign the number of pages to a variable
    num_pages = response['NoOfPages']
    # instantiate an empty dataframe
    companies_df = pd.DataFrame()
    # in a loop over the 7 pages of the API results, fill the dataframe with the respective resutls
    for page in range(1, num_pages+1):
        # apply the complete API call url
        url = f'http://apidata.globaldata.com/CompanyIntegratedViewNSMG/api/Content/{api}?TokenID={token}%3D&PageNumber={page}'
        # GET the response json
        response = requests.get(url).json()
        # parse the JSON to a pandas dataframe
        curr_df = pd.DataFrame.from_dict(response['Companies'])
        # fill the dataframe above with each response
        companies_df = pd.concat([companies_df, curr_df],sort=False)
    # Set type datetime to PublishedDate because it is string
    companies_df['PublishedDate'] = pd.to_datetime(companies_df['PublishedDate'])
    companies_df['PublishedDate'] = companies_df['PublishedDate'].astype('datetime64[ns, UTC]')
    return companies_df


def get_data(list_of_ids, token, entity):
    ''' 
        Input:  
                list_of_ids: list of ID of entity
                token: auth token,
                entity: Deal or News
        Output:
                return dataframe containing all data for all IDs from list_of_ids
    '''
    
    # create df for all data from updated list
    df_entities = pd.DataFrame()
    for company_id in list_of_ids:
        # url for API Call
        url = f'http://apidata.globaldata.com/CompanyIntegratedViewNSMG/api/Content/Get{entity}Details?TokenID={token}%3D&CompanyID={company_id}'
        # get response from API Call
        response = requests.get(url).json()
        # get number of pages
        num_pages = response['NoOfPages']
        entity_df = pd.DataFrame()
        # in a loop over the  pages of the API results
        for page in range(1, num_pages + 1):
            # apply the complete API call url
            url = f'http://apidata.globaldata.com/CompanyIntegratedViewNSMG/api/Content/Get{entity}Details?TokenID={token}%3D&CompanyID={company_id}&PageNumber={page}'
            # GET the response json for entity Data
            response = requests.get(url).json()
            # parse the response to a pandas dataframe
            json_df = pd.DataFrame.from_dict(response)
            if entity == 'Deal':
                curr_df = json_normalize(json_df[entity + 's'])
            else:
                curr_df = json_normalize(json_df[entity])
            curr_df['CompanyId'] = company_id
            entity_df = pd.concat([entity_df, curr_df])
        # concatenate the company dataframe to the companies dataframe
        df_entities = pd.concat([df_entities, entity_df]) 
    return df_entities


def unnest_columns(df_to_unnest, entity_id,column_to_unnest, internal_id, additional_id = None, entity_name = None):
    ''' 
        Input:  
                df_to_unnest: dataframe which contains nested columns
                entity_id: DealID or NewsArticleId,
                column_to_unnest: name of the column which will be unnested
                internal_id: name of the column which is used as id in some tables 
                additional_id: for some tables we need to add additional CompanyId, which is set here
                entity_name: for some tables we need to add a new CompanyId 
                             and here we set whether the name will be DealCompanyId or NewsCompanyId
        Output:
                return dataframe containing the unnested column's data
    '''
    df = pd.DataFrame()
    # for each item in the dataframe
    for item in set(df_to_unnest[entity_id].tolist()):
        # keep only those rows for the specific entity
        individual = df_to_unnest[df_to_unnest[entity_id] == item]
        # if nested column value is not empty
        if not isinstance(individual[column_to_unnest].tolist()[0], type(None)) :
            # unnest the column 
            row = json_normalize(individual[column_to_unnest].tolist()[0])
            # check if internal_id is empty
            if internal_id != "":
                # append the internal id to the resulting new row
                row[internal_id] = individual[entity_id].tolist()[0]
            else:
                # append the entity id to the resulting new row
                row[entity_id] = individual[entity_id].tolist()[0]
            # check if we need to add additonal CompanyId
            if (additional_id is not None and entity_name is not None):
                row[entity_name + additional_id] = individual[additional_id].tolist()[0]
            # concat to the final dataframe
            df = pd.concat([df, row], sort=False)
    return df

def fixDate(column, df):
    ''' 
        Input:  
                column: date column containing a 0001-01-01 date
                df: dataframe containing the date column
        Output:
                return dataframe with fixed date column
    '''
    for date in df[column].tolist():
        if (date == '0001-01-01T00:00:00') :
            (df[column][df[column] == date]) = None  
    return df

def fix_datetype(df, col_list):
    ''' 
        Input:  
                df: dataframe containing the date columns
                col_list: list of columns, which datetime type should be changed 
                
        Output:
                return dataframe with fixed date column
    '''
    # copy the original dataframe
    df_fixed = df.copy()
    # for each column in the list
    for col in col_list:
        # convert the column to datetime
        df_fixed[col] = pd.to_datetime(df_fixed[col])
        # change the datetime type
        df_fixed[col] = df_fixed[col].astype('datetime64[ns, UTC]')
    return df_fixed


def log_update(dataset, table, process):
    '''
        log the tables 
    '''
    with open ('/home/ingest/logs/update_companies_processing_results.csv', 'a') as fl:
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if process:
            fl.write(str(curr_time) + '|' + dataset + '.' + table + '|Replaced successfully.\n')
        else:
            fl.write(str(curr_time) + '|' + dataset + '.' + table + '|There is no data, skipping replacement.\n')
    print(f'Processing for table {table} in dataset {dataset} logged successfully.')    
    return True


def update_data(df, dataset, table, project_id, entity_id_col, ids_datapoints):
    ''' 
        If the df is empty, log the results
        Else initialize class Updater, update data and metadata
        Input:
            df: dataframe to upload
            dataset: dataset to which we upload the df
            table: specific table which will contain the df
            project_id: project ID    
            entity_id_col: entity indicator column
            ids_datapoints: datapoint id columns
        Output:
            return True if all operations are successfull
    '''
    # check if dataframe is empty
    if df.empty:
        print(f'There is no data for {dataset}.{table}. Skipping replacement.')
        log_update(dataset, table, False)
    else:
        # get list of entity_ids
        entity_ids = df[entity_id_col].tolist()
        # replace \r\n with \n and \r with \n for API data
        df = df.replace(to_replace=r'\r\n', value='\n', regex=True)
        df = df.replace(to_replace=r'\r', value='\n', regex=True)
        # replace '' with None, as the existing data contains None, instead of ''
        df = df.replace({'': None})
        print(f'Initialize class Updater for {dataset}.{table}')
        update_table = datahub_updater.Updater(project_id, 
                                                dataset, table, df,
                                                entity_id_col, ids_datapoints,
                                                 entity_ids)
        print(f'Update {dataset}.{table}')
        update_table.update_data()
        # log updated table
        log_update(dataset, table, True)
    return True


def process_data():
    '''
        Download full Banking Company data from GD API, transform nested columns, 
        update tables and metadata in both Postgre and GBQ via class Updater.
    '''
    # set project ID
    project_id = "project-pulldata"
    # Download company data from API
    # set the token id
    token = 'hPP7@MubwL1C750pkWgUieNorBovxMsito/HZ9TilA4'
    # set dataset
    dataset = 'BankingCompanyProfiles'
    # get company listing
    print(f'Start to download full data for CompanyListing')
    df_listing = getCompanyListingOrDetails('GetCompanyListing', token)
    print(f'Finished download of full data for CompanyListing')
    # get list of company_ids in order to download news and deals
    company_ids = df_listing['CompanyId'].tolist()
    # update data for CompanyListing
    update_data(df_listing, dataset, 'CompanyListing', project_id, 'CompanyId', ['CompanyId'])
    # get company details
    print(f'Start to download full data for CompanyDetails')
    df_details = getCompanyListingOrDetails('GetCompanyDetails', token)
    print(f'Finished download of full data for CompanyDetails')
    # get COMPLETE News data for updated IDs
    print(f'Start to download full data for NewsDetails')
    df_news = get_data(company_ids, 'hPP7@MubwL1C750pkWgUieNorBovxMsito/HZ9TilA4', 'News')
    print(f'Finished download of full data for NewsDetails')
    # Set type datetime to PublishedDate because it is string
    df_news['PublishedDate'] = pd.to_datetime(df_news['PublishedDate'])
    df_news['PublishedDate'] = df_news['PublishedDate'].astype('datetime64[ns, UTC]')
    print(f'Start to download full data for DealDetails')
    df_deals = get_data(company_ids, 'hPP7@MubwL1C750pkWgUieNorBovxMsito/HZ9TilA4', 'Deal')
    print(f'Finished download of full data for DealDetails')
    # Transform company data
    print(f'Processing data for:')
    
    # Transform data for BankingCompanyProfiles.CompanyDetails
    print(f'CompanyDetails')
    df_company_details = df_details.drop(columns = ['History', 'KeyEmployees', 'LocationSubsidiaries',
       'KeyTopcompetitors', 'SwotAnalysis'])
    # update data for CompanyDetails
    update_data(df_company_details, dataset, 'CompanyDetails', project_id, 'CompanyId', ['CompanyId'])
    
    # Transform data for BankingCompanyProfiles.HistoryDetails
    print(f'HistoryDetails')
    df_history_details = unnest_columns(df_details, 'CompanyId','History',"")
    # update data for HistoryDetails
    update_data(df_history_details, dataset, 'HistoryDetails', project_id, 'Id', ['Id','CompanyId'])
    
    # Transform data for BankingCompanyProfiles.KeyEmployeesDetails
    print(f'KeyEmployeesDetails')
    df_key_employees_details = unnest_columns(df_details, 'CompanyId','KeyEmployees',"")
    # update data for KeyEmployeesDetails
    update_data(df_key_employees_details, dataset, 'KeyEmployeesDetails', project_id, 'ID', ['ID','CompanyId'])
    
    # Transform data for BankingCompanyProfiles.LocationSubsidiariesDetails
    print(f'LocationSubsidiariesDetails')
    df_key_location_subsidiaries_details = unnest_columns(df_details, 'CompanyId','LocationSubsidiaries','SubsidiariesId')
    df_key_location_subsidiaries_details['CompanyID'] = df_key_location_subsidiaries_details['CompanyID'].astype('int32')
    # update data for LocationSubsidiariesDetails
    #update_data(df_key_location_subsidiaries_details, dataset, 'LocationSubsidiariesDetails', project_id, 'SubsidiariesId', ['CompanyID', 'SubsidiariesId'])
    # Due to a BUG in the api we cant use the class updater. DSCOE-115
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
    gbq_client = get_credentials(project_id)
    replace_data(df_key_location_subsidiaries_details, dataset, 'LocationSubsidiariesDetails', postgre_engine, gbq_client, project_id)

    # Transform data for BankingCompanyProfiles.KeyTopCompetitorsDetails
    print(f'KeyTopCompetitorsDetails')
    df_key_top_competitors_details = unnest_columns(df_details, 'CompanyId','KeyTopcompetitors','CompetitorId')
    # update data for KeyTopCompetitorsDetails
    update_data(df_key_top_competitors_details, dataset, 'KeyTopCompetitorsDetails', project_id, 'CompetitorId', ['CompetitorId','CompanyId'])
    
    # Transform data for BankingCompanyProfiles.SwotAnalysisDetails
    print(f'SwotAnalysisDetails')
    df_swot_analysis_details_full = unnest_columns(df_details, 'CompanyId','SwotAnalysis',"")
    df_swot_analysis_details = df_swot_analysis_details_full[['Overview','CompanyId']]
    # update data for SwotAnalysisDetails
    update_data(df_swot_analysis_details, dataset, 'SwotAnalysisDetails', project_id, 'CompanyId', ['CompanyId'])
    
    # Transform data for BankingCompanyProfiles.StrengthsSwotAnalysisDetails
    print(f'StrengthsSwotAnalysisDetails')
    df_swot_strengths_details = unnest_columns(df_swot_analysis_details_full, 'CompanyId','Strengths',"")
    df_swot_strengths_details['CompanyId'] = df_swot_strengths_details['CompanyId'].astype('int32')
    # update data for StrengthsSwotAnalysisDetails
    update_data(df_swot_strengths_details, dataset, 'StrengthsSwotAnalysisDetails', project_id, 'CompanyId', ['CompanyId', 'Title'])
    
    # Transform data for BankingCompanyProfiles.WeaknessSwotAnalysisDetails
    print(f'WeaknessSwotAnalysisDetails')
    df_swot_weakness_details = unnest_columns(df_swot_analysis_details_full, 'CompanyId','Weakness',"")
    df_swot_weakness_details['CompanyId'] = df_swot_weakness_details['CompanyId'].astype('int32')
    # update data for WeaknessSwotAnalysisDetails
    update_data(df_swot_weakness_details, dataset, 'WeaknessSwotAnalysisDetails', project_id, 'CompanyId', ['CompanyId', 'Title'])
    
    # Transform data for BankingCompanyProfiles.OpportunitiesSwotAnalysisDetails
    print(f'OpportunitiesSwotAnalysisDetails')
    df_swot_opportunities_details = unnest_columns(df_swot_analysis_details_full, 'CompanyId','Opportunities',"")
    df_swot_opportunities_details['CompanyId'] = df_swot_opportunities_details['CompanyId'].astype('int32')
    # update data for OpportunitiesSwotAnalysisDetails
    update_data(df_swot_opportunities_details, dataset, 'OpportunitiesSwotAnalysisDetails', project_id, 'CompanyId', ['CompanyId', 'Title'])
    
    # Transform data for BankingCompanyProfiles.ThreatsSwotAnalysisDetails
    print(f'ThreatsSwotAnalysisDetails')
    df_swot_threats_details = unnest_columns(df_swot_analysis_details_full, 'CompanyId','Threats',"")
    df_swot_threats_details['CompanyId'] = df_swot_threats_details['CompanyId'].astype('int32')
    # update data for ThreatsSwotAnalysisDetails
    update_data(df_swot_threats_details, dataset, 'ThreatsSwotAnalysisDetails', project_id, 'CompanyId', ['CompanyId', 'Title'])
    
    # Transform data for BankingCompanyProfiles.NewsDetails
    # some rows appear not identical because there are symbols like '“','”' and '’', which are encoded
    # example NewsArticleId are 1238389,1484086,1177420
    print(f'NewsDetails')
    df_news_details = df_news.drop(columns=['NewsArticleCompanies'])
    # update data for NewsDetails
    update_data(df_news_details, dataset, 'NewsDetails', project_id, 'NewsArticleId', ['CompanyId', 'NewsArticleId'])
    
    # Transform data for BankingCompanyProfiles.NewsArticleCompaniesDetails
    print(f'NewsArticleCompaniesDetails')
    df_newsarticlecompanies = unnest_columns(df_news, 'NewsArticleId','NewsArticleCompanies',"", 'CompanyId', 'News')
    # update data for on NewsArticleCompaniesDetails
    update_data(df_newsarticlecompanies, dataset, 'NewsArticleCompaniesDetails', project_id, 'NewsArticleId', ['NewsArticleId', 'NewsCompanyId', 'CompanyUrlNode'])
   
    # Transform deals data
    # Transform data for BankingCompanyProfiles.DealDetails
    print(f'DealDetails')
    ## Fixing date 0001-01-01
    df_deals = fixDate('DealCompletedDate', df_deals)
    df_deals = fixDate('AnnounceDate', df_deals)
    df_deals = fixDate('OptionalDateValue', df_deals)
    # Transform dates from string to datetime objects
    lst_fixdate = ['DealCompletedDate', 'PublishedDate', 'AnnounceDate', 'RumorDate', 'OptionalDateValue']
    df_deals = fix_datetype(df_deals, lst_fixdate)
    df_deal_details = df_deals.drop(columns = ['TargetAssets','Targets', 'Aquirers','Vendors', 'Entities', 'FinancialAdvisors', 'LegalAdvisors'])
    # update data for DealDetails
    update_data(df_deal_details, dataset, 'DealDetails', project_id, 'DealID', ['DealID','CompanyId'])
    
    # Transform data for BankingCompanyProfiles.TargetAssetsDealDetails
    print(f'TargetAssetsDealDetails')
    df_targetassets_details = unnest_columns(df_deals, 'DealID','TargetAssets',"", 'CompanyId', 'Deal')
    # update data for TargetAssetsDealDetails
    update_data(df_targetassets_details, dataset, 'TargetAssetsDealDetails', project_id, 'DealID', ['DealID','DealCompanyId','AssetName'])
    
    # Transform data for BankingCompanyProfiles.TargetsDealDetails
    print(f'TargetsDealDetails')
    df_targets_details = unnest_columns(df_deals, 'DealID','Targets',"",'CompanyId', 'Deal')
    # Set type int to CompanyId because unnesting converts it to float
    df_targets_details['CompanyId'] = df_targets_details['CompanyId'].astype('int32')
    # update data for TargetsDealDetails
    update_data(df_targets_details, dataset, 'TargetsDealDetails', project_id, 'DealID', ['DealID','DealCompanyId', 'CompanyId'])
    
    # Transform data for BankingCompanyProfiles.AquirersDealDetails
    print(f'AquirersDealDetails')
    df_aquirers_details = unnest_columns(df_deals, 'DealID','Aquirers',"", 'CompanyId', 'Deal')
    # Set type int to CompanyId because unnesting converts it to float
    df_aquirers_details['CompanyId'] = df_aquirers_details['CompanyId'].astype('int32')
    # update data for AquirersDealDetails
    update_data(df_aquirers_details, dataset, 'AquirersDealDetails', project_id, 'DealID', ['DealID','DealCompanyId', 'CompanyId'])

    # Transform data for BankingCompanyProfiles.VendorsDealDetails
    print(f'VendorsDealDetails')
    df_vendors_details = unnest_columns(df_deals, 'DealID','Vendors',"", 'CompanyId', 'Deal')
    # Set type int to CompanyId because unnesting converts it to float
    df_vendors_details['CompanyId'] = df_vendors_details['CompanyId'].astype('int32')
    # update data for VendorsDealDetails
    update_data(df_vendors_details, dataset, 'VendorsDealDetails', project_id, 'DealID', ['DealID','DealCompanyId', 'CompanyId'])
    
    # Transform data for BankingCompanyProfiles.EntitiesDealDetails
    print(f'EntitiesDealDetails')
    df_entities_details = unnest_columns(df_deals, 'DealID','Entities',"", 'CompanyId', 'Deal')
    # update data for EntitiesDealDetails
    update_data(df_entities_details, dataset, 'EntitiesDealDetails', project_id, 'DealID', ['DealID','DealCompanyId', 'CompanyId'])
    
    # Transform data for BankingCompanyProfiles.FinancialAdvisorsDealDetails
    print(f'FinancialAdvisorsDealDetails')
    df_financial_advisors_details = unnest_columns(df_deals, 'DealID','FinancialAdvisors',"", 'CompanyId', 'Deal') 
    # update data for FinancialAdvisorsDealDetails
    update_data(df_financial_advisors_details, dataset, 'FinancialAdvisorsDealDetails', project_id, 'DealID', ['DealID','DealCompanyId', 'AdvisorName'])
    
    # Transform data for BankingCompanyProfiles.LegalAdvisorsDealDetails
    print(f'LegalAdvisorsDealDetails')
    df_legal_advisors_details = unnest_columns(df_deals, 'DealID','LegalAdvisors',"", 'CompanyId', 'Deal') 
    # update data for LegalAdvisorsDealDetails
    update_data(df_legal_advisors_details, dataset, 'LegalAdvisorsDealDetails', project_id, 'DealID', ['DealID','DealCompanyId'])
    
    return True


default_args = {
    'owner': 'project-pulldata_companies_data_update_v2',
    'start_date': datetime.datetime(2020, 10, 15, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_companies_data_update_v2',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 21 * * *',
     ) as dag:
    
    
    company_update = PythonOperator(task_id='process_data',
                               python_callable=process_data)

company_update