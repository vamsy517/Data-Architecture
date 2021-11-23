import datetime
import pytz
from datetime import date, timedelta
import os

import pandas_gbq
import pandas as pd
import scipy.stats as stats
import numpy as np

from google.oauth2 import service_account
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from sqlalchemy import create_engine 

import requests
import json
from pandas.io.json import json_normalize


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


def unnest_columns(df_to_unnest, entity_id,column_to_unnest, internal_id):
    ''' 
        Input:  
                df_to_unnest: dataframe which contains nested columns
                entity_id: DealID or NewsArticleId,
                column_to_unnest: name of the column which will be unnested
                internal_id: name of the column which is used as id in some tables  
        Output:
                return dataframe containing the unnested column's data
    '''
    df = pd.DataFrame()
    # for each item in the dataframe
    for item in df_to_unnest[entity_id].tolist():
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

def update_data():
    '''
        Input: ID for the project in GBQ
        Download full Banking Company data from GD API, transform nested columns, create new Banking tables,
        delete old Banking tables, upload new tables and update metadata.
    '''
    print('This DAG is deprecated!')
    return True
    # set project ID
    project_id = "project-pulldata"
    # create connection to postgree and GBQ
    # NOTE!!! ALSO instantiate clients / engines closer to when needed - connections terminated otherwise
    postgre_engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres')
    gbq_client = get_credentials(project_id)
    # Download company data from API
    # set the token id
    token = 'hPP7@MubwL1C750pkWgUieNorBovxMsito/HZ9TilA4'
    # get company listing
    print(f'Download data for CompanyListing')
    df_listing = getCompanyListingOrDetails('GetCompanyListing', token)
    # Perform delete and upload on CompanyListing , update metadata and log results
    replace_data(df_listing, 'BankingCompanyProfiles', 'CompanyListing', postgre_engine, gbq_client, project_id)
    # get list of company ids
    company_ids = df_listing['CompanyId'].tolist()
    # get company details
    print(f'Start to download full data for CompanyDetails')
    df_details = getCompanyListingOrDetails('GetCompanyDetails', token)
    print(f'Finished download of full data for CompanyDetails')
    # get COMPLETE News data for updated IDs
    print(f'Start to download full data for NewsDetails')
    df_news = get_data(company_ids, 'hPP7@MubwL1C750pkWgUieNorBovxMsito/HZ9TilA4', 'News')
    # Set type datetime to PublishedDate because it is string
    df_news['PublishedDate'] = pd.to_datetime(df_news['PublishedDate'])
    print(f'Finished download of full data for NewsDetails')
    print(f'Start to download full data for DealDetails')
    df_deals = get_data(company_ids, 'hPP7@MubwL1C750pkWgUieNorBovxMsito/HZ9TilA4', 'Deal')
    print(f'Finished download of full data for DealDetails')
    # Transform company data
    print(f'Processing data for:')
    
    # Transform data for BankingCompanyProfiles.CompanyDetails
    print(f'CompanyDetails')
    df_company_details = df_details.drop(columns = ['History', 'KeyEmployees', 'LocationSubsidiaries',
       'KeyTopcompetitors', 'KeyFacts', 'SwotAnalysis'])
    # Perform delete and upload on CompanyDetails , update metadata and log results
    replace_data(df_company_details, 'BankingCompanyProfiles', 'CompanyDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.HistoryDetails
    print(f'HistoryDetails')
    df_history_details = unnest_columns(df_details, 'CompanyId','History',"")
    # Perform delete and upload on HistoryDetails , update metadata and log results
    replace_data(df_history_details, 'BankingCompanyProfiles', 'HistoryDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.KeyEmployeesDetails
    print(f'KeyEmployeesDetails')
    df_key_employees_details = unnest_columns(df_details, 'CompanyId','KeyEmployees',"")
    # Perform delete and upload on KeyEmployeesDetails , update metadata and log results
    replace_data(df_key_employees_details, 'BankingCompanyProfiles', 'KeyEmployeesDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.LocationSubsidiariesDetails
    print(f'LocationSubsidiariesDetails')
    df_key_location_subsidiaries_details = unnest_columns(df_details, 'CompanyId','LocationSubsidiaries','SubsidiariesId')
    # Perform delete and upload on LocationSubsidiariesDetails , update metadata and log results
    replace_data(df_key_location_subsidiaries_details, 'BankingCompanyProfiles', 'LocationSubsidiariesDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.KeyTopCompetitorsDetails
    print(f'KeyTopCompetitorsDetails')
    df_key_top_competitors_details = unnest_columns(df_details, 'CompanyId','KeyTopcompetitors','CompetitorId')
    # Perform delete and upload on KeyTopCompetitorsDetails , update metadata and log results
    replace_data(df_key_top_competitors_details, 'BankingCompanyProfiles', 'KeyTopCompetitorsDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.KeyFactsDetails
    print(f'KeyFactsDetails')
    df_key_facts_details = unnest_columns(df_details, 'CompanyId','KeyFacts',"")
    # Perform delete and upload on KeyFactsDetails , update metadata and log results
    replace_data(df_key_facts_details, 'BankingCompanyProfiles', 'KeyFactsDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.SwotAnalysisDetails
    print(f'SwotAnalysisDetails')
    df_swot_analysis_details_full = unnest_columns(df_details, 'CompanyId','SwotAnalysis',"")
    df_swot_analysis_details = df_swot_analysis_details_full[['Overview','CompanyId']]
    # Perform delete and upload on SwotAnalysisDetails , update metadata and log results
    replace_data(df_swot_analysis_details, 'BankingCompanyProfiles', 'SwotAnalysisDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.StrengthsSwotAnalysisDetails
    print(f'StrengthsSwotAnalysisDetails')
    df_swot_strengths_details = unnest_columns(df_swot_analysis_details_full, 'CompanyId','Strengths',"")
    df_swot_strengths_details['CompanyId'] = df_swot_strengths_details['CompanyId'].astype('int32')
    # Perform delete and upload on StrengthsSwotAnalysisDetails , update metadata and log results
    replace_data(df_swot_strengths_details, 'BankingCompanyProfiles', 'StrengthsSwotAnalysisDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.WeaknessSwotAnalysisDetails
    print(f'WeaknessSwotAnalysisDetails')
    df_swot_weakness_details = unnest_columns(df_swot_analysis_details_full, 'CompanyId','Weakness',"")
    df_swot_weakness_details['CompanyId'] = df_swot_weakness_details['CompanyId'].astype('int32')
    # Perform delete and upload on WeaknessSwotAnalysisDetails , update metadata and log results
    replace_data(df_swot_weakness_details, 'BankingCompanyProfiles', 'WeaknessSwotAnalysisDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.OpportunitiesSwotAnalysisDetails
    print(f'OpportunitiesSwotAnalysisDetails')
    df_swot_opportunities_details = unnest_columns(df_swot_analysis_details_full, 'CompanyId','Opportunities',"")
    df_swot_opportunities_details['CompanyId'] = df_swot_opportunities_details['CompanyId'].astype('int32')
    # Perform delete and upload on OpportunitiesSwotAnalysisDetails , update metadata and log results
    replace_data(df_swot_opportunities_details, 'BankingCompanyProfiles', 'OpportunitiesSwotAnalysisDetails', postgre_engine, gbq_client, project_id)
    
    
    # Transform data for BankingCompanyProfiles.ThreatsSwotAnalysisDetails
    print(f'ThreatsSwotAnalysisDetails')
    df_swot_threats_details = unnest_columns(df_swot_analysis_details_full, 'CompanyId','Threats',"")
    df_swot_threats_details['CompanyId'] = df_swot_threats_details['CompanyId'].astype('int32')
    # Perform delete and upload on ThreatsSwotAnalysisDetails , update metadata and log results
    replace_data(df_swot_threats_details, 'BankingCompanyProfiles', 'ThreatsSwotAnalysisDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.NewsDetails
    print(f'NewsDetails')
    df_news_details = df_news.drop(columns=['NewsArticleCompanies'])
    # Perform delete and upload on NewsDetails , update metadata and log results
    replace_data(df_news_details, 'BankingCompanyProfiles', 'NewsDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.NewsArticleCompaniesDetails
    print(f'NewsArticleCompaniesDetails')
    df_newsarticlecompanies = unnest_columns(df_news, 'NewsArticleId','NewsArticleCompanies',"")
    # Perform delete and upload on NewsArticleCompaniesDetails , update metadata and log results
    replace_data(df_newsarticlecompanies, 'BankingCompanyProfiles', 'NewsArticleCompaniesDetails', postgre_engine, gbq_client, project_id)
    
    # Transform deals data
    # Transform data for BankingCompanyProfiles.DealDetails
    print(f'DealDetails')
    ## Fixing date 0001-01-01
    df_deals = fixDate('DealCompletedDate', df_deals)
    df_deals = fixDate('AnnounceDate', df_deals)
    df_deals = fixDate('OptionalDateValue', df_deals)
    # Transform dates from string to datetime objects
    df_deals['DealCompletedDate'] = pd.to_datetime(df_deals['DealCompletedDate'])
    df_deals['PublishedDate'] = pd.to_datetime(df_deals['PublishedDate'])
    df_deals['AnnounceDate'] = pd.to_datetime(df_deals['AnnounceDate'])
    df_deals['RumorDate'] = pd.to_datetime(df_deals['RumorDate'])
    df_deals['OptionalDateValue'] = pd.to_datetime(df_deals['OptionalDateValue'])
    df_deal_details = df_deals.drop(columns = ['TargetAssets','Targets', 'Aquirers','Vendors', 'Entities', 'FinacialAdvisors', 'LegalAdvisors'])
    # Perform delete and upload on DealDetails , update metadata and log results
    replace_data(df_deal_details, 'BankingCompanyProfiles', 'DealDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.TargetAssetsDealDetails
    print(f'TargetAssetsDealDetails')
    df_targetassets_details = unnest_columns(df_deals, 'DealID','TargetAssets',"")
    # Perform delete and upload on TargetAssetsDealDetails , update metadata and log results
    replace_data(df_targetassets_details, 'BankingCompanyProfiles', 'TargetAssetsDealDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.TargetsDealDetails
    print(f'TargetsDealDetails')
    df_targets_details = unnest_columns(df_deals, 'DealID','Targets',"")
    # Set type int to CompanyId because unnesting converts it to float
    df_targets_details['CompanyId'] = df_targets_details['CompanyId'].astype('int32')
    # Perform delete and upload on TargetsDealDetails , update metadata and log results
    replace_data(df_targets_details, 'BankingCompanyProfiles', 'TargetsDealDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.AquirersDealDetails
    print(f'AquirersDealDetails')
    df_aquirers_details = unnest_columns(df_deals, 'DealID','Aquirers',"")
    # Set type int to CompanyId because unnesting converts it to float
    df_aquirers_details['CompanyId'] = df_aquirers_details['CompanyId'].astype('int32')
    # Perform delete and upload on AquirersDealDetails , update metadata and log results
    replace_data(df_aquirers_details, 'BankingCompanyProfiles', 'AquirersDealDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.VendorsDealDetails
    print(f'VendorsDealDetails')
    df_vendors_details = unnest_columns(df_deals, 'DealID','Vendors',"")
    # Set type int to CompanyId because unnesting converts it to float
    df_vendors_details['CompanyId'] = df_vendors_details['CompanyId'].astype('int32')
    # Perform delete and upload on VendorsDealDetails , update metadata and log results
    replace_data(df_vendors_details, 'BankingCompanyProfiles', 'VendorsDealDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.EntitiesDealDetails
    print(f'EntitiesDealDetails')
    df_entities_details = unnest_columns(df_deals, 'DealID','Entities',"")
    # Perform delete and upload on EntitiesDealDetails , update metadata and log results
    replace_data(df_entities_details, 'BankingCompanyProfiles', 'EntitiesDealDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.FinacialAdvisorsDealDetails
    print(f'FinancialAdvisorsDealDetails')
    df_financial_advisors_details = unnest_columns(df_deals, 'DealID','FinacialAdvisors',"")
    # Perform delete and upload on FinancialAdvisorsDealDetails , update metadata and log results
    replace_data(df_financial_advisors_details, 'BankingCompanyProfiles', 'FinancialAdvisorsDealDetails', postgre_engine, gbq_client, project_id)
    
    # Transform data for BankingCompanyProfiles.LegalAdvisorsDealDetails
    print(f'LegalAdvisorsDealDetails')
    df_legal_advisors_details = unnest_columns(df_deals, 'DealID','LegalAdvisors',"")
    # Perform delete and upload on LegalAdvisorsDealDetails , update metadata and log results
    replace_data(df_legal_advisors_details, 'BankingCompanyProfiles', 'LegalAdvisorsDealDetails', postgre_engine, gbq_client, project_id)
        
    return True

default_args = {
    'owner': 'project-pulldata_companies_data_update',
    'start_date': datetime.datetime(2020, 10, 15, 21, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('project-pulldata_companies_data_update',
     catchup=False,
     default_args=default_args,
     schedule_interval='0 21 * * *',
     ) as dag:
    
    
    company_update = PythonOperator(task_id='update_data',
                               python_callable=update_data)

company_update