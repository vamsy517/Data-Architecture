import pandas as pd
import datetime
import time
from ingest_utils.metadata import update_metadata
from ingest_utils.constants import PULLDATA_PROJECT_ID, LOGS_FOLDER, POSTGRE_AUTH, PERMUTIVE_PARDOT_MATCH_LOG_B2B
from ingest_utils.class_datahub_updater import Updater
from ingest_utils.utils import fix_dataframe_columns
from pardot_utils.constants import *
from pardot_utils.gbq_queries import *
from pardot_utils.utils import get_new_created_updated_data, split, get_email_metrics, get_api_key, \
    get_new_created_data_visits, log_update, unnest_page_views
from sql_queries.gbq_queries import QUERY_WHOLE_TABLE
import pytz
from permutive_pardot_match.utils import get_prospect_data
from ingest_utils.database_gbq import get_gbq_table, upload_table_to_gbq


def collect_all_emails_ids(emails_df) -> bool:
    """
    Download all the matching data between Pardot and Permutive.
    Upload the matches to the corresponding table in GBQ.
    :params emails: dataframe with emails
    """

    dataset_nsmg = NSMG_DATASET
    dataset_verdict = VERDICT_DATASET
    table = 'PermutivePardotMatch'

    # create a empty dataframe to fill with matching data
    mapping_table = pd.DataFrame()
    # set api_key
    api_key = get_api_key()

    # remove problematic emails
    emails_df = emails_df[emails_df['value'] != 'ravi.tangirala@progressivemediagroup.in']
    emails_df = emails_df[emails_df['value'] != 'ravikumar.tangirala@globaldata.com']
    emails_df = emails_df[emails_df['value'] != 'sam.hall@globaldata.com']
    # if the emails dataframe is empty print a message
    if emails_df.empty:
        print('There are no new emails to process.')
    # if emails dataframe is not empty, process the emails
    else:
        print('Processing matches between Pardot and Permutive...')
        # for each row in emails
        for item in emails_df.values:
            # create a df for a single emails data
            single_df = pd.DataFrame()
            # try to get the prospect id for the current email
            try:
                response = get_prospect_data(item[0], api_key)

            except:
                print('Trying again...')
                # if error, wait 60 sec and try again
                time.sleep(60)
                try:
                    # try to get the prospect id
                    response = get_prospect_data(item[0], api_key)
                except:
                    # if error persists, continue for next row
                    continue
            # if the response from get_prospect_data is 'ivalid key' ,
            # set a new api key and repeat the process to obtain the id
            if response == 'invalid key':
                api_key = get_api_key()
                try:
                    response = get_prospect_data(item[0], api_key)
                except:
                    print('Trying again...')
                    time.sleep(60)
                    try:
                        response = get_prospect_data(item[0], api_key)
                    except:
                        continue
            # if the response from get_prospect_data is different from 'invalid email', get the necessary data
            if response != 'invalid email':
                prospect_id = response
                # set the prospect_id as integer
                single_df['prospect_id'] = [int(prospect_id)]
                # set the permutive id , which comes from original emails data
                single_df['permutive_id'] = [item[1]]
                # set the created date as current timestamp
                single_df['created'] = [datetime.datetime.now(pytz.utc)]
                # append the matching data to the final fataframe
                mapping_table = mapping_table.append(single_df)
        # if the mapping_Table is not empty, upload to GBQ table
        if not mapping_table.empty:
            # separate data to Verdict and NSMG:

            nsmg_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_NSMG_PROSPECTS).id.unique())
            verdict_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_VERDICT_PROSPECTS).id.unique())
            nsmg_df = mapping_table[mapping_table['prospect_id'].isin(nsmg_prospect_ids_list)]
            verdict_df = mapping_table[mapping_table['prospect_id'].isin(verdict_prospect_ids_list)]

            print('Uploading dataframe with ids to GBQ.')
            upload_table_to_gbq(PULLDATA_PROJECT_ID, nsmg_df, dataset_nsmg, table)
            print('Uploading dataframe with ids to GBQ.')
            upload_table_to_gbq(PULLDATA_PROJECT_ID, verdict_df, dataset_verdict, table)
        # if it is empty return a message
        else:
            print('No mathing entires found.')
    return True


def upload_data_emails(prospects_counter: str) -> int:
    """
    Download Pardot Emails/Emails Metrics new data and update the table in GBQ and Postgre
    :return: Number of API calls made
    """
    project_id = PULLDATA_PROJECT_ID
    dataset_nsmg = NSMG_DATASET
    dataset_verdict = VERDICT_DATASET
    pardot_api = EMAIL_API
    clicks_table = EMAIL_CLICKS_TABLE
    emails_table = EMAILS_TABLE
    prospects_table = PROSPECTS_TABLE
    log_file_nsmg = f'{LOGS_FOLDER}{NSMG_EMAIL_LOG}'
    log_file_verdict = f'{LOGS_FOLDER}{VERDICT_EMAIL_LOG}'
    data_name = EMAILS_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # get max created and updated values to start download form that values
    max_created_updated_nsmg = get_gbq_table(project_id, QUERY_MAX_MIN_EMAIL.format(project_id=project_id,
                                                                                    dataset=dataset_nsmg,
                                                                                    table=clicks_table))
    max_created_updated_verdict = get_gbq_table(project_id, QUERY_MAX_MIN_EMAIL.format(project_id=project_id,
                                                                                       dataset=dataset_verdict,
                                                                                       table=clicks_table))
    old_df_created_at = datetime.datetime.strptime(max(max_created_updated_nsmg.max_created_at.max(),
                        max_created_updated_verdict.max_created_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')

    # get old table for prospects from GBQ
    old_df_nsmg_prospect = get_gbq_table(project_id,
                                         QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset_nsmg,
                                                                  table=prospects_table))
    old_df_verdict_prospect = get_gbq_table(project_id,
                                            QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset_verdict,
                                                                     table=prospects_table))
    # get new created data
    endtime = datetime.datetime.strptime(max(old_df_nsmg_prospect.created_at.max(),
                        old_df_verdict_prospect.created_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')

    # get new created data
    new_created_df, counter_emails = get_new_created_updated_data(url, old_df_created_at, 'created',
                                                                  data_name, endtime, int(prospects_counter))
    print("Calls after collecting created email clicks data: ", counter_emails)
    if not new_created_df.empty:
        # fix data types and column names
        new_created_df = new_created_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        new_created_df['email_template_id'] = new_created_df['email_template_id'].astype(pd.Int64Dtype())
        date_cols = ['created_at']
        new_created_df[date_cols] = new_created_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S',
                                                                    errors='ignore', utc=True)
        new_created_df['list_email_id'] = new_created_df['list_email_id'].astype(pd.Int64Dtype())
        new_created_df = new_created_df.convert_dtypes()

        # separate data to Verdict and NSMG:

        nsmg_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_NSMG_PROSPECTS).id.unique())
        verdict_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_VERDICT_PROSPECTS).id.unique())
        email_nsmg_df = new_created_df[new_created_df['prospect_id'].isin(nsmg_prospect_ids_list)]
        email_verdict_df = new_created_df[~new_created_df['prospect_id'].isin(nsmg_prospect_ids_list)]
        # NSMG:
        ids_clicks_nsmg = list(email_nsmg_df.id)
        # split final data in chunks and run updater class on every chunk
        # we split the data because is to big and can't be uploaded at onc
        chunks = split(email_nsmg_df, 30000)
        for c in chunks:
            ids_clicks_split = list(c.id)
            update_clicks = Updater(dataset_nsmg, clicks_table, c, 'id', ['id'], ids_clicks_split,
                                    upload_to_postgre=False)
            update_clicks.update_data()

        # get the data for email metric details and fix the data types
        email_df, counter_nsmg = get_email_metrics(email_nsmg_df, counter_emails)
        print("Calls after collecting nsmg emails metrics data: ", counter_nsmg)
        filter_col = [col for col in email_df if col.endswith('rate') or col.endswith('ratio')]
        for col in filter_col:
            email_df[col] = email_df[col].str.rstrip('%').astype('float') / 100.0
        email_df = email_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        email_df.id = email_df.id.astype('int')
        ids_email_metrics_nsmg = list(email_df.id)
        chunks = split(email_df, 30000)
        for c in chunks:
            ids_email_split = list(c.id)
            update_email_metrics = Updater(dataset_nsmg, emails_table, c, 'id', ['id'], ids_email_split,
                                           upload_to_postgre=False)
            update_email_metrics.update_data()

        # VERDICT:
        ids_clicks_verdict = list(email_verdict_df.id)
        # split final data in chunks and run updater class on every chunk
        # we split the data because is to big and can't be uploaded at onc
        chunks = split(email_verdict_df, 30000)
        for c in chunks:
            ids_clicks_split = list(c.id)
            update_clicks = Updater(dataset_verdict, clicks_table, c, 'id', ['id'], ids_clicks_split,
                                    upload_to_postgre=False)
            update_clicks.update_data()

        # get the data for email metric details and fix the data types
        email_df, counter_verdict = get_email_metrics(email_verdict_df, counter_nsmg)
        print("Calls after collecting verdict emails metrics data: ", counter_verdict)
        filter_col = [col for col in email_df if col.endswith('rate') or col.endswith('ratio')]
        for col in filter_col:
            email_df[col] = email_df[col].str.rstrip('%').astype('float') / 100.0
        email_df = email_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        email_df.id = email_df.id.astype('int')
        ids_email_metrics_verdict = list(email_df.id)
        chunks = split(email_df, 30000)
        for c in chunks:
            ids_email_split = list(c.id)
            update_email_metrics = Updater(dataset_verdict, emails_table, c, 'id', ['id'], ids_email_split,
                                           upload_to_postgre=False)
            update_email_metrics.update_data()
    else:
        ids_clicks_nsmg = []
        ids_email_metrics_nsmg = []
        ids_clicks_verdict = []
        ids_email_metrics_verdict = []
        # record log file
    log_update(log_file_nsmg, ids_clicks_nsmg, clicks_table)
    log_update(log_file_nsmg, ids_email_metrics_nsmg, emails_table)
    log_update(log_file_verdict, ids_clicks_verdict, clicks_table)
    log_update(log_file_verdict, ids_email_metrics_verdict, emails_table)
    return counter_verdict


def upload_data_list_memberships() -> bool:
    """
    Download Pardot List Membership new data and update the table in GBQ and Postgre
    :return: True if Pardot List Membership data is updated
    """
    project_id = PULLDATA_PROJECT_ID
    dataset_nsmg = NSMG_DATASET
    dataset_verdict = VERDICT_DATASET
    pardot_api = LIST_MEMBERSHIP_API
    table = LIST_MEMBERSHIP_TABLE
    log_file_nsmg = f'{LOGS_FOLDER}{NSMG_LIST_MEMBERSHIP_LOG}'
    log_file_verdict = f'{LOGS_FOLDER}{VERDICT_LIST_MEMBERSHIP_LOG}'
    data_name = LIST_MEMBERSHIP_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # get max created and updated values to start download form that values
    nsmg_max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                              dataset=dataset_nsmg,
                                                                              table=table))
    verdict_max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                                 dataset=dataset_verdict,
                                                                                 table=table))
    old_df_created_at = max(nsmg_max_created_updated.max_created_at.max(),
                            verdict_max_created_updated.max_created_at.max())
    old_df_updated_at = max(nsmg_max_created_updated.max_updated_at.max(),
                            verdict_max_created_updated.max_updated_at.max())
    old_df_min_id = min(nsmg_max_created_updated.minid, verdict_max_created_updated.minid)
    old_df_max_id = max(nsmg_max_created_updated.maxid, verdict_max_created_updated.maxid)
    # get new created data
    # get old table for prospects from GBQ
    old_df_nsmg_prospect = get_gbq_table(project_id,
                                         QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset_nsmg,
                                                                  table=table))
    old_df_verdict_prospect = get_gbq_table(project_id,
                                            QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset_verdict,
                                                                     table=table))
    # get new created data
    endtime = max(old_df_nsmg_prospect.created_at.max(), old_df_verdict_prospect.created_at.max())
    new_created_df = get_new_created_updated_data(url, old_df_created_at,
                                                  str(old_df_max_id - 1), 'created',
                                                  data_name, endtime, before_mark_delta=3)
    # get new updated data
    new_updated_df = get_new_created_updated_data(url, old_df_updated_at,
                                                  str(old_df_min_id - 1), 'updated',
                                                  data_name, endtime, before_mark_delta=3)
    # concat update and new data
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)
    final_df = final_df.drop_duplicates(subset='id', keep="last")

    # check if DataFrame is empty
    if not final_df.empty:
        # fix data types
        final_df = final_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        d = {'false': False, 'true': True}
        final_df['opted_out'] = final_df['opted_out'].map(d)
        date_cols = ['created_at', 'updated_at']
        final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime,
                                                        format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
        final_df = final_df.convert_dtypes()

        # separate data to Verdict and NSMG:

        nsmg_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_NSMG_PROSPECTS).id.unique())
        verdict_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_VERDICT_PROSPECTS).id.unique())
        nsmg_df = final_df[final_df['prospect_id'].isin(nsmg_prospect_ids_list)]
        verdict_df = final_df[final_df['prospect_id'].isin(verdict_prospect_ids_list)]
        # NSMG:
        # record IDs
        nsmg_ids = list(final_df.id)
        # split final data in chunks and run updater class on every chunk
        # we split the data because is to big and can't be uploaded at onc
        chunks = split(nsmg_df, 20000)
        for c in chunks:
            ids_details = list(c.id)
            update_list_nsmg = Updater(dataset_nsmg, table, c, 'id', ['id'], ids_details)
            update_list_nsmg.update_data()
        # Verdict:
        # record IDs
        verdict_ids = list(final_df.id)
        # split final data in chunks and run updater class on every chunk
        # we split the data because is to big and can't be uploaded at onc
        chunks = split(verdict_df, 20000)
        for c in chunks:
            ids_details = list(c.id)
            update_list_verdict = Updater(dataset_verdict, table, c, 'id', ['id'], ids_details)
            update_list_verdict.update_data()
    else:
        nsmg_ids = []
        verdict_ids = []
    # record log file
    log_update(log_file_nsmg, nsmg_ids, table)
    log_update(log_file_verdict, verdict_ids, table)
    return True


def upload_data_lists() -> bool:
    """
    Download Pardot Lists new data and update the table in GBQ and Postgre
    :return: True if Pardot Lists data is updated
    """
    project_id = PULLDATA_PROJECT_ID
    dataset_nsmg = NSMG_DATASET
    dataset_verdict = VERDICT_DATASET
    pardot_api = LISTS_API
    table = LISTS_TABLE
    log_file_nsmg = f'{LOGS_FOLDER}{NSMG_LISTS_LOG}'
    log_file_verdict = f'{LOGS_FOLDER}{VERDICT_LISTS_LOG}'
    data_name = LISTS_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # get max created and updated values to start download form that values
    nsmg_max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                              dataset=dataset_nsmg,
                                                                              table=table))
    verdict_max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                                 dataset=dataset_verdict,
                                                                                 table=table))
    old_df_created_at = max(nsmg_max_created_updated.max_created_at.max(),
                            verdict_max_created_updated.max_created_at.max())
    old_df_updated_at = max(nsmg_max_created_updated.max_updated_at.max(),
                            verdict_max_created_updated.max_updated_at.max())
    old_df_min_id = min(nsmg_max_created_updated.minid, verdict_max_created_updated.minid)
    old_df_max_id = max(nsmg_max_created_updated.maxid, verdict_max_created_updated.maxid)

    # get max created time for ListMembership GBQ
    nsmg_max_created_updated_list = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                                   dataset=dataset_nsmg,
                                                                                   table='ListMemberships'))
    verdict_max_created_updated_list = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                                      dataset=dataset_verdict,
                                                                                      table='ListMemberships'))
    endtime = max(nsmg_max_created_updated_list.max_created_at.max(),
                  verdict_max_created_updated_list.max_created_at.max())
    # get new created data
    new_created_df = get_new_created_updated_data(url, old_df_created_at,
                                                  str(old_df_max_id - 1), 'created',
                                                  data_name, endtime)
    # get new updated data
    new_updated_df = get_new_created_updated_data(url, old_df_updated_at,
                                                  str(old_df_min_id - 1), 'updated',
                                                  data_name, endtime)
    # concat update and new data
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)
    final_df = final_df.drop_duplicates(subset='id', keep="last")

    # check if DataFrame is empty
    if not final_df.empty:

        # fix columns names
        final_df = fix_dataframe_columns(final_df)
        # fix all numeric values
        final_df = final_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        # all date columns
        date_cols = ['created_at', 'updated_at']
        final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore',
                                                        utc=True)
        # fix data types
        final_df[['title', 'description']] = final_df[['title', 'description']].astype(str)
        d = {'false': False, 'true': True}
        final_df['is_public'] = final_df['is_public'].map(d)
        final_df['is_dynamic'] = final_df['is_dynamic'].map(d)
        final_df['is_crm_visible'] = final_df['is_crm_visible'].map(d)
        final_df = final_df.convert_dtypes()
        # separate data to Verdict and NSMG:

        nsmg_lists_ids_list = list(get_gbq_table('project-pulldata', QUERY_NSMG_LIST_MEMBERSHIP).list_id.unique())
        verdict_lists_ids_list = list(get_gbq_table('project-pulldata', QUERY_NSMG_LIST_MEMBERSHIP).list_id.unique())
        nsmg_df = final_df[final_df['id'].isin(nsmg_lists_ids_list)]
        verdict_df = final_df[final_df['id'].isin(verdict_lists_ids_list)]

        # NSMG:
        # get new IDs
        nsmg_ids = list(nsmg_df.id)
        update_list_table_nsmg = Updater(dataset_nsmg, table, nsmg_df, 'id', ['id'], nsmg_ids)
        update_list_table_nsmg.update_data()
        # Verdict:
        # get new IDs
        verdict_ids = list(verdict_df.id)
        update_list_table_verdict = Updater(dataset_verdict, table, verdict_df, 'id', ['id'], verdict_ids)
        update_list_table_verdict.update_data()
    else:
        nsmg_ids = []
        verdict_ids = []

    # record log file
    log_update(log_file_nsmg, nsmg_ids, table)
    log_update(log_file_verdict, verdict_ids, table)
    return True


def upload_data_opportunity(emails_counter: str) -> int:
    """
    Download Pardot Opportunity new data and update the table in GBQ and Postgre
    :return: Number of API calls made
    """
    project_id = PULLDATA_PROJECT_ID
    dataset_nsmg = NSMG_DATASET
    dataset_verdict = VERDICT_DATASET
    pardot_api = OPPORTUNITY_API
    table = OPPORTUNITY_TABLE
    log_file_nsmg = f'{LOGS_FOLDER}{NSMG_OPPORTUNITY_LOG}'
    log_file_verdict = f'{LOGS_FOLDER}{VERDICT_OPPORTUNITY_LOG}'
    data_name = OPPORTUNITY_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # get max created and updated values to start download form that values
    nsmg_max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                              dataset=dataset_nsmg,
                                                                              table=table))
    verdict_max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                                 dataset=dataset_verdict,
                                                                                 table=table))
    old_df_created_at = datetime.datetime.strptime(max(nsmg_max_created_updated.max_created_at.max(),
                        verdict_max_created_updated.max_created_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
    old_df_updated_at = datetime.datetime.strptime(max(nsmg_max_created_updated.max_updated_at.max(),
                        verdict_max_created_updated.max_updated_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
    # get new created data
    endtime = datetime.datetime.now()
    new_created_df, counter_created = get_new_created_updated_data(url, old_df_created_at,
                                                  'created',
                                                  data_name, endtime, int(emails_counter))
    print("Calls after collecting opportunity new data: ", counter_created)
    # get new updated data
    new_updated_df, counter_updated = get_new_created_updated_data(url, old_df_updated_at,
                                                  'updated',
                                                  data_name, endtime, counter_created)
    print("Calls after collecting opportunity updated data: ", counter_updated)
    # concat update and new data
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)
    final_df = final_df.drop_duplicates(subset='id', keep="last")
    # check if DataFrame is empty
    if not final_df.empty:
        final_df = fix_dataframe_columns(final_df)
        # fix data types
        final_df = final_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        if 'closed_at' not in final_df.columns:
            final_df['closed_at'] = pd.NaT
        date_cols = ['closed_at', 'created_at', 'updated_at']
        final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime,
                                                        format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
        if 'campaign' not in final_df.columns:
            final_df['campaign'] = None
        if 'prospects' not in final_df.columns:
            final_df['prospects'] = None
        if 'prospectsprospect' not in final_df.columns:
            final_df['prospectsprospect'] = None
        str_cols = ['campaign', 'prospects', 'prospectsprospect']
        final_df[str_cols] = final_df[str_cols].astype('str')
        final_df = final_df.convert_dtypes()
        # fix column prospecstprospect
        final_df['prospectsprospect'] = final_df['prospectsprospect'].astype('str')
        # separate data to Verdict and NSMG:
        url_nsmg = QUERY_NSMG_CAMPAIGNS
        nsmg_campaigns_ids_list = list(get_gbq_table('project-pulldata', url_nsmg).id.unique())
        nsmg_df = final_df[final_df['campaignid'].isin(nsmg_campaigns_ids_list)]
        verdict_df = final_df[~final_df['campaignid'].isin(nsmg_campaigns_ids_list)]
        # NSMG:
        # record IDs
        nsmg_ids = list(nsmg_df.id)
        # check if list is empty
        if len(nsmg_ids) > 0:
            update_list_table_nsmg = Updater(dataset_nsmg, table, nsmg_df, 'id', ['id'], nsmg_ids, upload_to_postgre=False)
            update_list_table_nsmg.update_data()
        else:
            print('There is no data to upload for NSMG opportunity table')
        # Verdict:
        # record IDs
        verdict_ids = list(verdict_df.id)
        if len(verdict_ids) > 0:
            update_list_table_verdict = Updater(dataset_verdict, table, verdict_df, 'id', ['id'], verdict_ids,
                                            upload_to_postgre=False)
            update_list_table_verdict.update_data()
        else:
            print('There is no data to upload for Verdict opportunity table')
    else:
        nsmg_ids = []
        verdict_ids = []
    # record log file
    log_update(log_file_nsmg, nsmg_ids, table)
    log_update(log_file_verdict, verdict_ids, table)
    return counter_updated


def upload_data_prospects() -> int:
    """
    Download Pardot Prospects new data and update the table in GBQ and Postgre
    :return: Number of API calls made
    """
    project_id = PULLDATA_PROJECT_ID
    dataset_nsmg = NSMG_DATASET
    dataset_verdict = VERDICT_DATASET
    pardot_api = PROSPECTS_API
    table = PROSPECTS_TABLE
    log_file_nsmg = f'{LOGS_FOLDER}{NSMG_PROSPECTS_LOG}'
    log_file_verdict = f'{LOGS_FOLDER}{VERDICT_PROSPECTS_LOG}'
    data_name = PROSPECTS_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # get old table from GBQ
    old_df_nsmg = get_gbq_table(project_id,
                                QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset_nsmg, table=table))
    old_df_verdict = get_gbq_table(project_id, QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset_verdict,
                                                                        table=table))
    # get new created data
    old_df_created_at = datetime.datetime.strptime(max(old_df_nsmg.created_at.max(),
                        old_df_verdict.created_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
    old_df_updated_at = datetime.datetime.strptime(max(old_df_nsmg.updated_at.max(),
                        old_df_verdict.updated_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')

    endtime = datetime.datetime.now()
    new_created_df, counter_created = get_new_created_updated_data(url, old_df_created_at, 'created',
                                                                   data_name, endtime, 0)
    print("Calls after collecting new data: ", counter_created)
    # get new updated data
    new_updated_df, counter_updated = get_new_created_updated_data(url, old_df_updated_at, 'updated',
                                                                   data_name, endtime, counter_created)
    print("Calls after collecting updated data: ", counter_updated)

    # concat update and new data
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)

    # check if DataFrame is empty
    if not final_df.empty:
        final_df = fix_dataframe_columns(final_df)
        # drop sensitive data
        final_df = final_df.drop(
            ['salutation', 'first_name', 'last_name', 'email', 'password', 'phone', 'fax', 'address_one',
             'address_two'], axis=1)
        try:
            final_df = final_df.drop(['mobile_phone'], axis=1)
        except:
            pass

        # fix data types
        final_df = final_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        # cols with date data for convert to datetime
        date_cols = ['created_at', 'updated_at', 'crm_last_sync',
                     'last_activity_at',
                     'last_activityvisitor_activitycreated_at',
                     'last_activityvisitor_activityupdated_at']
        # convert to datetime
        final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S', errors='ignore',
                                                        utc=True)
        final_df = final_df.convert_dtypes()
        final_df['id'] = final_df['id'].astype('int')
        final_df['campaign_id'] = final_df['campaign_id'].astype('int')
        url_nsmg = QUERY_NSMG_CAMPAIGNS
        nsmg_campaigns_ids_list = list(get_gbq_table('project-pulldata', url_nsmg).id.unique())
        prospects_nsmg_df = final_df[final_df['campaign_id'].isin(nsmg_campaigns_ids_list)]
        prospects_verdict_df = final_df[~final_df['campaign_id'].isin(nsmg_campaigns_ids_list)]
        # NSMG:

        final_nsmg_df = pd.concat([old_df_nsmg, prospects_nsmg_df], axis=0, ignore_index=True, sort=False)
        # drop duplicates rows
        final_nsmg_df = final_nsmg_df.drop_duplicates(subset='id', keep="last")
        final_nsmg_df = final_nsmg_df.loc[:, ~final_nsmg_df.columns.duplicated()]
        # get list of prospect ID for log
        ids_nsmg = list(final_nsmg_df.id)

        # upload data to gbq
        print('Upload NSMG prospects to GBQ')
        final_nsmg_df.to_gbq(destination_table=f'{dataset_nsmg}.{table}', project_id=project_id, if_exists='replace')

        # VERDICT:

        final_verdict_df = pd.concat([old_df_verdict, prospects_verdict_df], axis=0, ignore_index=True, sort=False)
        # drop duplicates rows
        final_verdict_df = final_verdict_df.drop_duplicates(subset='id', keep="last")
        final_verdict_df = final_verdict_df.loc[:, ~final_verdict_df.columns.duplicated()]
        # get list of prospect ID for log
        ids_verdict = list(final_verdict_df.id)

        # upload data to gbq
        print('Upload Verdict prospects to GBQ.')
        final_verdict_df.to_gbq(destination_table=f'{dataset_verdict}.{table}', project_id=project_id,
                                if_exists='replace')
        chunks = split(final_verdict_df, 20000)
        for c in chunks:
            # update metadata
            update_metadata(project_id, POSTGRE_AUTH, c, dataset_verdict, table)
        chunks = split(final_nsmg_df, 20000)
        for c in chunks:
            # update metadata
            update_metadata(project_id, POSTGRE_AUTH, c, dataset_nsmg, table)
    else:
        ids_nsmg, ids_verdict = [], []
    # record log file
    log_update(log_file_nsmg, ids_nsmg, table)
    log_update(log_file_verdict, ids_verdict, table)
    return counter_updated


def upload_data_visitors(opportunity_counter: str) -> int:
    """
    Download Pardot visitors new data and update the table in GBQ and Postgre
    :return: True if Pardot visitors data is updated
    """
    project_id = PULLDATA_PROJECT_ID
    dataset_nsmg = NSMG_DATASET
    dataset_verdict = VERDICT_DATASET
    pardot_api = VISITORS_API
    table = VISITORS_TABLE
    log_file_nsmg = f'{LOGS_FOLDER}{NSMG_VISITORS_LOG}'
    log_file_verdict = f'{LOGS_FOLDER}{VERDICT_VISITORS_LOG}'
    data_name = VISITORS_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # get max created and updated values to start download form that values
    nsmg_max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                              dataset=dataset_nsmg,
                                                                              table=table))
    verdict_max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                                 dataset=dataset_verdict,
                                                                                 table=table))
    old_df_created_at = datetime.datetime.strptime(max(nsmg_max_created_updated.max_created_at.max(),
                        verdict_max_created_updated.max_created_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
    old_df_updated_at = datetime.datetime.strptime(max(nsmg_max_created_updated.max_updated_at.max(),
                        verdict_max_created_updated.max_updated_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')

    old_df_nsmg_prospect = get_gbq_table(project_id,
                                         QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset_nsmg,
                                                                  table='Prospects'))
    old_df_verdict_prospect = get_gbq_table(project_id,
                                            QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset_verdict,
                                                                     table='Prospects'))
    # get new created data
    endtime = datetime.datetime.strptime(max(old_df_nsmg_prospect.created_at.max(), 
                                             old_df_verdict_prospect.created_at.max()).strftime('%Y-%m-%d %H:%M:%S'),
                                         '%Y-%m-%d %H:%M:%S')
    # get new created data
    new_created_df, counter_created = get_new_created_updated_data(url, old_df_created_at, 'created',
                                                  data_name, endtime, int(opportunity_counter), before_mark_delta=0.1)
    print("Calls after collecting visitors new data: ", counter_created)
    # get new updated data
    new_updated_df, counter_updated = get_new_created_updated_data(url, old_df_updated_at, 'updated',
                                                  data_name, endtime, counter_created, before_mark_delta=1)
    print("Calls after collecting visitors updated data: ", counter_updated)
    # concat update and new data
    final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)
    final_df = final_df.drop_duplicates(subset='id', keep="last")

    # check if DataFrame is empty
    if not final_df.empty:
        # fix data types
        final_df = final_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        date_cols = ['created_at', 'updated_at']
        final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime,
                                                        format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
        str_cols = ['campaign_parameter', 'medium_parameter', 'source_parameter','content_parameter','term_parameter']
        final_df[str_cols] = final_df[str_cols].astype('str')
        #final_df = final_df.convert_dtypes()
        # separate data to Verdict and NSMG:

        nsmg_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_NSMG_PROSPECTS).id.unique())
        nsmg_df = final_df[final_df['prospect_id'].isin(nsmg_prospect_ids_list)]
        verdict_df = final_df[~final_df['prospect_id'].isin(nsmg_prospect_ids_list)]
        # NSMG:
        # record IDs
        nsmg_ids = list(nsmg_df.id)
        chunks = split(nsmg_df, 20000)
        for c in chunks:
            ids_details = list(c.id)
            update_list_verdict = Updater(dataset_nsmg, table, c, 'id', ['id'], ids_details, upload_to_postgre=False)
            update_list_verdict.update_data()
        # Verdict:
        # record IDs
        verdict_ids = list(verdict_df.id)
        # split final data in chunks and run updater class on every chunk
        # we split the data because is to big and can't be uploaded at onc
        chunks = split(verdict_df, 20000)
        for c in chunks:
            ids_details = list(c.id)
            update_list_verdict = Updater(dataset_verdict, table, c, 'id', ['id'], ids_details, upload_to_postgre=False)
            update_list_verdict.update_data()
    else:
        nsmg_ids = []
        verdict_ids = []
    # record log file
    log_update(log_file_nsmg, nsmg_ids, table)
    log_update(log_file_verdict, verdict_ids, table)
    return counter_updated


def upload_data_visits() -> bool:
    """
    Download Pardot List Membership new data and update the table in GBQ and Postgre
    :return: True if Pardot List Membership data is updated
    """
    dataset_nsmg = NSMG_DATASET
    dataset_verdict = VERDICT_DATASET
    pardot_api = VISITS_API
    table = VISITS_TABLE
    log_file_nsmg = f'{LOGS_FOLDER}{NSMG_VISITS_LOG}'
    log_file_verdict = f'{LOGS_FOLDER}{VERDICT_VISITS_LOG}'
    data_name = VISITS_DATA_NAME
    url = GET_BATCH_URL.format(pardot_api=pardot_api)
    # separate data to Verdict and NSMG:

    nsmg_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_NSMG_PROSPECTS).id.unique())
    verdict_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_VERDICT_PROSPECTS).id.unique())
    # NSMG:
    nsmg_all_df = pd.DataFrame()
    for ids in nsmg_prospect_ids_list:
        new_created_df = get_new_created_data_visits(url, '10', ids, data_name)
        nsmg_all_df = pd.concat([nsmg_all_df, new_created_df], axis=0, ignore_index=True, sort=False)
    nsmg_all_df = unnest_page_views(nsmg_all_df)
    nsmg_ids = list(nsmg_all_df.id)
    nsmg_all_df = nsmg_all_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    date_cols = ['created_at', 'updated_at', 'first_visitor_page_view_at', 'last_visitor_page_view_at']
    nsmg_all_df[date_cols] = nsmg_all_df[date_cols].apply(pd.to_datetime,
                                                          format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
    nsmg_all_df = nsmg_all_df.convert_dtypes()
    nsmg_all_df = fix_dataframe_columns(nsmg_all_df)
    update_list_table_nsmg = Updater(dataset_nsmg, table, nsmg_all_df, 'id', ['id'], nsmg_ids)
    update_list_table_nsmg.update_data()
    # Verdict:
    verdict_all_df = pd.DataFrame()
    for ids in verdict_prospect_ids_list:
        new_created_df = get_new_created_data_visits(url, '10', ids, data_name)
        verdict_all_df = pd.concat([verdict_all_df, new_created_df], axis=0, ignore_index=True, sort=False)
    verdict_all_df = unnest_page_views(verdict_all_df)
    verdict_ids = list(verdict_all_df.id)
    verdict_all_df = verdict_all_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
    date_cols = ['created_at', 'updated_at', 'first_visitor_page_view_at', 'last_visitor_page_view_at']
    verdict_all_df[date_cols] = verdict_all_df[date_cols].apply(pd.to_datetime,
                                                                format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
    verdict_all_df = verdict_all_df.convert_dtypes()
    verdict_all_df = fix_dataframe_columns(verdict_all_df)
    update_list_table_nsmg = Updater(dataset_verdict, table, verdict_all_df, 'id', ['id'], verdict_ids)
    update_list_table_nsmg.update_data()

    nsmg_ids = []
    verdict_ids = []
    # record log file
    log_update(log_file_nsmg, nsmg_ids, table)
    log_update(log_file_verdict, verdict_ids, table)
    return True


# def upload_data_visitor_activity(visitors_counter: str) -> bool:
def upload_data_visitor_activity() -> bool:
    """
    Download Pardot Visitor Activity new data and update the table in GBQ and Postgre
    :return: True if Pardot Visitor Activity data is updated
    """
    visitors_counter = '0'
    if datetime.datetime.now().weekday() == 4:
        project_id = PULLDATA_PROJECT_ID
        dataset_nsmg = NSMG_DATASET
        dataset_verdict = VERDICT_DATASET
        pardot_api = VISITOR_ACTIVITY_API
        table = VISITOR_ACTIVITY_TABLE
        log_file_nsmg = f'{LOGS_FOLDER}{NSMG_VISITOR_ACTIVITY_LOG}'
        log_file_verdict = f'{LOGS_FOLDER}{VERDICT_VISITOR_ACTIVITY_LOG}'
        data_name = VISITOR_ACTIVITY_DATA_NAME
        url = GET_BATCH_URL.format(pardot_api=pardot_api)
        # get max created and updated values to start download form that values
        for _ in range(8):
            nsmg_max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                                  dataset=dataset_nsmg,
                                                                                  table=table))
            verdict_max_created_updated = get_gbq_table(project_id, QUERY_MAX_MIN.format(project_id=project_id,
                                                                                     dataset=dataset_verdict,
                                                                                     table=table))
            old_df_created_at = datetime.datetime.strptime(max(nsmg_max_created_updated.max_created_at.max(),
                                verdict_max_created_updated.max_created_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
            old_df_updated_at = datetime.datetime.strptime(max(nsmg_max_created_updated.max_updated_at.max(),
                                verdict_max_created_updated.max_updated_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
            # get old table for prospects from GBQ
            old_df_nsmg_prospect = get_gbq_table(project_id, QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset_nsmg, table='Prospects'))
            old_df_verdict_prospect = get_gbq_table(project_id, QUERY_WHOLE_TABLE.format(project_id=project_id, dataset=dataset_verdict, table='Prospects'))
            # get new created data
            endtime = datetime.datetime.strptime(max(old_df_nsmg_prospect.created_at.max(), old_df_verdict_prospect.created_at.max()).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
            # get new created data
            new_created_df, counter_created = get_new_created_updated_data(url, old_df_created_at, 'created',
                                                                           data_name, endtime, int(visitors_counter), max_calls=50)
            print("Calls after collecting new data: ", counter_created)
            # get new updated data
            new_updated_df, counter_updated = get_new_created_updated_data(url, old_df_updated_at, 'updated',
                                                                           data_name, endtime, counter_created, max_calls=50)
            print("Calls after collecting updated data: ", counter_updated)
            # concat update and new data
            final_df = pd.concat([new_created_df, new_updated_df], axis=0, ignore_index=True, sort=False)
            final_df = final_df.drop_duplicates(subset='id', keep="last")

            # check if DataFrame is empty
            if not final_df.empty:
                # fix columns
                final_df = fix_dataframe_columns(final_df)
                # fix data types
                final_df = final_df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
                date_cols = ['created_at', 'updated_at']
                final_df[date_cols] = final_df[date_cols].apply(pd.to_datetime,
                                                                format='%Y-%m-%d %H:%M:%S', errors='ignore', utc=True)
                if 'email_id' not in final_df.columns:
                    final_df['email_id'] = None
                #final_df = final_df.convert_dtypes()
                # separate data to Verdict and NSMG:

                nsmg_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_NSMG_PROSPECTS).id.unique())
                verdict_prospect_ids_list = list(get_gbq_table('project-pulldata', QUERY_VERDICT_PROSPECTS).id.unique())
                nsmg_df = final_df[final_df['prospect_id'].isin(nsmg_prospect_ids_list)]
                verdict_df = final_df[~final_df['prospect_id'].isin(nsmg_prospect_ids_list)]
                # NSMG:
                # record IDs
                nsmg_ids = list(nsmg_df.id)
                # split final data in chunks and run updater class on every chunk
                # we split the data because is to big and can't be uploaded at onc
                chunks = split(nsmg_df, 20000)
                for c in chunks:
                    ids_details = list(c.id)
                    update_list_nsmg = Updater(dataset_nsmg, table, c, 'id', ['id'], ids_details, upload_to_postgre=False)
                    update_list_nsmg.update_data()

                # Verdict:
                # record IDs
                verdict_ids = list(verdict_df.id)
                chunks = split(verdict_df, 20000)
                for c in chunks:
                    ids_details = list(c.id)
                    update_list_verdict = Updater(dataset_verdict, table, c, 'id', ['id'], ids_details, upload_to_postgre=False)
                    update_list_verdict.update_data()
            else:
                nsmg_ids = []
                verdict_ids = []
            # record log file
            log_update(log_file_nsmg, nsmg_ids, table)
            log_update(log_file_verdict, verdict_ids, table)
            # set counter to 0 for the next loop of 19500
            visitors_counter = 0
    else:
        print("Skipping visitors activity download.")
    return True
