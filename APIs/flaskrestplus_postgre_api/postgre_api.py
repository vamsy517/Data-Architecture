import werkzeug
werkzeug.cached_property = werkzeug.utils.cached_property
from flask import Flask, jsonify, request, send_from_directory
from flask_restplus import Api, Resource, fields 
from flask_restplus import reqparse
import os
import numpy as np
from sqlalchemy import create_engine
import json
import os
import pandas as pd
from datetime import datetime

##########################################################################################################################
# Create the engine for the sqlachemy connection
engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres', case_sensitive = True)

# Get the data from postgre with the specified datasets and tables
def postgre_api(request):
    whitelist = ['covid19data','banks', 'city', 'companies', 'metadata', 'country', 'mnc', 'bankingcompanyprofiles']
    # Check if url contains DatasetList option 
    if 'DatasetList' in request.keys():
        datasetlist = request['DatasetList']
        # Get list of all datasets
        datasets_list_raw = engine.execute(f"SELECT * FROM INFORMATION_SCHEMA.SCHEMATA").fetchall()
        datasets_list = [item[1] for item in datasets_list_raw]
        dataset_sanitised = [item for item in datasets_list if item in whitelist]
        response = dataset_sanitised
    else:
        # Set the requested dataset
        schema = request['Dataset']
        # Check if we should return dataset or table
        if request['TableOrDataset'] == 'Dataset':
            # if schema is not in schemas list return json.dumps('Invalid query.')
            schemas_list_raw = engine.execute(f"SELECT * FROM INFORMATION_SCHEMA.SCHEMATA").fetchall()
            schemas_list = [item[1] for item in schemas_list_raw]
            schema_sanitised = [item for item in schemas_list if item in whitelist]
            if schema in schema_sanitised:
                # in case we should return the dataset, we prepare the sql query to extract the list of tables in that dataset
                tables = engine.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'").fetchall()
                # transform the list of tables into json format
                response = json.dumps([item[2] for item in tables])
            else:
                response = json.dumps('Invalid Dataset parameter.')
        elif request['TableOrDataset'] == 'Table':
            # if schema is not in schemas list return json.dumps('Invalid query.')
            schemas_list_raw = engine.execute(f"SELECT * FROM INFORMATION_SCHEMA.SCHEMATA").fetchall()
            schemas_list = [item[1] for item in schemas_list_raw]
            schemas_sanitised = [item for item in schemas_list if item in whitelist]
            if schema in schemas_sanitised:
                # if table is not in schemas list return json.dumps('Invalid query.') 
                # in case we should return the dataset, we prepare the sql query to extract the list of tables in that dataset
                tables_list_raw = engine.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'").fetchall()
                tables_list = [item[2] for item in tables_list_raw]
                # Set the requested table name
                table = request['Table']
                if table in tables_list:
                    # if we should return the table itself , we prepare the sql to extract that table
                    tables = engine.execute(f'SELECT * FROM {schema}."{table}"').fetchall()
                    # change datetime values to string
                    def myconverter(o):
                        if isinstance(o, datetime):
                            return o.__str__()
                    # transform the list of tables into json format
                    # use the custom myconverter to conver the datetime type to string
                    # because datetime is not JSON serializable
                    response = json.dumps([dict(table) for table in tables], default=myconverter)
                else:
                    response = json.dumps('Invalid Table parameter.')
            else:
                response = json.dumps('Invalid Dataset parameter.')
        else:
            response = json.dumps('Invalid TableOrDataset parameter.')
    return response

def checkToken(token):
    # access postgre to get valid token
    tokens = engine.execute(f'SELECT tokens FROM dataextractionapi.accesstokens WHERE deprecated = False').fetchall()
    # if token is correct, increment the api calls counter
    if token in [orig_token[0] for orig_token in tokens]:
        token_s = "'" + token + "'"
        counts = engine.execute(f'UPDATE dataextractionapi.accesstokens SET "apicallscount" = "apicallscount" + 1 WHERE "tokens" = {token_s}')
    return (token in [orig_token[0] for orig_token in tokens])


###########################################################################################################################    
###########################################################################################################################           
# create Flask API
flask_app = Flask(__name__)

app = Api(app = flask_app, 
          version = "1.0", 
          title = "Postgre API", 
          description = "Extract datasets and tables from Postgre<br/><br/>DSCOE Team:<br/>- Yavor Vrachev (Head of Data Architecture) - yavor.vrachev@ns-mediagroup.com<br/>- Veselin Valchev (Lead of Data Science Team) - veselin.valchev@ns-mediagroup.com<br/>- Nina Nikolaeva - nina.nikolaeva@ns-mediagroup.com<br/>- Petyo Karatov - petyo.karatov@ns-mediagroup.com<br/>- Dzhumle Mehmedova - dzhumle.mehmedova@ns-mediagroup.com")

name_space = app.namespace('NSMGData')
@name_space.route("/")
class MainClass(Resource):
    
    params={
        'Token': {'description': 'Enter Authentication Token (if no token is provided, please contact DSCOE team)', 'type': 'string'},
        'DatasetList': {'description': 'Enter any value to see a list of available datasets (if empty, fill in the parameters below)', 'type': 'string'},
        'TableOrDataset': {'description': 'Enter "Dataset" to request all tables for a specific dataset or "Table" to download a specific table', 'type': 'string'},
        'Dataset': {'description': 'Enter a valid lowercase dataset name', 'type': 'string'}, 
        'Table': {'description': 'Enter a valid table name', 'type': 'string'},
        'JSON': {'description': 'Enter "yes" to get response as JSON. Leave empty to download archive with csv.<br/>NOTE: getting large tables as JSON may get the browser to freeze!', 'type': 'string'},
           }
    @app.doc(params=params)
    def get(self):
        token = request.args.get('Token')
        call_datetime = datetime.now()
        datasetlist = request.args.get('DatasetList')
        tableordataset = request.args.get('TableOrDataset')
        dataset = request.args.get('Dataset')
        table = request.args.get('Table')
        json = request.args.get('JSON')
        api_caller = request.remote_addr
        # log only salted parameters
        dict_log = {
            "token": '-' + str(token),
            "call_datetime": call_datetime,
            "datasetlist": '-' + str(datasetlist),
            "tableordataset": '-' + str(tableordataset),
            "dataset": '-' + str(dataset),
            "table": '-' + str(table),
            "json": '-' + str(json),
            "api_caller": api_caller
        }
        # create dataframe with all information from API
        df_log = pd.DataFrame([dict_log])
        df_log = df_log.set_index('token')
        # add log to Posgtre database
        df_log.to_sql(f'calls_history',schema='dataextractionapi',
                          con=engine,method='multi', chunksize = 100000, if_exists = 'append')
        
        if checkToken(token):
            # check DatasetList parameter
            if datasetlist:
                request_json = {"DatasetList": datasetlist
                               }
                results = postgre_api(request_json)
            else:
                # parse request

                request_json = {"TableOrDataset": tableordataset,
                                    "Dataset": dataset,
                                    "Table" : table
                                    }
                results = postgre_api(request_json)
                # check if Table is set as parameter for TableOrDataset and if the provided table name table is a valid table
                if (tableordataset == 'Table') and ('Invalid' not in results):
                    # read the API results to dataframe
                    df = pd.read_json(results)
                    df = df.drop(['index'], axis=1)
                    # create name for the data file
                    fileName = f'{dataset}_{table}_' + datetime.now().strftime('%Y_%m_%d_%H%M%S%f')
                    archive_name_text = fileName + '.csv'
                    # add compression options to save as gz
                    compression_opts = dict(method='gzip',archive_name=archive_name_text)
                    # create compressed csv file
                    df.to_csv(os.getcwd() + '/result_files/' + fileName + '.gz', index=False, compression=compression_opts)
                    # return option to download the compressed file
                    if (json == 'yes'):
                        return results
                    else:
                        return send_from_directory(os.getcwd() + '/result_files', fileName + '.gz', as_attachment=True)
        else:
            results = 'Access denied, token invalid.'
        return jsonify(results)
