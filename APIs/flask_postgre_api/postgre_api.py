from flask import Flask, jsonify, request
import os
import numpy as np
from sqlalchemy import create_engine
import json

# Create the engine for the sqlachemy connection
engine = create_engine('postgresql+psycopg2://postgres:test@104.155.153.113:5432/postgres', case_sensitive = True)

# Get the data from postgre with the specified datasets and tables
def postgre_api(request):
    # Check if url contains DatasetList option 
    if 'DatasetList' in request.keys():
        datasetlist = request['DatasetList']
        # Get list of all datasets
        datasets_list_raw = engine.execute(f"SELECT * FROM INFORMATION_SCHEMA.SCHEMATA").fetchall()
        datasets_list = [item[1] for item in datasets_list_raw]
        response = datasets_list
    else:
        # Set the requested dataset
        schema = request['Dataset']
        # Check if we should return dataset or table
        if request['TableOrDataset'] == 'Dataset':
            # if schema is not in schemas list return json.dumps('Invalid query.')
            schemas_list_raw = engine.execute(f"SELECT * FROM INFORMATION_SCHEMA.SCHEMATA").fetchall()
            schemas_list = [item[1] for item in schemas_list_raw]
            if schema in schemas_list:
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
            if schema in schemas_list:
                # if table is not in schemas list return json.dumps('Invalid query.') 
                # in case we should return the dataset, we prepare the sql query to extract the list of tables in that dataset
                tables_list_raw = engine.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'").fetchall()
                tables_list = [item[2] for item in tables_list_raw]
                # Set the requested table name
                table = request['Table']
                if table in tables_list:
                    # if we should return the table itself , we prepare the sql to extract that table
                    tables = engine.execute(f'SELECT * FROM {schema}."{table}"').fetchall()
                    # transform the list of tables into json format
                    response = json.dumps([dict(table) for table in tables])
                else:
                    response = json.dumps('Invalid Table parameter.')
            else:
                response = json.dumps('Invalid Dataset parameter.')
        else:
            response = json.dumps('Invalid TableOrDataset parameter.')
    return response
        
        
# create Flask API
app = Flask(__name__)
# decorate the app with the route
@app.route('/NSMGdata', methods=['POST','GET'])
# function to parse get request and return results
def extract():
    # check request type
    if request.method == 'POST':
        # parse request
        request_json = request.get_json()
        # perform token authentication
        if checkToken(request_json['Token']):
            results = postgre_api(request_json)
        else:
            results = 'Access denied, token invalid.'
    else:
        # perform token authentication
        token = request.args.get('Token')
        if checkToken(token):
            # check DatasetList parameter
            if request.args.get('DatasetList'):
                request_json = {"DatasetList": request.args.get('DatasetList')}
            else:
                # parse request
                table_or_dataset = request.args.get('TableOrDataset')
                dataset = request.args.get('Dataset')
                table = request.args.get('Table')
                request_json = {"TableOrDataset": table_or_dataset,
                                    "Dataset": dataset,
                                    "Table" : table}
            results = postgre_api(request_json)
        else:
            results = 'Access denied, token invalid.'
        
    return jsonify(results)

def checkToken(token):
    # access postgre to get valid token
    tokens = engine.execute(f'SELECT tokens FROM dataextractionapi.accesstokens WHERE deprecated = False').fetchall()
    # if token is correct, increment the api calls counter
    if token in list(tokens[0]):
        token_s = "'" + token + "'"
        counts = engine.execute(f'UPDATE dataextractionapi.accesstokens SET "apicallscount" = "apicallscount" + 1 WHERE "tokens" = {token_s}')
    return (token in list(tokens[0]))
              
if __name__ == '__main__':
    #run the app on port 10_000 on the local machine
    app.run(host='0.0.0.0', port=10000, debug=False)