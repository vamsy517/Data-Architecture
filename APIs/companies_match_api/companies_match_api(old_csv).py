import werkzeug
werkzeug.cached_property = werkzeug.utils.cached_property
from flask import Flask
from flask_restplus import Api, Resource
from flask_restplus import reqparse
from flask import Flask, jsonify, request, make_response
import os
import pandas as pd
import numpy as np
from Levenshtein import distance as levenshtein_distance
import time
import re
from cleanco import cleanco

# load in memory list with all companies that we use for matching and their names afte cleaning
# we use already cleaned names to save time to cleaning every time
clearbit_df = list(pd.read_csv('clearbit_all.csv').name)
clearbit_all = [item if type(item) == str else str(item) for item in clearbit_df]
df_fixname = list(pd.read_csv('Fixed.csv').name)
df_fixname = [item if type(item) == str else str(item) for item in df_fixname]
#function to clean companies names
pattern = re.compile(r"[[:punct:]]+")
def clean_name(name):
    '''
        We set name to lower case, replace all & with and strip and remove group
        Use regeg to clean punctuation
        Use cleanco library to clean company names
        You can check what cleanco do in official libery documentation:
        https://pypi.org/project/cleanco/
        Input: company name
        Output: clean name
    '''
    name = name.lower().replace('&', 'and').replace('group', '').strip()
    name = pattern.sub("", name)
    name = cleanco(name).clean_name()
    return name
def get_distance(df_names, n_closest, threshold, clearbit_all, df_fixname):
    '''
        Input:
            df_names: dataframe with companies that we will find matches
            n_closest: numer of returned matches
            threshold: prefferred threshold for Levenshtein distance
            clearbit_all: load in memory list with all companies that we use for matching
        Return:
            copy_wishlist_df: dataframe with all matched companie names
    '''
    # function to get list of closed matches that we will apply to dataframe with companies that we will upload
    def get_closest_names(company_name,  clearbit_df, df_fixname, n_closest, threshold):
        '''
            Input:
                company_name: single company name
                clearbit_all: list with all companies that we will use to match
                n_closest: numer of returned matches
                threshold: prefferred threshold for Levenshtein distance
            Return: 
                closest_names: return list with closest names
        '''
        # levenshtein distances list with levenshtein distance score for that spesific company name
        levenshtein_distances_list = [
            levenshtein_distance(company_name, clearbit_company) for clearbit_company in df_fixname]
        # list with n closest indexes from list of all companies
        closest_strings_indices = np.argsort(levenshtein_distances_list)[:n_closest]
        # dict with n closest indexes from list of all companies as keys and coresponding distance score
        key_value_dict = {index:levenshtein_distances_list[index] for index in closest_strings_indices}
        # if threshold is None get list with closest names indexes
        # else get list with closest names indexes and add None for indexes bigger then threshold 
        if threshold == 'None':
            closest_names = [clearbit_all[index] for index in closest_strings_indices.tolist()]
        else:
            closest_names = [clearbit_all[index] if key_value_dict[index] <= int(threshold) else 'None' for index in closest_strings_indices.tolist()]
        return closest_names
    
    # create column, containing the closest matches as list by aplling on every df row the get_closest_names function
    df_name_fix = df_names.copy()
    df_name_fix['name'] = df_names['name'].apply(clean_name)
    df_names['closest_companies'] = df_name_fix.apply(lambda x: get_closest_names(
    x['name'],  clearbit_df, df_fixname, n_closest, threshold), axis = 1)
    # copy the dataframe
    copy_wishlist_df = df_names.copy()
    # create separate column for every element in the list and remove closest_companies column
    closest_strings = [f'closest_str_{i + 1}' for i in range(len(copy_wishlist_df.iloc[0,-1]))]
    copy_wishlist_df['len'] = copy_wishlist_df.apply(lambda x: len(x['closest_companies']), axis = 1)
    assert copy_wishlist_df['len'].unique()[0] == n_closest
    copy_wishlist_df[closest_strings] = pd.DataFrame(copy_wishlist_df.closest_companies.tolist(), index=copy_wishlist_df.index)
    copy_wishlist_df = copy_wishlist_df.drop(['closest_companies', 'len'], axis = 1)
    return copy_wishlist_df

# create file upload field and add arguments
file_upload = reqparse.RequestParser()
file_upload.add_argument('csv_file',
                         type=werkzeug.datastructures.FileStorage, 
                         location='files', 
                         required=True, 
                         help='Upload csv with company names.')
file_upload_time = reqparse.RequestParser()
file_upload_time.add_argument('csv_file',
                         type=werkzeug.datastructures.FileStorage, 
                         location='files', 
                         required=True, 
                         help='Upload csv with company names.')
# create flask app object
flask_app = Flask(__name__)
# config max content length for uploaded file to be no more then 100 mb
flask_app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
#
app = Api(app = flask_app, 
          version = "1.0", 
          title = "Company Name Matching API", 
          description = "Return closest company names based on provided list.<br/><br/>DSCOE Team:<br/>- Yavor Vrachev (Head of Data Architecture) - yavor.vrachev@ns-mediagroup.com<br/>- Veselin Valchev (Lead of Data Science Team) - veselin.valchev@ns-mediagroup.com<br/>- Nina Nikolaeva - nina.nikolaeva@ns-mediagroup.com<br/>- Petyo Karatov - petyo.karatov@ns-mediagroup.com<br/>- Dzhumle Mehmedova - dzhumle.mehmedova@ns-mediagroup.com")

name_space_time = app.namespace('CheckExecTime', description='Check how long will take to execute Company Names Matching')
name_space_match = app.namespace('CompanyNamesMatch', description='Perform Company Names Matching')

# app.config['Upload_folder'] = './temp_download/'
# gobal var allowed extension for files that we upload
ALLOWED_EXTENSIONS = {'csv'}
# finction to check if the file that try to upload is in correct format( extension is .csv)
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# main app class
@name_space_time.route("/check_exec_time")
class check_time(Resource):
    # variable with params for creation the textboxes for params
    params={
        'token': {'description': 'Enter token','type':'string'}
    }    
    @app.expect(file_upload)
    @app.doc(params=params)
    def post(self):
        # get token from request
        token = request.args.get('token')
        # check if the token is valid
        # and return Invalid tokes if not
        if token != 'GRJufHnAf4Dv4Tzu07jvaKfK0tA':
            return jsonify('Invalid token')
        # get the uploaded file as werkzeug.datastructures.FileStorage object
        args = file_upload_time.parse_args()
#         args['doc1'].save(os.path.join(app.config['Upload_folder'],secure_filename(args['doc1'].filename)))
        # get actual temp file
        csv_file = args['csv_file'].stream
        # check if the file is in correct format
        if not allowed_file(args['csv_file'].filename):
            return jsonify('File is not in csv format')
        # create dataframe from uploaded csv file
        df_names = pd.read_csv(csv_file)
        # check if the content of the datafame is one cloumn with header 'name'
        # if not return error message
        if not(df_names.shape[1] == 1 and df_names.columns[0] == 'name'):
            return jsonify('File content in not in correct format')
        # get number of names and check how time will take to run companies match
        number_of_names = df_names.shape[0]
        aprox_time = str(number_of_names * 1.3) + ' seconds' 
        return jsonify(aprox_time)

@name_space_match.route("/companies_match")
class my_file_upload(Resource):
    # variable with params for creation the textboxes for params
    params={
        'n_closest': {'description': 'Enter a prefferred number of returned companies','type':'int', 'default':3},
        'threshold': {'description': 'Enter a prefferred threshold for distance between 0 and 10, where 0 means an exact match','type':'int', 'default':'None'},
        'token': {'description': 'Enter token','type':'string'},
    }    
    @app.expect(file_upload)
    @app.doc(params=params)
    def post(self):
        start_time = time.time()
        # get token from request
        token = request.args.get('token')
        # check if the token is valid
        # and return Invalid tokes if not
        if token != 'GRJufHnAf4Dv4Tzu07jvaKfK0tA':
            return jsonify('Invalid token')
        # get the uploaded file as werkzeug.datastructures.FileStorage object
        args = file_upload.parse_args()
#         args['doc1'].save(os.path.join(app.config['Upload_folder'],secure_filename(args['doc1'].filename)))
        # get actual temp file
        csv_file = args['csv_file'].stream
        # check if the file is in correct format
        if not allowed_file(args['csv_file'].filename):
            return jsonify('File is not in csv format')
        # create dataframe from uploaded csv file
        df_names = pd.read_csv(csv_file)
        # check if the content of the datafame is one cloumn with header 'name'
        # if not return error message
        if not(df_names.shape[1] == 1 and df_names.columns[0] == 'name'):
            return jsonify('File content in not in correct format')
        # get n closest parameter and check if it is integer    
        n_closest = request.args.get('n_closest')
        if n_closest.isdigit():
            n_closest = int(request.args.get('n_closest'))
        else:
            return jsonify('Please provide intiger number for n_closest')
        # get threshold parameter and check if it is integer or None
        threshold = request.args.get('threshold')
        if threshold.isdigit() or threshold == 'None':
            # if int or None we execute get_distance to get final dataframe
            copy_wishlist_df = get_distance(df_names, n_closest, threshold, clearbit_df, df_fixname)
        else:
            return jsonify('Please provide intiger number for threshold or None')
        # create response for download the csv file with the generated matches
        resp = make_response(copy_wishlist_df.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=generated_matches.csv"
        resp.headers["Content-Type"] = "text/csv"
        print("--- %s seconds ---" % (time.time() - start_time))
        return resp

if __name__ == '__main__':
    #run the app on port 10_005 on the local machine
    app.run(host='0.0.0.0', port=10005, debug=False)