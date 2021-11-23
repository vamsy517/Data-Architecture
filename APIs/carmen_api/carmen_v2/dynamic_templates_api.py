import werkzeug
werkzeug.cached_property = werkzeug.utils.cached_property
from flask import Flask, jsonify, request, send_from_directory
from flask_restplus import Api, Resource, fields 
from flask_restplus import reqparse
from datetime import datetime

from carmen import CarmenTemplates
from code_base import post_request, logpr

# dynamic_temps
# dynamic_temps
    
###########################################################################################################################           
# create Flask API
flask_app = Flask(__name__)

tmp = CarmenTemplates()

app = Api(app = flask_app, 
          version = tmp.version__(), 
          title = "CARMEN API", 
          description = '''CARMEN stands for Content Automated by Robots for the Monitor E-publishing Network''')

name_space = app.namespace('NSMGCARMEN')
@name_space.route("/")
class MainClass(Resource):
    
    params={
        'token': {'description': 'Enter Authentication Token (if no token is provided, please contact DSCOE team)', 'type': 'string'},
        'dataset': {'description': 'Name of the dataset with lower case letters (e.g. country)','type':'string'},
        'country': {'description': 'Accepted parameter values:<br/>1. Default is United Kingdom - if the field is left blank.<br/>2. Multiple countries can be selected by typing them out separated with "," and NO spaces. Example: Bulgaria,Serbia,Germany.<br/>3. Bulk option - by typing out "bulk" the API will generate outptut for all available countries in the Countries dataset in GBQ.<br/>4. A number of countries (up to 10).', 'type': 'string'},
        'create_wp': {'description': "Enter 'yes' to create a post with the generated information on WP. Default is 'no'", 'type': 'string'},
        'sortby': {'description': "This is only usable when the country parameter is an number. Sort by a specific feature (e.g. Population) in the country data.", 'type': 'string'},
        'text': {'description': 'Input the text template', 'type': 'string'},
        }
    @app.doc(params=params)
    def get(self):
        #token = request.args.get('Token')
        params_dict = {
            'call_datetime': datetime.now(),
            'api_caller': request.remote_addr
        }
        for param in ['country','create_wp','sortby','text','dataset','token']:
            params_dict[param] = request.args.get(param)

        if params_dict['token'] == 'PA-QNYYtayIhNAeqUlUlB8m72cirGHfXQiIRtucvURL7JFPNS1IXMJD_G4g2GtREjAaC1CYKZKvSynOuERCRm05HCRbp':
            a = CarmenTemplates()
            a.text_parse_tags(text=params_dict['text'])
            a.get_params(country_list=params_dict['country'], create_wp=params_dict['create_wp'], sortby=params_dict['sortby'],
            dataset=params_dict['dataset'])
            a.get_wp_regions()
            a.get_data_gbq_limited()
            a.text_loop()
            return jsonify(a.results)
        elif params_dict['token'] == None:
            return jsonify("Please provide a token")
        else:
            return jsonify("Please provide a valid token")
        
        
        
