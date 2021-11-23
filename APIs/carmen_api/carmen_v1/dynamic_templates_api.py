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
        'country': {'description': 'Accepted parameter values:<br/>1. Default is United Kingdom - if the field is left blank.<br/>2. Multiple countries can be selected by typing them out separated with "," and NO spaces. Example: Bulgaria,Serbia,Germany.<br/>3. Bulk option - by typing out "bulk" the API will generate outptut for all available countries in the Countries dataset in GBQ.<br/>4. A number of countries (up to 10).', 'type': 'string'},
        'gdp_range': {'description': 'Choose a gdp_range (only affects some of the data). Default is 2010,2025', 'type': 'string'}, 
        'create_wp': {'description': "Enter 'yes' to create a post with the generated information on WP. Default is 'no'", 'type': 'string'},
        #'JSON': {'description': 'Enter "yes" to get response as JSON. Leave empty to download archive with csv.<br/>NOTE: getting large tables as JSON may get the browser to freeze!', 'type': 'string'},
           }
    @app.doc(params=params)
    def get(self):
        params_dict = {
            'call_datetime': datetime.now(),
            'api_caller': request.remote_addr
        }
        for param in ['country','gdp_range','create_wp','token']: #,'JSON']:
            params_dict[param] = request.args.get(param)

        if params_dict['country'] == None:
            params_dict['country'] = ['United Kingdom']
        else:
            params_dict['country'] = list(params_dict['country'].split(','))

        if params_dict['gdp_range'] == None:
            params_dict['gdp_range'] = [2010,2025]
        else:
            params_dict['gdp_range'] = list(params_dict['gdp_range'].split(','))

        if params_dict['create_wp'] == None:
            params_dict['create_wp'] = 'no'

        if params_dict['token'] == 'shEc5B58LD0lZDka4XrNi0vlefXXpMpFGh8m3icZ7WKiEY4kiThPlrPt6lqf7pRS6QtAdBCJeWvSIUDXeoampr8sOHNF':
            a = CarmenTemplates()
            a.data_pull_gbq(country=params_dict['country'], 
                            gdp_growth_years=params_dict['gdp_range'])
            a.text_generate()
            if params_dict['create_wp']=='yes':
                a.text_publish()
            return jsonify(a.results)
        elif params_dict['token'] == None:
            return jsonify("Please provide a token")
        else:
            return jsonify("Please provide a valid token")
