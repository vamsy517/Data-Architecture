# This is the repo for the ingest engine APIs

The NSMG data science team created several APIs to run on a GCP virtual machine. Access to the machine and the APIs is restricted.

## The logical steps within the create token (create_token.ipynb) are as follows

- Create new token for the api
- Add the new token to dataextractionapi.accesstokens

## The logical steps within the Postgre API (flask_postgre_api/postgre_api.py) are as follows

- The API is accessbile via web browser using one of the following:
 http://34.72.120.54:10000/NSMGdata?Token=<token>DatasetList=<true>(optional)&TableOrDataset=<Table> or <Dataset>&Dataset=<name of dataset>&Table=<name of table>

- The link contains the following fields, concatenated via &:
  - Token
    - the correct token is stored in a table in Postgre, called *accesstokens*, along with creation date and status of the token, i.e whether it is deprecated or not
	- in case the token in the link does not correspond to the token, stored in *accesstokens* table, the API returns an error message
  - DatasetList
    - optional parameter, which when set will force the API to return a list of all datasets
  - TableOrDataset
    - the correct values for this field are *Table* or *Dataset*
	- in case any other value is provided in the link for this field, the API returns an error message
	- in case the value for *TableOrDataset* is *Dataset*, then there is no need to provide value for *Table*
	- in case the value for *TableOrDataset* is *Table*, both *Dataset* and *Table* should have values
  - Dataset
    - in case the value provided for Dataset is not an actual dataset in Postgre, the API returns an error message
	- in case the value provided for Dataset is an actual dataset in Postgre, the API returns a list of all tables, included in this dataset
  - Table
    - in case the value provided for Table is not an actual table in the specific dataset, the API returns an error message
	- in case the value provided for Table is an actual table in the specific dataset, the API returns the specific table in JSON format


## The logical steps within the Postgre API (flaskrestplus_postgre_api/postgre_api.py) are as follows

- The API is accessbile via web browser using the following link: http://34.72.120.54:10000/
- by design it returns the datasets, which are in the following list: 
  - 'covid19data','banks', 'city', 'companies', 'metadata', 'country', 'mnc', 'bankingcompanyprofiles'

- The API expects the following parameters to be entered in the provided boxes:
  - Token
    - the correct token is stored in a table in Postgre, called *accesstokens*, along with creation date and status of the token, i.e whether it is deprecated or not
    - in case the provided token does not correspond to the token, stored in *accesstokens* table, the API returns an error message
  - DatasetList
    - optional parameter, for which when a value is provided will force the API to return a list of all datasets
  - TableOrDataset
    - the correct values for this field are *Table* or *Dataset*
	- in case any other value is provided in the box for this field, the API returns an error message
	- in case the value for *TableOrDataset* is *Dataset*, then there is no need to provide value for *Table*
	- in case the value for *TableOrDataset* is *Table*, both *Dataset* and *Table* should have values
  - Dataset
    - in case the value provided for Dataset is not an actual dataset in Postgre, the API returns an error message
	- in case the value provided for Dataset is an actual dataset in Postgre, the API returns a list of all tables, included in this dataset
  - Table
    - in case the value provided for Table is not an actual table in the specific dataset, the API returns an error message
	- in case the value provided for Table is an actual table in the specific dataset:
	  - the API returns a gz file, containing the specific table, which can be downloaded via Download File link
	  - once the file is downloaded, it needs to be unzipped and then a .csv suffix needs to be added to it in order to open the file in Excel or any other text editor.
  - JSON
    - in case the value provided for JSON is 'yes' the API returns the specific table in JSON format
	
- Each call to the API is recorded as log via salted parameters in Postgre in dataextractionapi dataset in calls_history table


## The logical steps within the Country Dynamic Templates API (country_dynamic_templates/dynamic_templates_api.py) are as follows

- The API is accessbile via web browser using the following link: http://34.72.120.54:10003/
- By design it returns a .JSON file with:
	- authorid - the WP author ID
 	- content - text content generated based on the template
 	- status - status of the post when it's created. This is related to WP - publish, future, draft, pending, private.
 	- title - the title of the created WP post (if one is created)
 
- The API expects the following parameters to be entered in the provided boxes:
 	- country - The name of the country we want to generate the data for
 	- authorid - The WP author ID that would be assigned if the post is created
 	- year_to_analyze - The year for which the data will be generated
 	- gdp_range - a range of numbers for which some data for the country's GDP will be calculated (growth rate etc)
 	- create_wp - whether to create a WP post or not
 
- No logging or authentication at the moment.

- Future features may include:
 	- Multiple values for some parameters like country
 	- More template options
 	- Option to write your own template with tags for metrics/info


## Set up FlaskRESTPlus on VM

1. Log into VM as root and add new user data_hub_api
   - sudo adduser data_hub_api
2. Add new user to sudoers file: sudo usermod -aG sudo newuser
   - command to verify: groups newuser
3. Execute sudo without password:
   - sudo visudo
   - $USER ALL=(ALL) NOPASSWD: ALL
4. Change user to data_hub_api: su - data_hub_api
5. Install Anaconda
   - go to data_hub_api home folder: cd /home/data_hub_api
   - get latest anaconda version for linux : wget -c <link_to_anaconda_version>
   - run the anaconda installer: bash <downloaded_filename>
   - exit the current shell and reopen (in order for the installation to take effect)
6. Install Anaconda libraries:
   - pandas-gbq - pip install pandas-gbq
   - Google API - pip install --upgrade google-api-python-client
7. Install FlaskRESTPlus
   - pip install flask-restplus
8. Install Psycopg2
   - pip install psycopg2
9. Install gunicorn (to use as production server)
- pip install gunicorn




### Configure Postgre API to run as service
In order to run an API as an independent and constantly available service, we need to setup a linux service. The steps below are mostly derived from [this guide](https://medium.com/@shahbaz.ali03/run-apache-airflow-as-a-service-on-ubuntu-18-04-server-b637c03f4722).

#### Run Postgre API as service

1. Log in as root
2. Create a new file postgre-api.service in system folder: touch /etc/systemd/system/postgre-api.service
3. Edit this file and add the following lines:
```
	[Unit]
	Description=Postgre server api
	After=network-online.target
	[Service]
	User = airflow_gcp
	Type=simple
	ExecStart=/bin/bash -c 'source /home/airflow_gcp/anaconda3/bin/activate && cd /home/airflow_gcp/APIs && python postgre_api.py'
	Restart=on-failure
	RestartSec=5s
	PrivateTmp=true
	[Install]
	WantedBy=multi-user.target
```
4. Change the rights of the service file:
   - chmod 664 /etc/systemd/system/postgre-api.service


#### Run FlaskRESTPlus Postgre API as service on production server

1. Log in as root
2. Create a new file postgre-flaskrestplus.service in system folder: touch /etc/systemd/system/postgre-flaskrestplus.service
3. Edit this file and add the following lines:
```
	[Unit]
	Description=Postgre FlaskRESTPlus server api
	After=network-online.target
	[Service]
	User = data_hub_api
	Type=simple
	ExecStart=/bin/bash -c 'source /home/data_hub_api/anaconda3/bin/activate && cd /home/data_hub_api/Postgre_API && && gunicorn -w 2 -b 0.0.0.0:10000 postgre_api:flask_app'
	Restart=on-failure
	RestartSec=5s
	PrivateTmp=true
	[Install]
	WantedBy=multi-user.target
```

* NOTE: Gunicorn needs only from 4 to 12 workers in order to process hundreds of thousands requests. There is such a thing as too many workers. (resources are finite)...*


4. Change the rights of the service file:
   - chmod 664 /etc/systemd/system/postgre-flaskrestplus.service



#### Enable and start the services

- systemctl enable postgre-api.service
- systemctl start postgre-api.service
- systemctl enable postgre-flaskrestplus.service
- systemctl start postgre-flaskrestplus.service


#### Check if the services are up and running

- systemctl status postgre-api.service
- systemctl status postgre-flaskrestplus.service


#### Stop the services

- systemctl stop postgre-api.service
- systemctl stop postgre-flaskrestplus.service


#### Services reload

Everytime a change is applied to any of the scripts, the following command needs to be run:

- systemctl daemon-reload


## Support

Reach out to us:

- Yavor Vrachev (Head of Data Architecture) - yavor.vrachev@ns-mediagroup.com
- Veselin Valchev (Lead of Data Science Team) - veselin.valchev@ns-mediagroup.com
- Nina Nikolaeva - nina.nikolaeva@ns-mediagroup.com
- Petyo Karatov - petyo.karatov@ns-mediagroup.com
- Dzhumle Mehmedova - dzhumle.mehmedova@ns-mediagroup.com
