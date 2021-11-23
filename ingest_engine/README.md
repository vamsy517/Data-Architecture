# This is the repo for the ingest engine of the GCP Database of NSMG

The NSMG data science team created several Airflow dag scripts to run on a GCP virtual machine. Access to the machine and dags is restricted.

## The logical steps within the ingestion dag (ingest_engine.py) are as follows

- Every 5 minutes, inspect the '/home/ingest/input' folder on the virtual machine
  - CSV file naming convention is as follows: \<dataset\>__\<table\>_\<timestamp\>.csv
  - EXCEL file naming convention is as follows: \<dataset\>_\<timestamp\>.xlsx; sheetname is the name of the table, which will to be uploaded to Bigquery
- If a new file is detected, pass it to 'GBQ_Uploader' class from the 'upload_to_gbq.py' script
- complete cycle by writing logs and adding file to '/home/ingest/logs/processed.csv' list


## The logical steps within the economics data update dag (project-pulldata_economics_data_update.py) are as follows

- Disabled at the moment and and the lists for city_ids and country_ids are hardcoded to empty lists
- Every day at 00:01, we make a call to the GD Economics API to check for updates for City and Country data 
  - the API is called with from_date parameter, equal to the date the last update check was performed
- If there are any updates for City or Country:
  - collect a list of updated entities
  - download complete data for the updated entities
  - delete all entries for all of the updated entities from all existing tables (Details and Year)
  - upload(append) the downloaded full data to the Details and Year Data tables
  - update existing metadata table
- Complete cycle by writing logs

*NOTE: Connections to databases (SQL Alchemy Engine) timeout after certain period. Be aware of long runtimes.*

## The logical steps within the economics data update dag (project-pulldata_economics_data_update_v2.py) are as follows

- Every day at 22:00, make a call to the GD Economics API to check for updates for City and Country data 
  - the API is called with from_date parameter, equal to the date the last update check was performed
  - NOTE: We trust that GD keeps consistency between Listing and Details APIs!
- Check if the token is valid and if it is not generate a new one via the API method
- If there are any updates for City or Country:
  - collect a list of updated entities
  - there is a class Updater (datahub_updater.py) which does the following:
	- download complete data for the updated entities
	- compares old and new values and in case there is a change adds current time to last_updated_date column
	- delete all entries for all of the updated entities from all existing tables (Details and Year)
	- upload(append) the downloaded full data to the Details and Year Data tables
	- update existing metadata table
- Complete cycle by writing logs

*NOTE: Connections to databases (SQL Alchemy Engine) timeout after certain period. Be aware of long runtimes.*

## The logical steps within the companies data update dag (project-pulldata_companies_data_update.py) are as follows

- Deprecated at the moment
- Every day at 21:00, make a call to the Company Integrated View API to download all companies data  
- Download complete companies data 
- Transform the downloaded data into separate dataframes
- For each dataframe:
  - delete existing table from Postgre and BigQuery
  - upload the dataframe to the corresponding table in Postgre and BigQuery
  - update existing metadata table
- Complete cycle by writing logs


## The logical steps within the economics data update dag (project-pulldata_companies_data_update_v2.py) are as follows

- Every day at 00:00, make a call to the Company Integrated View API to download all companies data 
  - Download complete companies data 
  - Transform the downloaded data into separate dataframes
  - there is a class Updater (datahub_updater.py) which does the following:
	- compares old and new values and in case there is a change adds current time to last_updated_date column
	- delete all entries for all entities from all existing tables
	- upload(append) the downloaded full data to the existing tables
	- update existing metadata table
- Complete cycle by writing logs

## The logical steps within the mnc data update dag (project-pulldata_mnc_data_update.py) are as follows

- Every day at 21:00, make a call to the NSMG API to download all MNC Segments and Subsidiaries data 
  - Download complete companies data 
  - Transform the downloaded data into separate dataframes
  - there is a class Updater (datahub_updater.py) which does the following:
	- compares old and new values and in case there is a change adds current time to last_updated_date column
	- delete all entries for all entities from all existing tables
	- upload(append) the downloaded full data to the existing tables
	- update existing metadata table
- Complete cycle by writing logs

## The logical steps within the alchemer data update dag (project-pulldata_alchemer_data_update.py) are as follows

- Every day at 02:00, make a call to the Alchemer API to download all surveys data  
- Download complete surveys data 
- Transform the downloaded data into separate dataframes
- For each dataframe:
  - delete existing table from Postgre and BigQuery
  - upload the dataframe to the corresponding table in Postgre and BigQuery
  - update existing metadata table
- Complete cycle by writing logs

## The logical steps within the Pardot.Lists data update dag (project-pulldata_pardot_lists_update.py) are as follows

- Every day at 19:00 server time, make a call to the Pardot Lists API to download new created and new updated data 
- Concatenate data for new create and new updated to single dataframe
-fix data types
- Use our datahub_updater class to upload the new dataframe to the database
- Complete cycle by writing logs

## The logical steps within the PardotB2C.Lists data update dag (project-pulldata_pardot_lists_update_B2C.py) are as follows

- Every day at 19:00 server time, make a call to the Pardot Lists API to download new created and new updated data 
- Concatenate data for new create and new updated to single dataframe
-fix data types
- Use our datahub_updater class to upload the new dataframe to the database
- Complete cycle by writing logs

## The logical steps within the Pardot.Lists_Memberships data update dag (project-pulldata_pardot_lists_memberships_update.py) are as follows

- Every day at 23:00 server time, make a call to the Pardot Lists Memberships API to download new created and new updated data 
- Concatenate data for new create and new updated to single dataframe
-fix data types
- Use our datahub_updater class to upload the new dataframe to the database
- Complete cycle by writing logs

## The logical steps within the PardotB2C.Lists_Memberships data update dag (project-pulldata_pardot_lists_memberships_update_B2C.py) are as follows

- Every day at 23:00 server time, make a call to the Pardot Lists Memberships API to download new created and new updated data 
- Concatenate data for new create and new updated to single dataframe
-fix data types
- Use our datahub_updater class to upload the new dataframe to the database
- Complete cycle by writing logs

## The logical steps within the Pardot.Email and Pardot.EmailsClicks data update dag (project-pulldata_pardot_emails_update.py) are as follows

- Every day at 20:00 server time, make a call to the Pardot Emails and EmailsClicks API to download new created
- First download new created EmailsClicks and then iterate by email id for emails metric details
-fix data types
- Use our datahub_updater class to upload the new dataframe to the database
- Complete cycle by writing logs

## The logical steps within the PardotB2C.Email and PardotB2C.EmailsClicks data update dag (project-pulldata_pardot_emails_update_B2C.py) are as follows

- Every day at 20:00 server time, make a call to the Pardot Emails and EmailsClicks API to download new created
- First download new created EmailsClicks and then iterate by email id for emails metric details
-fix data types
- Use our datahub_updater class to upload the new dataframe to the database
- Complete cycle by writing logs


## The logical steps within the source control dag (ingest_engine_github.py) are as follows
 
 - fetch the master branch
 - check for any updates
 - if there are any new commits, pull the new commits into VM Github Repo
 - replace the old .py files with the new ones
 - check if there are any changes in the API folder
   - if yes replace the files and restart the API service
 
 
 ## The logical steps within the health check dag (ingest_engine_health_check.py) are as follows
 
 - get all logs, which end with 'processing_results.csv' from /home/ingest/logs/
 - get a list of all new successfully uploaded and/or updated tables in GBQ and Postgre since the last performed health check
 - for each of these tables, perform a health check
   - create a summary table with dataset, tablename, column name, column type, sample values, coverage, Z-Score (only for numeric columns)
   - upload the summary table to MetaData.HealthCheck
 - add current day to the '/home/ingest/logs/health_check_log.csv' as the last healthcheck date
 
 ## The logical steps within the test api dag (test_data_hub_api.py) are as follows

- Every day at 00:01, make a call to the Postgre API with token, dataset = city and table =  CityEconomicsDetails 
  - click the download button
  - check the shape of the table and the size of the downloaded file
  - report the findings in '/home/ingest/logs/api_test_log.csv'
  
  
 ## The logical steps within the cleanup dag (ingest_cleanup_api_temp_folder.py) are as follows

- Every day at 01:00 delete the temp files, generated by Postgre API calls, located in '/home/data_hub_api/Postgre_API/result_files/'
  - report the findings in '/home/ingest/logs/cleanup_log.csv'
  
  In order for the dag to complete successfully two users (data_hub_api and airflow_gcp) need to have write access to /home/data_hub_api/Postgre_API/result_files/.
  
  
 ### Steps to allow airflow_gcp and data_hub_api users to have write access to '/home/data_hub_api/Postgre_API/result_files/'
 
- Create new group, which will include the affected users: 
  - sudo groupadd postgre_api
- Add the affected users to the group
  - sudo adduser airflow_gcp postgre_api
  - sudo adduser data_hub_api postgre_api
- list the users in the group
  - grep postgre_api /etc/group
- Make the group the owner of the specific folder
  - chgrp -R postgre_api /home/data_hub_api/Postgre_API/result_files/ 
- check folder permissions
  - ls -ld 
  
  
 ## The logical steps within the emailing dag (ingest_email.py) are as follows

- Every day at 05:00, inspect the following log files on the Virtual machine:
  - '/home/ingest/logs/processing_results.csv'
  - '/home/ingest/logs/update_processing_results.csv' 
  - '/home/ingest/logs/api_test_log.csv'
  - '/home/ingest/logs/cleanup_log.csv'
  - '/home/ingest/logs/update_covid19_processing_results.csv'
  - '/home/ingest/logs/update_companies_processing_results.csv'
- If any data has been uploaded or updated since the last email, there should be a record of that in these files; add the record to a temp dataframe and insert it into an email
- add to the email content also the findings from the executions of the api test dag, cleanup dag, Covid19 dag and companies update dag
- Email the content to Nina, Yavor, Petyo, Veselin and Dzhumle (emails below)
- Add current day to the '/home/ingest/logs/email_log.csv' as the last email date


### Steps for linking GIT on VM to GitHub

- Log in to VM
- Generate SSH key: sudo ssh-keygen
- Create SSH config: sudo cat > /root/.ssh/config
- Add the following line to the config: IdentityFile ~/.ssh/<key>
- Add public key to GitHub Repo (you have to be owner in order to do it):
  - go to repo>Settings>Deploy Keys
  - paste <key>.pub file
- Clone Repo in VM
  - go to /home dir on VM
  - clone repo: git clone <repo>
- Change ownership of repo folder to the new airflow user on linux
  - make sure other users have at most read permissions for files within

- Every 5 minutes, inspect the master branch of the 'NS-Media-Group/NSMG-DataArchitecture' repo (GitHub project has SSH public key from VM deployed)
- If no changes to master, pass with print
- Else, pull master and copy (and replace) all .py files from the '/home/NSMG-DataArchitecture/ingest_engine' folder to the '/home/vvalchev/airflow/dags' folder

Airflow scheduler must be run on the VM for the pipelines to be active. An Airflow webserver must be run on the VM for monitoring and control over dags.

The two notebooks relate to getting the Cities & Countries data from the Global Data API. The two notebooks get the required data and save it to CSV. These CSVs were then moved to the ingest/input folder for ingestion by the Airflow pipeline.


### Setup airflow on VM

1. Login as root on the virtual machine
2. Create new user airflow: useradd -m -d /home/airflow airflow 
3. Set password: passwd airflow
4. Add new user to sudoers file: sudo usermod -aG sudo newuser
   - command to verify: groups newuser
5. Execute sudo withou password:
   - sudo visudo
   - $USER ALL=(ALL) NOPASSWD: ALL
6. Change user to airflow: su - airflow
7. Install Anaconda
   - go to airflow home folder: cd airflow/
   - get latest anaconda version for linux : wget -c \<link_to_anaconda_version\>
   - run the anaconda installer: bash \<downloaded_filename\>
   - exit the current shell and reopen (in order for the installation to take effect)
8. Install Anaconda libraries:
   - pandas-gbq - pip install pandas-gbq
   - Google API - pip install --upgrade google-api-python-client
9. Install Airflow
   - pip install apache-airflow
   - airflow initdb
   - Create dags folder in /home/airflow/airflow: mkdir dags
10. Set /home/airflow/airflow/airflow.cfg
    - check whether executor = SequentialExecutor
    - set email configuration:
      - smtp_host = smtp.gmail.com
      - smtp_user = nsmgbulgaria@gmail.com
      - smtp_password = \<password\>
      - smtp_port = 587
      - smtp_mail_from = nsmgbulgaria@gmail.com
11. Copy all current dags from /home/vvalchev/airflow/dags to home/airflow/aiflow/dags:
    - go to /home/vvalchev/airflow/dags
    - cp \<filename\> home/airflow/aiflow/dags
12. *The owner of /home/ingest folder is airflow_gcp user and only this user can make changes to the contents of this folder*
13. Start airflow scheduler from /home/airflow/airflow
    - aiflow scheduler
14. Start airflow webserver from /home/airflow/airflow from another shell:
    - airflow webserver

### Setup PostgreSQL on VM
Airflow needs a dedicated database to track, monitor and log dags. By default, Airflow comes with an SQLite database. We need to setup a separate db - we choose PostgreSQL. The following steps are derived mostly from [this guide](https://medium.com/@taufiq_ibrahim/apache-airflow-installation-on-ubuntu-ddc087482c14).
1. Install dependencies:
   - sudo apt-get install postgresql postgresql-contrib
   - sudo apt-get install postgresql
   - sudo apt-get install python-psycopg2
   - sudo apt-get install libpq-dev
2. Create a postgres user for the airflow linux user \(with the same name\):
   - sudo -u postgres psql
   - CREATE USER \<linux_user_for_airflow\> PASSWORD '\<placeholder_pass\>';
   - CREATE DATABASE airflow_db;
   - GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \<linux_user_for_airflow\>;
   - \du # to check roles
3. Exit postgres console, and check that the database is created and the airflow linux user has access:
   - su - \<linux_user_for_airflow\> #log as the airflow user on linux
   - psql -d airflow_db
4. Change postgres configs:
   - sudo nano /etc/postgresql/9.5/main/pg_hba.conf
     - Change IPV4 address to '0.0.0.0/0' and the IPV4 method to 'trust'
	 - sudo service postgresql restart
   - sudo nano /etc/postgresql/9.5/main/postgresql.conf
     - Change listen_addresses to '\*'
	 - sudo service postgresql restart
5. Change airflow config:
   - nano /home/airflow_gcp/airflow/airflow.cfg
     - Change executor to 'LocalExecutor'
	 - Change sql_alchemy_conn to 'postgresql+psycopg2://\<linux_user_for_airflow\>@localhost:5432/airflow_db'
	 - airflow initdb
6. After airflow is running, check that the new database is being filled with data:
   - log in as \<linux_user_for_airflow\>
   - psql -d airflow_db
   - \dt # you should see all relevant tables - dags, logs, etc.
   - SELECT \* FROM log ORDER BY dttm DESC; # you should see the most recent log events

### Configure Apache-Airflow to run as service
In order to run Airflow as an independent and constantly available service, we need to setup a linux service. The steps below are mostly derived from [this guide](https://medium.com/@shahbaz.ali03/run-apache-airflow-as-a-service-on-ubuntu-18-04-server-b637c03f4722).

#### Run airflow webserver as service

1. Log in as root
2. Create a new file airflow-webserver.service in system folder: touch /etc/systemd/system/airflow-webserver.service
3. Edit this file and add the following lines:
```
	[Unit]
	Description=Airflow webserver daemon
	After=network-online.target postgresql.service
	[Service]
	User = airflow_gcp
	Type=simple
	ExecStart=/bin/bash -c 'source /home/airflow_gcp/anaconda3/bin/activate && cd /home/airflow_gcp/airflow && airflow webserver'
	Restart=on-failure
	RestartSec=5s
	PrivateTmp=true
	[Install]
	WantedBy=multi-user.target
```
4. Change the rights of the service file:
   - chmod 664 /etc/systemd/system/airflow-webserver.service


#### Run airflow scheduler as service

1. Log in as root
2. Create a new file airflow-scheduler.service in system folder: touch /etc/systemd/system/airflow-scheduler.service
3. Edit this file and add the following lines:
```
	[Unit]
	Description=Airflow scheduler daemon
	After=network-online.target postgresql.service
	[Service]
	User = airflow_gcp
	Type=simple
	ExecStart=/bin/bash -c 'source /home/airflow_gcp/anaconda3/bin/activate && cd /home/airflow_gcp/airflow && airflow scheduler'
	Restart=always
	RestartSec=5s
	[Install]
	WantedBy=multi-user.target
```
4. Change the rights of the service file:
   - chmod 664 /etc/systemd/system/airflow-scheduler.service


#### Enable and start the services

- systemctl enable airflow-scheduler.service
- systemctl start airflow-scheduler.service
- systemctl enable airflow-webserver.service
- systemctl start airflow-webserver.service

#### Check if the services are up and running

- systemctl status airflow-scheduler.service
- systemctl status airflow-webserver.service

#### Stop the services

- systemctl stop airflow-scheduler.service
- systemctl stop airflow-webserver.service

#### Services reload

Everytime a change is applied to any of the scripts, the following command needs to be run:

- systemctl daemon-reload


### Setup GCP service account for Airflow

1. Go to Admin Console for the project in GCP
2. Go to Service Accounts
3. Create a new service account: airflow-ingest-engine
4. Add the following roles:
   - BigQuery User
   - BigQuery Data Editor
   - BigQuery Job User
   - BigQuery Metadata Viewer
5. Create a new key - JSON (the key is downloaded automatically)
6. Rename the file: gbq_credentials.json
7. Log in to the VM
8. Switch user to airflow_gcp
9. Upload the downloaded key to /home/ingest/credentials
10. Change the permissions to the key to be read, written and executed only by airflow_gcp : chmod 700 gbq_credentials.json

### Setup Putty connection to GCP VM

1. Download and install Putty
2. With Puttygen utility (comes with Putty), generate a public-private key pair
   - generate with user and pass to be used as linux credentials
   - save public and private key files to secure location on local machine
3. Open the GCP console for the required project (e.g. project-pulldata)
4. Navigate to and edit the required virtual machine for that project (e.g. airflow-1)
   - click 'show and edit' for 'SSH Keys'
   - click 'add item' and paste the public key generated in step 2
5. Open Putty and set the following parameters:
   - host name: \<username_for_ssh_pair\>@\<vm_external_ip_address\>
   - port: 22
   - connection >> SSH >> auth >> private key file for authentication: \<private_key_location_on_local_machine\>
   - connection >> SSH >> Tunnels:
     - add source port: 8088; destination: localhost:8080 # for Airflow - airflow service is constantly running on port 8088 on VM
     - add source port: \<port_hosting_Jupyter\>; destination: localhost:8888 # for Jupyter; select port to host Jupyter on VM at Jupyter start - make sure that VM port is free and other team members do not use it for their own Jupyter
10. Check connections:
    - Open a Putty connection with above parameters and login with credentials
    - once connection is established, on local browser open address "http://localhost:8088/admin/"
    - if Airflow console is displayed, connection and tunnelling work fine

## Support

Reach out to us:

- Yavor Vrachev (Head of Data Architecture) - yavor.vrachev@ns-mediagroup.com
- Veselin Valchev (Lead of Data Science) - veselin.valchev@ns-mediagroup.com
- Nina Nikolaeva - nina.nikolaeva@ns-mediagroup.com
- Petyo Karatov - petyo.karatov@ns-mediagroup.com
- Dzhumle Mehmedova - dzhumle.mehmedova@ns-mediagroup.com
