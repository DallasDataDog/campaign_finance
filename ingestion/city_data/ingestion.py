from pathlib import Path
import sys
import yaml
import requests
import pandas as pd
import io
import csv

# Get the current file's directory
current_dir = Path(__file__).parent
parent_dir=current_dir.parent
sys.path.append(str(parent_dir))
import snowflake_auth as sf

#Reading YAML authentication file that contains API Key value
with open('authentication.yaml', 'r') as file:
    config = yaml.safe_load(file)

#Section related to calling the Dallas Open Data API for camapaign finance data. The API key is retireved from the authentication.yaml file
payload = {}
headers = {config['dallas_open_data']['api_key']: 'X-App-Token'} #You can request an API key on the dallas open data website

#API call to retrieve campaign finance records
url = "https://www.dallasopendata.com/resource/ndxz-gccx.csv?$select=id,record_id,report_id,file_link,first_name,last_name,business_name,contact_type,record_type,amount,schedule_type,candidate_name,election_date,transaction_date,street,city,state,zipcode,geo_location.latitude,geo_location.longitude,:created_at,:updated_at&$order=transaction_date&$limit=49000"
response_data = requests.request("GET", url, headers=headers, data=payload)

#generating csv file, with max update date in its name
df_camp_fin_records = pd.read_csv(io.StringIO(response_data.text))
update_date = pd.to_datetime(df_camp_fin_records[':updated_at']).dt.strftime('%Y-%m-%d').max()
output_file_name = "dallas_campaign_finance_{0}.csv".format(update_date)
df_camp_fin_records.to_csv(current_dir / 'seed/' / output_file_name, quoting=csv.QUOTE_ALL ,index=False)

#closing request objects
response_data.close()

#Section for loading data into Snowflake RAW Layer
#Create a new Snowflake connection. sf.sf_auth('SCHEMA NAME'). Uses the local module, snowflake_auth.py.
conn = sf.sf_auth('COD_CAMPAIGN_FINANCE')

#Read file that contains internal stage ddl
with open(current_dir / 'stages/ddl_raw_files.sql','r') as script:
    stages_ddl=io.StringIO(script.read())
#Execute DDL for internal stage creation
for statement in conn.execute_stream(stages_ddl):
    for result in statement:
        print(result)


#read file that contains file format ddl
with open(current_dir / 'file_format/ddl_csv_files.sql','r') as script:
    file_format_ddl=io.StringIO(script.read())
#execute DDL for file format creation
for statement in conn.execute_stream(file_format_ddl):
    for result in statement:
        print(result)


#Uploading file(s) to stage
sf.sf_upload_file_to_stage(conn, current_dir / 'seed/', output_file_name, 'RAW_FILES')


#generate script for create table and copy into
with open(current_dir / 'tables/all_report_records.sql') as script:
    table_ddl_and_copy=io.StringIO(script.read())
#execute create table and copy into
for statement in conn.execute_stream(table_ddl_and_copy):
    for result in statement:
        print(result)

conn.close()



#Section for pagination of API, not used
# limit=10000 #number of rows brought back by each api call
# offset=0 #the number of rows by which to move each subsequent api
# has_data = True 
# while has_data:
#     url = "https://www.dallasopendata.com/resource/ndxz-gccx.csv?$select=id,record_id,report_id,file_link,first_name,last_name,business_name,contact_type,record_type,amount,schedule_type,candidate_name,election_date,transaction_date,street,city,state,zipcode,geo_location.latitude,geo_location.longitude,:created_at,:updated_at&$order=transaction_date&$limit={0}&$offset={1}".format(limit,offset)
#     payload = {}
#     headers = {'': 'X-App-Token'}
#     response = requests.request("GET", url, headers=headers, data=payload)
#     string=""
#     if offset == 0:
#         string_list=response.text.split('\n')[0:limit+1]
#     else:
#         string_list=response.text.split('\n')[1:limit+1]
#     for k in string_list:
#         string += k +'\n'
#     if len(string_list) > 1:
#         output_file = open("dallas_campaign_finance_date_paging.csv", "a")
#         output_file.write(string)
#         offset+=limit
#     else:
#         has_data = False
#     output_file.close()
#     response.close()