from pathlib import Path
from dotenv import load_dotenv
import logging
import sys
import requests
import pandas as pd
import io
import csv
import os

load_dotenv(override=True) #import values from .env config file
current_dir = Path(__file__).parent # Save the current file's directory
sys.path.append(os.environ.get('SNOWFLAKE_ADMIN_PATH'))
import snowflake_utils as sf

snowflake_config = {
    "account_name" : os.environ.get('SNOWFLAKE_ACCOUNT'),
    "user_name" : os.environ.get('SNOWFLAKE_USER'),
    "role_name" : os.environ.get('SNOWFLAKE_ROLE'),
    "private_key_path" : os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH')
}

#setup logging
logging.basicConfig(format='[%(asctime)s] %(levelname)s %(message)s')
logger = logging.getLogger('Camp Fin Ingestion')
logger.setLevel(logging.INFO)

try:
    #Section related to calling the Dallas Open Data API for campaign finance data. The API key is retireved from the authentication.yaml file
    payload = {}
    headers = {os.environ.get('DALLAS_OPEN_DATA_API_KEY'): 'X-App-Token'} #You can request an API key on the dallas open data website
    url = "https://www.dallasopendata.com/resource/ndxz-gccx.csv?$select=id,record_id,report_id,file_link,first_name,last_name,business_name,contact_type,record_type,amount,schedule_type,candidate_name,election_date,transaction_date,street,city,state,zipcode,geo_location.latitude,geo_location.longitude,:created_at,:updated_at&$order=transaction_date&$limit=49000" #API call to retrieve campaign finance records
    response_data = requests.request("GET", url, headers=headers, data=payload)
    logger.info("Retrieved records from API")
except Exception as e:
    logger.error(e)
    sys.exit(1)

#generating csv file, with max update date in its name
df_camp_fin_records = pd.read_csv(io.StringIO(response_data.text))
update_date = pd.to_datetime(df_camp_fin_records[':updated_at']).dt.strftime('%Y-%m-%d').max()
output_file_name = "dallas_campaign_finance_{0}.csv".format(update_date)
df_camp_fin_records.to_csv(current_dir / 'seed/' / output_file_name, quoting=csv.QUOTE_ALL ,index=False)
logger.info(f"Created csv export with {len(df_camp_fin_records)} records")

#closing request objects
response_data.close()

#Section for loading data into Snowflake RAW Layer
with sf.sf_conn(snowflake_config) as conn:
    with open(current_dir / 'stages/ddl_raw_files.sql','r') as script:     #Read file that contains internal stage ddl
        stages_ddl=io.StringIO(script.read())
    for statement in conn.execute_stream(stages_ddl):     #Execute DDL for internal stage creation
        for result in statement:
            logger.info(result)

    #read file that contains file format ddl
    with open(current_dir / 'file_format/ddl_csv_files.sql','r') as script:
        file_format_ddl=io.StringIO(script.read())
    #execute DDL for file format creation
    for statement in conn.execute_stream(file_format_ddl):
        for result in statement:
            logger.info(result)

    #Uploading file(s) to stage
    sf.file_to_stage(conn, current_dir / 'seed/', output_file_name, 'RAW.COD_CAMPAIGN_FINANCE.RAW_FILES')

    #generate script for create table and copy into
    with open(current_dir / 'tables/all_report_records.sql') as script:
        table_ddl_and_copy=io.StringIO(script.read())
    #execute create table and copy into
    for statement in conn.execute_stream(table_ddl_and_copy):
        for result in statement:
            logger.info(result)



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