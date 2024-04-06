from pathlib import Path
import sys
import pandas as pd
import io
import os

# Get the current file's directory
current_dir = Path(__file__).parent
parent_dir=current_dir.parent
sys.path.append(str(parent_dir))
import snowflake_auth as sf


#Create a new Snowflake connection. sf.sf_auth('SCHEMA NAME'). Uses the local module, snowflake_auth.py.
conn = sf.sf_auth('TEC_CAMPAIGN_FINANCE')


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
for k in os.listdir(current_dir / 'seed/'):
    sf.sf_upload_file_to_stage(conn, current_dir / 'seed/', k, 'RAW_FILES')

#sf.sf_upload_file_to_stage(conn, current_dir / 'seed/', 'cover.csv', 'RAW_FILES')

#generate script for create table and copy into
with open(current_dir / 'tables/tec_data_tables.sql') as script:
    table_ddl_and_copy = io.StringIO(script.read())
#execute create table and copy into
for statement in conn.execute_stream(table_ddl_and_copy):
    for result in statement:
        print(result)


conn.close()