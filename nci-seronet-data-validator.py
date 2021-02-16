import boto3
import json
import urllib3

import pandas as pd
import re
from datetime import datetime
import dateutil.tz
import mysql.connector
import pathlib

import file_validator_object
from prior_test_result_validator import prior_test_result_validator
from demographic_data_validator  import demographic_data_validator
from Biospecimen_validator       import Biospecimen_validator
#from other_files_validator       import other_files_validator

DB_MODE = "DB_Mode"
TEST_MODE = "Test_Mode"
#####################################################################
def lambda_handler(event, context):
    temp_file_loc = '/tmp'
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource("s3")
    ssm = boto3.client("ssm")
    
    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    jobs_dbname = ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
    file_dbname = "seronetdb-Validated"
#############################################################################################################
# set up success and failure slack channel
    failure = ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")
    success = ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")
    http = urllib3.PoolManager()
#############################################################################################################
## if no submission errors, pull key peices from sql schema and import cbc id file
    jobs_conn,j_status_message   = connect_to_sql_database(jobs_dbname,host_client,user_name,user_password)
    filedb_conn,f_status_message = connect_to_sql_database(file_dbname,host_client,user_name,user_password)

    if (j_status_message == "Connection Failed") or (f_status_message == "Connection Failed"):
        print("Unable to Connect to MYSQL Database")
        print("Terminating File Validation Process")
        close_connections(jobs_conn)
        close_connections(filedb_conn)
        return{}
    del j_status_message,f_status_message
    try:
#############################################################################################################
## get list of IDS for SARS_CoV-2 Positve and Negative Participants, also get all Biospecimen IDS
        prior_cov_test = get_mysql_queries(filedb_conn,1)
        demo_ids       = get_mysql_queries(filedb_conn,2)
        biospec_ids    = get_mysql_queries(filedb_conn,3)
    
        key_index_dict = {"prior_clinical_test.csv":['Research_Participant_ID'],
                          "demographic.csv":['Research_Participant_ID'],
                          "biospecimen.csv":['Biospecimen_ID'],
                          "aliquot.csv":['Aliquot_ID','Biospecimen_ID'],
                          "equipment.csv":['Equipment_ID','Biospecimen_ID'],
                          "reagent.csv":['Biospecimen_ID','Reagent_Name'],
                          "consumable.csv":['Biospecimen_ID','Consumable_Name']}
#############################################################################################################
        Validation_Type = DB_MODE
        if 'TEST_MODE' in event:
            if event['TEST_MODE']=="On":         #if testMode is off treats as DB mode with manual trigger
                Validation_Type = TEST_MODE
#############################################################################################################
## Query the jobs table database and get list of zip files that passed File-Validation
        MY_SQL = "SELECT * FROM table_submission_validator where  batch_validation_status = %s"
        successful_submissions = pd.read_sql(MY_SQL, con=jobs_conn, params=['Batch_Validation_SUCCESS'])
        successful_submissions_ids = successful_submissions['submission_file_id']
        for iterS in successful_submissions_ids:
            MY_SQL = ("SELECT * FROM table_file_validator where submission_file_id = %s and file_validation_status = %s")
            files_to_check = pd.read_sql(MY_SQL, con=jobs_conn, params=[iterS,'FILE_VALIDATION_IN_PROGRESS'])
            file_names = [pathlib.PurePath(i).name for i in files_to_check['file_validation_file_location']]
##########################################################################################################################################            
            if 'submission.csv' not in file_names:
                data = {'CBC_ID': ['00'],'CBC_Name': ['Unknown'], "Submitted_Participants":[0],"Submited_Biospecimens":[0]}
                submitting_center = pd.DataFrame(data, columns = ['CBC_ID','CBC_Name',"Submitted_ Participants", "Submited_Biospecimens"])
            else:
                submitting_center = get_submission_metadata(s3_client,temp_file_loc,files_to_check,file_names,filedb_conn)
        
            all_file_objects = []
            for current_file in file_names:
                if current_file in ['submission.csv']:
                    all_file_objects.append((current_file,[]))
                else:
                    current_object = get_all_file_objects(pd,s3_client,key_index_dict,current_file,files_to_check,file_names)    
                    all_file_objects.append((current_file,current_object))
                if current_file in ["prior_clinical_test.csv"]:
                    prior_cov_test = pd.DataFrame(current_object.Data_Table[["Research_Participant_ID","SARS_CoV_2_PCR_Test_Result"]])
                if current_file in ['demographic.csv']:
                     demo_ids = pd.DataFrame(current_object.Data_Table[["Research_Participant_ID","Age"]])
                if current_file in ['biospecimen.csv']:
                    biospec_ids = pd.DataFrame(current_object.Data_Table[["Biospecimen_ID","Biospecimen_Type"]])
##########################################################################################################################################
            error_results = []
            cross_valid_error = [['Message_Type','CSV_Sheet_Name_1','CSV_Sheet_Name_1','ID_Value','Error_message']] 
            if "prior_clinical_test.csv" in file_names:
                current_object = all_file_objects[file_names.index("prior_clinical_test.csv")][1]
                current_object = prior_test_result_validator(current_object,demo_ids,re,submitting_center['CBC_ID'][0],pd)
                error_results = current_object.write_error_file("Prior_Clinical_Test_Errors.csv",s3_resource,temp_file_loc,error_results,"Prior_Clinical_Test_Errors" )
##########################################################################################################################################
    except Exception as e:                          #if there are any errors, display and move to finally block
        print(e)
        display_error_line(e)
        print("Terminating Validation Process")
    finally:                                        #close all the sql connections if they exist
        print("Connection to RDS mysql instance is now closed")
        close_connections(jobs_conn)
        close_connections(filedb_conn)
        return{}
##########################################################################################################################################
def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({
            "filename": tb.tb_frame.f_code.co_filename,
            "name": tb.tb_frame.f_code.co_name,
            "lineno": tb.tb_lineno
        })
        tb = tb.tb_next
    print(str({
        'type': type(ex).__name__,
        'message': str(ex),
        'trace': trace
    }))
#####################################################################
def connect_to_sql_database(file_dbname,host_client,user_name,user_password):
    status_message = "Connected"
    conn = []
    try:
        conn = mysql.connector.connect(host = host_client, user=user_name, password=user_password, db=file_dbname, connect_timeout=5)
        print("SUCCESS: Connection to RDS mysql instance succeeded\n")
    except:
        status_message = "Connection Failed"
    return conn,status_message
##########################################################################################################################
def get_mysql_queries(conn,index):
    if index == 1:  ## pulls positive and negative participant ids from database for validation purposes
        sql_querry = ("SELECT %s,%s FROM `Participant_Prior_Test_Result` Where Test_Name = %s;")
        prior_test = pd.read_sql(sql_querry, conn,params=["Research_Participant_ID","Test_Result","SARS_Cov_2_PCR"])
        return prior_test
    elif index == 2:
        sql_querry = "SELECT %s,%s FROM Participant;"
        bio_ids = pd.read_sql(sql_querry, conn, params=["Research_Participant_ID","Age"])
        return bio_ids
    elif index == 3:
        sql_querry = "SELECT %s,%s FROM Biospecimen;"
        bio_ids = pd.read_sql(sql_querry, conn, params=["Biospecimen_ID","Biospecimen_Type"])
        return bio_ids
##########################################################################################################################
def close_connections(conn):
    if type(conn) == mysql.connector.connection.MySQLConnection:
        conn.close()
def get_bucket_and_key(files_to_check,file_names,current_file):
    current_metadata = files_to_check.iloc[file_names.index(current_file)]
    full_bucket_name = current_metadata[current_metadata.index == 'file_validation_file_location'][0]

    first_folder_cut = full_bucket_name.find('/')            
    org_key_name = full_bucket_name[(first_folder_cut+1):]
    bucket_name = full_bucket_name[:(first_folder_cut)]
    return bucket_name,org_key_name
def get_submission_metadata(s3_client,temp_file_loc,files_to_check,file_names,conn):
    bucket_name,org_key_name = get_bucket_and_key(files_to_check,file_names,"submission.csv")

    temp_file_loc = temp_file_loc + '/test_file.csv'
    s3_client.download_file(bucket_name,org_key_name,temp_file_loc)
    Data_Table = pd.read_csv(temp_file_loc,encoding='utf-8',na_filter = False)

    MY_SQL = ("SELECT * FROM CBC Where CBC_Name = %s")
    submitting_center = pd.read_sql(MY_SQL, con = conn, params=[Data_Table.columns.values[1]])
    submitting_center["Submitted_ Participants"] = Data_Table.iloc[1][1]
    submitting_center["Submited_Biospecimens"] = Data_Table.iloc[2][1]
    
    return submitting_center
def get_all_file_objects(pd,s3_client,key_index_dict,current_file,files_to_check,file_names):
    try:
        current_object = file_validator_object_v2.Submitted_file(current_file,key_index_dict[current_file])
        bucket_name,org_key_name = get_bucket_and_key(files_to_check,file_names,current_file)
        current_object.File_Bucket = bucket_name
        current_object.Error_dest_key = org_key_name.replace('UnZipped_Files/'+current_file,'Data_Validation_Results')

        current_object.load_csv_file(s3_client,bucket_name,org_key_name,pd)
    except s3_client.exceptions.NoSuchKey:
        current_object = [] 
    return current_object
def write_message_to_slack(http,error_results,current_submission,slack_success,slack_failure):
    eastern = dateutil.tz.gettz('US/Eastern')                                   #converts time into Eastern time zone (def is UTC)
    validation_date = datetime.now(tz=eastern).strftime("%Y-%m-%d %H:%M:%S")
    
    submision_string = current_submission['submission_validation_file_location'].tolist()[0]
    slash_index = [m.start() for m in re.finditer('/',submision_string)]
    file_submitted_by = submision_string[(slash_index[0]+1):(slash_index[1])]
    
    file_name = pathlib.PurePath(submision_string).name
    org_file_id = current_submission['orig_file_id'].tolist()[0]
    
    
    clean_files = [i[0] + ": " + str(i[1]) for i in error_results if i[1] == 0]
    error_files = [i[0] + ": " + str(i[1]) for i in error_results if i[1] >  0]
    #get the files that pass the validation
    passString = 'NA'
    #get the files that do not pass the validation
    failString = 'NA'
    if(len(clean_files)>0):
        passString = ', '.join(clean_files)
    if(len(error_files)>0):
        failString = ', '.join(error_files)
        
    total_errors = sum([i[1] for i in error_results])
    if total_errors == 0:
        slack_channel = slack_success
    else:
        slack_channel = slack_failure
        
    message_slack=f"{file_name}(Job ID: {org_file_id} CBC ID: {file_submitted_by})\nValidation pass: (_{passString}_) \n *Validation fail:* (*{failString}*) File validated on {validation_date}"
    data={"type": "mrkdwn","text": message_slack}
    http.request("POST",slack_channel,body=json.dumps(data), headers={"Content-Type":"application/json"})