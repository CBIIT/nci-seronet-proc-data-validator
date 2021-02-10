import boto3
import json
import urllib3

import pandas as pd
import re
import csv
from datetime import datetime
import dateutil.tz
import mysql.connector
import pathlib

import file_validator_object
from prior_test_result_validator import prior_test_result_validator
from demographic_data_validator  import demographic_data_validator
from Biospecimen_validator       import Biospecimen_validator
from other_files_validator       import other_files_validator

DB_MODE = "DB_Mode"
TEST_MODE = "Test_Mode"
#####################################################################
def lambda_handler(event, context):
    temp_file_loc = '/tmp'
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource("s3")
    ssm = boto3.client("ssm")
    sns = boto3.client("sns")
    
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
        pos_list,neg_list = file_validator_object.get_mysql_queries(file_dbname,filedb_conn,1,pd)
        biospec_ids = file_validator_object.get_mysql_queries(file_dbname,filedb_conn,2,pd)
        assay_ids = file_validator_object.get_mysql_queries(file_dbname,filedb_conn,3,pd)
        valid_particiant_ids = pos_list + neg_list
    
        key_index_dict = {"prior_clinical_test.csv":'Research_Participant_ID',
                          "demographic.csv":'Research_Participant_ID',
                          "biospecimen.csv":'Biospecimen_ID',
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
## For each file that Passes, get a list of the ziped files that also passed file-validation
        for iterS in successful_submissions_ids:
            MY_SQL = ("SELECT * FROM table_file_validator where submission_file_id = %s and file_validation_status = %s")
            files_to_check = pd.read_sql(MY_SQL, con=jobs_conn, params=[iterS,'FILE_VALIDATION_SUCCESS'])
            file_names = [pathlib.PurePath(i).name for i in files_to_check['file_validation_file_location']]
            
            if 'submission.csv' not in file_names:          #code causes errors if this file does not exist
                continue                                    #if submission was successful then it has to exist
    
            submitting_center,part_count,bio_count = get_submission_metadata(s3_client,temp_file_loc,files_to_check,file_names,file_dbname,jobs_conn)
            all_file_objects = []
            for current_file in file_names:
                if current_file in ['submission.csv']:
                    all_file_objects.append((current_file,[]))
                else:
                    current_object = file_validator_object.Submitted_file(current_file,key_index_dict[current_file])
                    bucket_name,org_key_name = get_bucket_and_key(files_to_check,file_names,current_file)
                    current_object.File_Bucket = bucket_name
                    current_object.Error_dest_key = org_key_name.replace('UnZipped_Files/'+current_file,'Data_Validation_Results')
    
                    current_object.load_csv_file(s3_client,bucket_name,org_key_name,pd)
                    all_file_objects.append((current_file,current_object))
    
                if current_file in ['demographic.csv']:
                    current_particiant_ids = valid_particiant_ids.append(current_object.Data_Table[['Research_Participant_ID']])
                    current_particiant_ids = current_particiant_ids['Research_Participant_ID'].tolist()
                if current_file in ['biospecimen.csv']:
                    biospec_ids = biospec_ids.append(current_object.Data_Table[['Biospecimen_ID','Biospecimen_Type']])
                if current_file in ['assay.csv']:
                    assay_id_list = assay_id_list.append(current_object.Data_Table[['Assay_ID','Biospecimen_Type']])
##########################################################################################################################################
            error_results = []
            if "prior_clinical_test.csv" in file_names:
                current_object = all_file_objects[file_names.index("prior_clinical_test.csv")][1]
                pos_list,neg_list = file_validator_object.split_participant_pos_neg_prior(current_object,pos_list,neg_list,pd)
                current_object = prior_test_result_validator(current_object,neg_list,pos_list,re,submitting_center['CBC_ID'],current_particiant_ids)
                error_results = current_object.write_error_file("Prior_Clinical_Test_Errors.csv",s3_resource,temp_file_loc,error_results,"Prior_Clinical_Test_Errors" )
            if "demographic.csv" in file_names:
                current_object = all_file_objects[file_names.index("demographic.csv")][1]
                current_object.get_pos_neg_logic(pos_list,neg_list)
                current_object.remove_unknown_sars_results_v2()
                current_object = demographic_data_validator(current_object,neg_list,pos_list,re,submitting_center['CBC_ID'])
                error_results = current_object.write_error_file("Demographic_Errors.csv",s3_resource,temp_file_loc,error_results,"Demographic_Errors")           
            if "biospecimen.csv" in file_names:
                current_object = all_file_objects[file_names.index("biospecimen.csv")][1]
                current_object.get_pos_neg_logic(pos_list,neg_list)
                current_object.remove_unknown_sars_results_v2()
                current_object = Biospecimen_validator(current_object,neg_list,pos_list,re,submitting_center['CBC_ID'],current_particiant_ids)
                error_results = current_object.write_error_file("Biospecimen_Errors.csv",s3_resource,temp_file_loc,error_results,"Biospecimen_Errors")
        if Validation_Type == TEST_MODE:
            testing_data = {'submission_validation_file_location':['Testbuket/Test_CBC_Name/'],'orig_file_id':['TEST_MODE']}
            current_submission = pd.DataFrame(testing_data, columns = ['submission_validation_file_location', 'orig_file_id'])
        elif Validation_Type == DB_MODE:
            current_submission =  successful_submissions[successful_submissions_ids== iterS]
        write_message_to_slack(http,error_results,current_submission,success,failure)   
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
def connect_to_sql_database(file_dbname,host_client,user_name,user_password):
    status_message = "Connected"
    conn = []
    try:
        conn = mysql.connector.connect(host = host_client, user=user_name, password=user_password, db=file_dbname, connect_timeout=5)
        print("SUCCESS: Connection to RDS mysql instance succeeded\n")
    except:
        status_message = "Connection Failed"
    return conn,status_message
def close_connections(conn):
    if type(conn) == mysql.connector.connection.MySQLConnection:
        conn.close()
def get_bucket_and_key(files_to_check,file_names,current_file):
    current_metadata = files_to_check.loc[file_names.index(current_file)]
    full_bucket_name = current_metadata[current_metadata.index == 'file_validation_file_location'][0]

    first_folder_cut = full_bucket_name.find('/')            
    org_key_name = full_bucket_name[(first_folder_cut+1):]
    bucket_name = full_bucket_name[:(first_folder_cut)]
    return bucket_name,org_key_name
def get_submission_metadata(s3_client,temp_file_loc,files_to_check,file_names,file_dbname,jobs_conn):
    bucket_name,org_key_name = get_bucket_and_key(files_to_check,file_names,"submission.csv")

    temp_file_loc = temp_file_loc + '/test_file'
    s3_client.download_file(bucket_name,org_key_name,temp_file_loc)
    Data_Table = pd.read_csv(temp_file_loc,encoding='utf-8',na_filter = False)

    MY_SQL = ("SELECT * FROM `" + file_dbname + "`.CBC Where CBC_Name = %s")
    submitting_center = pd.read_sql(MY_SQL, con = jobs_conn, params=[Data_Table.columns.values[1]])
    part_count =  Data_Table.iloc[1][1]
    bio_count =  Data_Table.iloc[2][1]

    return submitting_center,part_count,bio_count
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
##########################################################################################################################################