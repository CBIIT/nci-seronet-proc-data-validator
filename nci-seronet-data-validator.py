import boto3
import json
import urllib3
import mysql.connector as sql_connect
import pandas as pd
import s3 as pd_s3
import dateutil.tz
from dateutil.parser import parse
import datetime
import re

import File_Submission_Object
from Validation_Rules import Validation_Rules
#############################################################################################################  
eastern = dateutil.tz.gettz('US/Eastern')                                   #converts time into Eastern time zone (def is UTC)
validation_date = datetime.datetime.now(tz=eastern).strftime("%Y-%m-%d %H:%M:%S")
#############################################################################################################  
DB_MODE = "DB_Mode"
TEST_MODE = "Test_Mode"
############################################################################################################# 
def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    ssm = boto3.client("ssm")
    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    jobs_dbname =  ssm.get_parameter(Name="jobs_db_name", WithDecryption=True).get("Parameter").get("Value")
    names_dbname = ssm.get_parameter(Name="Prevalidated_DB", WithDecryption=True).get("Parameter").get("Value")
    files_dbname = ssm.get_parameter(Name="Validated_DB", WithDecryption=True).get("Parameter").get("Value")
#############################################################################################################
    http = urllib3.PoolManager()
    slack_failure = ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")     #failure slack channel
    slack_success = ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")     #success slack channel
#############################################################################################################
    http = urllib3.PoolManager()
    slack_failure = ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")     #failure slack channel
    slack_success = ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")     #success slack channel
#############################################################################################################
    jobs_conn,j_status_message    = connect_to_sql_database(jobs_dbname,host_client,user_name,user_password)
    namesdb_conn,n_status_message = connect_to_sql_database("INFORMATION_SCHEMA",host_client,user_name,user_password)
    filesdb_conn,f_status_message = connect_to_sql_database(files_dbname,host_client,user_name,user_password)
      
    if "Connection Failed" in [j_status_message,n_status_message,f_status_message]:
        print("Unable to Connect to MYSQL Database")
        stop_validation(jobs_conn,namesdb_conn,filesdb_conn)
        return{}
    del j_status_message,n_status_message,f_status_message
#############################################################################################################    
    Validation_Type = DB_MODE
    if ('TEST_MODE' in event) and (event['TEST_MODE']=="On"):       #if TEST_MODE is off treats as DB_MODE
        Validation_Type = TEST_MODE
#############################################################################################################
    submission_list = get_list_of_valid_submissions(pd,jobs_conn,filesdb_conn,s3_client,Validation_Type,event)
    if len(submission_list) == 0:
        print("There are no new files to process.")
        return{}
    col_name_table = get_sql_table_names(pd,namesdb_conn,names_dbname) 
    valid_ids = list(set(submission_list['orig_file_id']))
    
    for iterS in valid_ids:
        try:
            curr_sub_table = submission_list[submission_list['orig_file_id'] == iterS]
            current_sub_object = File_Submission_Object.Submission_Object(pd,curr_sub_table)
            print("\n## Starting the Data Validation Proccess for " + current_sub_object.File_Name + " ##\n")
            
            current_sub_object.Column_error_count = pd.DataFrame(columns = ["Message_Type","CSV_Sheet_Name","Column_Name","Error_Message"])
            current_sub_object.Curr_col_errors = pd.DataFrame(columns = ["Message_Type","CSV_Sheet_Name","Column_Name","Error_Message"])
            
            current_sub_object.sql_table_list = col_name_table
            current_sub_object.populate_data_tables(pd_s3,curr_sub_table,s3_client,filesdb_conn)
            col_error_val = check_submission_quality(current_sub_object,http,slack_failure)
            if col_error_val == -1:
                print("Errors were found when validating column names.  Unable to Proceed with Validation")
                if Validation_Type != TEST_MODE:
                    current_sub_object.update_jobs_tables(pd,jobs_conn,current_sub_object,"Column_Error",validation_date)
                continue
#########################################################################################################################################
            current_sub_object.populate_list_dict(pd,filesdb_conn,"prior_clinical_test.csv",'prior',["Research_Participant_ID","SARS_CoV_2_PCR_Test_Result"])
            current_sub_object.populate_list_dict(pd,filesdb_conn,"demographic.csv",'demo_id',["Research_Participant_ID","Age"])
            current_sub_object.populate_list_dict(pd,filesdb_conn,"biospecimen.csv","bio_id",["Research_Participant_ID","Biospecimen_ID","Biospecimen_Type"])
            current_sub_object.populate_list_dict(pd,filesdb_conn,"aliquot.csv",'aliquot',["Aliquot_ID","Biospecimen_ID"])
            current_sub_object.populate_list_dict(pd,filesdb_conn,"equipment.csv",'equip',["Equipment_ID","Biospecimen_ID"])
            current_sub_object.populate_list_dict(pd,filesdb_conn,"reagent.csv","reagent",["Reagent_Name","Biospecimen_ID"])
            current_sub_object.populate_list_dict(pd,filesdb_conn,"consumable.csv","consume",["Consumable_Name","Biospecimen_ID"])
            all_part_ids = current_sub_object.get_all_part_ids()
            all_bio_ids = current_sub_object.get_all_bio_ids()
##########################################################################################################################################
            valid_cbc_ids = current_sub_object.CBC_ID
            for file_name in current_sub_object.File_dict:
                if file_name not in ["submission.csv","shipping_manifest.csv"]:
                    data_table = current_sub_object.File_dict[file_name]['Data_Table']
                    data_table,drop_list = current_sub_object.merge_tables(file_name,data_table)
                    try:
                        col_names = data_table.columns
                        data_table = pd.DataFrame([convert_data_type(c) for c in l] for l in data_table.values)
                        data_table.columns = col_names
                    except Exception as e:
                        print(e)
##########################################################################################################################################              
                    Validation_Rules(pd,re,datetime,current_sub_object,data_table,file_name,valid_cbc_ids,drop_list)
##########################################################################################################################################
            current_sub_object.check_for_dup_ids("prior_clinical_test.csv",'Research_Participant_ID')
            current_sub_object.check_for_dup_ids("demographic.csv",'Research_Participant_ID')
            current_sub_object.check_for_dup_ids("biospecimen.csv",'Biospecimen_ID')
            current_sub_object.get_cross_sheet_Participant_ID(re,all_part_ids,valid_cbc_ids,'Research_Participant_ID')
            current_sub_object.get_cross_sheet_Biospecimen_ID(re,all_bio_ids,valid_cbc_ids,'Biospecimen_ID')
            current_sub_object.get_passing_part_ids(["prior_clinical_test.csv","demographic.csv","biospecimen.csv"],'Research_Participant_ID')
            current_sub_object.get_passing_part_ids(["biospecimen.csv","aliquot.csv","reagent.csv","equipment.csv","consumable.csv"],'Biospecimen_ID')
##########################################################################################################################################
            try:
                write_message_to_slack(http,current_sub_object,slack_success,slack_failure)
                current_sub_object.write_error_file(pd_s3,s3_client)
                if Validation_Type != TEST_MODE:
                    current_sub_object.update_jobs_tables(pd,jobs_conn,current_sub_object,"File_Error",validation_date)
            except Exception as job_error:
                print("An Error occured while writting to the SQL database or Slack Channel")
                display_error_line(job_error)
        except Exception as curr_error:
            display_error_line(curr_error)
            print("An Error occured during data Validation.  Moving onto Next Submitted File")
        finally:
            if len(submission_list) > 0:
                if iterS == valid_ids[-1]:
                    print("All Files have been run through the Data Validation Process")
    print("Closing all connections")
    stop_validation(jobs_conn,namesdb_conn,filesdb_conn)
##########################################################################################################################################
def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,"name": tb.tb_frame.f_code.co_name,"lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__,'message': str(ex),'trace': trace}))
##########################################################################################################################################
def connect_to_sql_database(file_dbname,host_client,user_name,user_password):
    status_message = "Connected"
    conn = []
    try:
        conn = sql_connect.Connect(host = host_client, user=user_name, password=user_password, db=file_dbname, connect_timeout=5)
        print("SUCCESS: Connection to RDS mysql instance succeeded\n")
    except Exception as e:
        print(e)
        status_message = "Connection Failed"
    return conn,status_message
def close_connections(conn):
    if isinstance(conn,sql_connect.MySQLConnection):
        conn.close()
def stop_validation(jobs_conn,namesdb_conn,filesdb_conn):
    print("Terminating The Data Validation Process")
    close_connections(jobs_conn)
    close_connections(namesdb_conn)
    close_connections(filesdb_conn)
def get_sql_table_names(pd,conn,db_name):
    sql_querry = "SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = %s"
    col_name_table = pd.read_sql(sql_querry, conn, params=[db_name])
    values_to_ignore = ['Submission_ID','Submission_CBC','Biorepository_ID','Shipping_ID', 'Test_Agreement','Submission_time']
    col_name_table = col_name_table[col_name_table["COLUMN_NAME"].apply(lambda x: x not in values_to_ignore)]
    return col_name_table
def get_submission_metadata(s3_client,Data_Table,conn):
    try:
        MY_SQL = ("SELECT * FROM CBC Where CBC_Name = %s")
        submitting_center = pd.read_sql(MY_SQL, con = conn, params=[Data_Table.columns.values[1]])
    except Exception:
        print ("Submission Center Name does not exist in the Database")
        submitting_center = []
    return submitting_center
##########################################################################################################################################
def get_list_of_valid_submissions(pd,jobs_conn,filesdb_conn,s3_client,Validation_Type,event):
    all_files_to_check = pd.DataFrame(columns=["orig_file_id","submission_file_id","unzipped_file_id","submission_validation_file_location","file_validation_file_location"])
    if Validation_Type == DB_MODE:
        MY_SQL = ("SELECT sub.orig_file_id,sub.submission_file_id,tbl.unzipped_file_id, sub.submission_validation_file_location, tbl.file_validation_file_location "
                 "FROM table_submission_validator as sub JOIN table_file_validator as tbl "
                  "ON sub.submission_file_id = tbl.submission_file_id "  
                  "where batch_validation_status = %s and file_validation_status = %s;")
        all_files_to_check = pd.read_sql(MY_SQL, con=jobs_conn, params=["Batch_Validation_SUCCESS","FILE_VALIDATION_IN_PROGRESS"])
    else:        
        print("testMode is enabled")
        all_files_to_check = pd.DataFrame(columns = ['orig_file_id', 'submission_file_id', 'unzipped_file_id',
                                'submission_validation_file_location', 'file_validation_file_location'])
        for iterS in range(1,len(event)):
            files_to_check = pd.DataFrame(columns = ['orig_file_id', 'submission_file_id', 'unzipped_file_id',
                                'submission_validation_file_location', 'file_validation_file_location'])

            test_file_list = event[list(event)[iterS]]['S3']
            files_to_check['file_validation_file_location'] = test_file_list
            for iterF in enumerate(test_file_list):
                test_file_list[iterF[0]] = iterF[1][:iterF[1].find("/UnZipped")] + "/" + list(event)[iterS]
                files_to_check.at[iterF[0],"unzipped_file_id"] = iterF[0]
            
            files_to_check['submission_validation_file_location'] = test_file_list
            files_to_check['orig_file_id'] = iterS
            files_to_check['submission_file_id'] = list(event)[iterS]
            all_files_to_check  = pd.concat([all_files_to_check,files_to_check])
    return all_files_to_check
def check_submission_quality(current_sub_object,http,failure):
    error_message  = []
    if "submission.csv" not in current_sub_object.File_dict:
        error_message = "Submission File was not included in the list of files to validate"
    elif current_sub_object.File_dict['submission.csv']['Data_Table'][0] is None:
        error_message = "Submission File was not found in the S3 Bucket"
    elif len(current_sub_object.Column_error_count) > 0:
        error_count = len(current_sub_object.Column_error_count)
        error_message = "Errors were found in " + str(error_count) + " column names, unable to Validate Submission"
    if len(error_message) > 0:
        write_slack_error(http,failure,error_message,current_sub_object)
        return -1
    return 0
##########################################################################################################################################
def convert_data_type(v):
    if str(v).find('_') > 0:
        return v
    else:
        try:
            return float(v)
        except ValueError:
            try:
                return parse(v)
            except ValueError:
                return v
##########################################################################################################################################
def write_slack_error(http,failure,error_message,current_submission):    
    file_submitted_by = current_submission.get_submit_by(re)
    file_name = current_submission.File_Name
    org_file_id = str(current_submission.Orig_ID)
    
    message_slack = (f"{file_name}(Job ID: {org_file_id} CBC ID: {file_submitted_by})\n " + 
                     f"*FAILURE_Reason:* *{error_message}* \n File validated on {validation_date}")
              
    data={"type": "mrkdwn","text": message_slack}
    http.request("POST",failure,body=json.dumps(data), headers={"Content-Type":"application/json"}) 
##########################################################################################################################################
def fix_table(test_table,Col_val):
    if Col_val not in test_table.columns:
        test_table[Col_val] = 0
    return test_table
def populate_slack_string(final_table,query_str,test_str,table_col):
    error_table = final_table.query(query_str)
    if len(error_table) > 0:
        files_with_errors = [i + " ("+ str(error_table.loc[i][table_col]) + ") " for i in error_table.index]
        test_str = ', '.join(files_with_errors)
    return test_str
def get_error_lists(current_submission,passString,pass_warn_String,failString):
    final_table = pd.DataFrame(0, index=current_submission.File_dict, columns=["Error","Warning"])
    test_table = pd.crosstab(current_submission.Error_list['CSV_Sheet_Name'],current_submission.Error_list['Message_Type'])
    test_table = fix_table(test_table,"Warning")
    test_table = fix_table(test_table,"Error")    
    for iterI in test_table.index:
        final_table.loc[iterI] = test_table.loc[iterI]
    
    failString = populate_slack_string(final_table,"Error > 0",failString,"Error")
    pass_warn_String = populate_slack_string(final_table,"Error == 0 and Warning > 0",pass_warn_String,"Warning")
    passString = populate_slack_string(final_table,"Error == 0 and Warning == 0",passString,"Warning")
    return passString,pass_warn_String,failString
def write_message_to_slack(http,current_submission,slack_success,slack_failure):
    slack_channel = slack_failure    
    file_submitted_by = current_submission.get_submit_by(re)
    file_name = current_submission.File_Name
    org_file_id = current_submission.Orig_ID
                
    total_errors = len(current_submission.Error_list)
    if total_errors == 0:
        slack_channel = slack_success
    
    passString,pass_warn_String,failString = get_error_lists(current_submission,"N/A","N/A","N/A")
    message_slack = (f"{file_name}(Job ID: {org_file_id} CBC ID: {file_submitted_by})\nValidation pass clean: ({passString})\n "  + 
                    f"Validation pass warning: (_{pass_warn_String}_) \n" + 
                    f"*Validation fail:* (*{failString}*)\n File validated on {validation_date}")
              
    data={"type": "mrkdwn","text": message_slack}
    http.request("POST",slack_channel,body=json.dumps(data), headers={"Content-Type":"application/json"})
##########################################################################################################################################