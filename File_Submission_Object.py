import pathlib
import icd10
######################################################################################################################
class Submission_Object:
    def __init__(self,pd,current_sub):                  #initalizes the Object
        """An Object that contains information for each Submitted File that Passed File_Validation."""
        first_index = current_sub.index[0]
        self.Orig_ID = current_sub['orig_file_id'][first_index]
        self.Unzipped_file_id = current_sub['unzipped_file_id']
        self.Submission_ID = current_sub['submission_file_id'][first_index]
        self.Submission_Location_Path = current_sub['submission_validation_file_location'][first_index]
        
        first_folder_cut = self.Submission_Location_Path.find('/')
        self.Bucket_Name = self.Submission_Location_Path[:(first_folder_cut)]
        self.File_Name = pathlib.PurePath(self.Submission_Location_Path).name
        self.File_dict = {}
        self.File_ids_dict = {"demo_id":[],"bio_id":[],"prior":[],"aliquot":[],"equip":[],"reagent":[], "consume":[],"assay":[],
                              "assay_target":[],"confirm":[]}
        
        self.Table_library = {"submission.csv":[],"demographic.csv":["Demographic_Data","Comorbidity","Prior_Covid_Outcome","Submission_MetaData"],
                              "assay.csv":["Assay_Metadata"],"assay_target.csv":["Assay_Target"],"biospecimen.csv":["Biospecimen","Collection_Tube"],
                              "prior_clinical_test.csv":["Prior_Test_Result"],"aliquot.csv":["Aliquot","Aliquot_Tube"],"equipment.csv":["Equipment"],
                              "confirmatory_clinical_test.csv":["Confirmatory_Test_Result"], "reagent.csv":["Reagent"],"consumable.csv":["Consumable"]}
       
        self.Error_list = pd.DataFrame(columns = ["Message_Type","CSV_Sheet_Name","Row_Index","Column_Name","Column_Value","Error_Message"])
######################################################################################################################
    def get_file_info(self,pd_s3,current_file,s3_client):
        File_Location_Path = current_file['file_validation_file_location']
        first_folder_cut = File_Location_Path.find('/')
        file_name = pathlib.PurePath(File_Location_Path).name
        self.File_dict[file_name] = {"Bucket_Name":[],"File_Location_Key":[],"Data_Table":[]}
        self.File_dict[file_name]["Bucket_Name"].append(File_Location_Path[:(first_folder_cut)])
        self.File_dict[file_name]["File_Location_Key"].append(File_Location_Path[(first_folder_cut+1):])
        
        self.get_data_tables(pd_s3,s3_client,file_name)
    def get_data_tables(self,pd_s3,s3_client,file_name):
        bucket_name = self.File_dict[file_name]["Bucket_Name"]
        key_name = self.File_dict[file_name]["File_Location_Key"]
        Data_Table = pd_s3.get_df_from_keys(s3_client, bucket_name[0], key_name[0] ,format = "csv",na_filter = False)
        self.File_dict[file_name]["Data_Table"].append(Data_Table)
        if file_name not in ["submission.csv","shipping_manifest.csv"]:
            self.cleanup_table(file_name)
            self.check_col_names(file_name)            
######################################################################################################################
    def cleanup_table(self,file_name):          
        curr_table = self.File_dict[file_name]["Data_Table"][0]
        curr_table.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)        #if a row is completely blank remove it
        curr_table = curr_table .loc[:,~ curr_table .columns.str.startswith('Unnamed')]     #if a column is completely blank, remove it
        self.File_dict[file_name]["Column_Header_List"] = list(curr_table.columns.values)   #list of Column Names
        self.File_dict[file_name]["File_Size"] = len(curr_table)                            #number of rows in the file
        self.File_dict[file_name]["Data_Table"] = curr_table
    def populate_data_tables(self,pd_s3,current_sub,s3_client,filesdb_conn):
        for iterF in range(len(current_sub.index)):
            self.get_file_info(pd_s3,current_sub.iloc[iterF],s3_client)
        if "submission.csv" in self.File_dict:
            submit_table = self.File_dict['submission.csv']['Data_Table'][0]
            self.get_submission_metadata(s3_client,submit_table,filesdb_conn)
##########################################################################################################################
    def check_col_names(self,file_name):
        names_to_check = self.sql_table_list[self.sql_table_list["TABLE_NAME"].apply(lambda x: x in self.Table_library[file_name])]["COLUMN_NAME"].tolist()  
        in_csv_not_sql = [i for i in self.File_dict[file_name]["Column_Header_List"] if i not in names_to_check]
        in_sql_not_csv = [i for i in names_to_check if i not in self.File_dict[file_name]["Column_Header_List"]]
        
        csv_errors = ["Column Found in CSV is not Expected"] * len(in_csv_not_sql)
        sql_errors = ["This Column is Expected and is missing from CSV File"] * len(in_sql_not_csv)
        name_list  = [file_name] * (len(in_csv_not_sql) + len(in_sql_not_csv))
        
        if len(name_list) > 0:
            self.Curr_col_errors["Message_Type"] = ["Error"]*len(name_list)
            self.Curr_col_errors["CSV_Sheet_Name"] = name_list
            self.Curr_col_errors["Column_Name"] = (in_csv_not_sql + in_sql_not_csv)
            self.Curr_col_errors["Error_Message"] = (csv_errors+sql_errors)
            self.Column_error_count = self.Column_error_count.append(self.Curr_col_errors)
            self.Curr_col_errors.drop(labels = range(0,len(name_list)),axis = 0, inplace = True)    
    def get_submission_metadata(self,s3_client,submit_table,filesdb_conn):
        if "submission.csv" not in self.File_dict:
            print("Submission File was not included in the list of files to validate")
        elif self.File_dict['submission.csv']['Data_Table'][0] is None:
            print("Submission File was not found in " + self.Submission_Location_Path + "/UnZipped_Files")
        else:
            try:
                submit_table = self.File_dict['submission.csv']['Data_Table'][0]
                sql_querry = filesdb_conn.cursor(prepared = True)            
                sql_querry.execute("SELECT CBC_ID FROM CBC Where CBC_Name = %s",(submit_table.columns.values[1],))
                self.CBC_ID = sql_querry.fetchall()
                self.CBC_ID = list(self.CBC_ID)[0][0]
                self.submit_Participant_IDs = self.File_dict['submission.csv']['Data_Table'][0].iloc[1][1]
                self.submit_Biospecimen_IDs = self.File_dict['submission.csv']['Data_Table'][0].iloc[2][1]
            except Exception:
                 self.CBC_ID = "00"
                 self.submit_Participant_IDs = "00"
                 self.submit_Biospecimen_IDs = "00"
##########################################################################################################################
    def populate_list_dict(self,pd,namesdb_conn,curr_sheet,dict_name,column_name_list):
        if curr_sheet in self.File_dict:
            self.File_ids_dict[dict_name] = self.File_dict[curr_sheet]['Data_Table'][column_name_list]
        else:
            self.File_ids_dict[dict_name] = get_mysql_queries(pd,namesdb_conn,curr_sheet)
    def get_all_part_ids(self):       
        all_part_ids = self.File_ids_dict['prior'].merge(self.File_ids_dict['demo_id'],how = "outer", on = "Research_Participant_ID")
        all_part_ids = all_part_ids.merge(self.File_ids_dict['bio_id'],how='outer',on="Research_Participant_ID")
        all_part_ids = all_part_ids.merge(self.File_ids_dict['confirm'],how='outer',on="Research_Participant_ID")
        return all_part_ids
    def get_all_bio_ids(self):       
        all_bio_ids = self.File_ids_dict['bio_id'].merge(self.File_ids_dict['aliquot'],how = "outer", on = "Biospecimen_ID")
        all_bio_ids = all_bio_ids.merge(self.File_ids_dict['equip'],how='outer',on="Biospecimen_ID")
        all_bio_ids = all_bio_ids.merge(self.File_ids_dict['reagent'],how='outer',on="Biospecimen_ID")
        all_bio_ids = all_bio_ids.merge(self.File_ids_dict['consume'],how='outer',on="Biospecimen_ID")
        return all_bio_ids
    def merge_tables(self,file_name,data_table):
        drop_list = []
        if file_name == "prior_clinical_test.csv":
            data_table = data_table.merge(self.File_ids_dict['demo_id'].drop_duplicates("Research_Participant_ID"),how='left',on="Research_Participant_ID")
            drop_list = ["Age","Biospecimen_ID","Biospecimen_Type"]
        elif file_name == "demographic.csv":
            data_table = data_table.merge(self.File_ids_dict['prior'],how='left',on="Research_Participant_ID")
            drop_list = ["SARS_CoV_2_PCR_Test_Result","Biospecimen_ID","Biospecimen_Type"]
        elif file_name == "biospecimen.csv":
            data_table = data_table.merge(self.File_ids_dict['prior'].drop_duplicates("Research_Participant_ID"),how='left',on="Research_Participant_ID")
            data_table = data_table.merge(self.File_ids_dict['demo_id'].drop_duplicates("Research_Participant_ID"),how='left',on="Research_Participant_ID")
            drop_list = ["Age","SARS_CoV_2_PCR_Test_Result"]
        elif file_name in ["aliquot.csv","equipment.csv","reagent.csv","consumable.csv"]:
            data_table = data_table.merge(self.File_ids_dict['bio_id'],how='left',on="Biospecimen_ID")
            drop_list = ["Biospecimen_Type"]
        elif file_name in ["aliquot.csv","equipment.csv","reagent.csv","consumable.csv"]:
            data_table = data_table.merge(self.File_ids_dict['bio_id'],how='left',on="Biospecimen_ID")
            drop_list = ["Biospecimen_Type"]
        elif file_name in ["assay_target.csv"]:
            data_table = data_table.merge(self.File_ids_dict['assay'],how='left',on="Assay_ID")
            drop_list = ["Assay_Name"]
        elif file_name in ["confirmatory_clinical_test.csv"]:
            data_table = data_table.merge(self.File_ids_dict['assay'],how='left',on="Assay_ID")
            data_table = data_table.merge(self.File_ids_dict['assay_target'],how='left',on=["Assay_ID","Assay_Target"])
            drop_list = ["Assay_Name","Assay_Antigen_Source"]
        return data_table,drop_list
##########################################################################################################################
    def add_error_values(self,msg_type,sheet_name,row_index,col_name,col_value,error_msg):
        new_row = {"Message_Type":msg_type,"CSV_Sheet_Name":sheet_name,"Row_Index":row_index,"Column_Name":col_name,"Column_Value":col_value,"Error_Message":error_msg}
        self.Error_list = self.Error_list.append(new_row, ignore_index=True)
    def sort_and_drop(self,header_name,keep_blank = False):
        self.Error_list.drop_duplicates(["Row_Index","Column_Name","Column_Value"],inplace = True)
        if keep_blank == False:
            drop_idx = self.Error_list.query("Column_Name == @header_name and Column_Value == ''").index
            self.Error_list.drop(drop_idx , inplace=True)
    def update_error_table(self,msg_type,error_data,sheet_name,header_name,error_msg,keep_blank = False):
        for i in error_data.index:
            self.add_error_values(msg_type,sheet_name,i+2,header_name,error_data.loc[i][header_name],error_msg)
        self.sort_and_drop(header_name,keep_blank)
###########################################################################################################################
    def check_assay_special(self,data_table,header_name,file_name,field_name):
        error_data = data_table.query("{0} != {0}".format(field_name))
        error_msg = header_name + " is not found in the table of valid " + header_name +"s in databse or submitted file"
        self.update_error_table("Error",error_data,file_name,header_name,error_msg,keep_blank = False)
    def check_id_field(self,sheet_name,data_table,re,field_name,pattern_str,cbc_id,pattern_error):        
        single_invalid = data_table[data_table[field_name].apply(lambda x : re.compile('^[0-9]{2}' + pattern_str).match(x) is None)]
        wrong_cbc_id   = data_table[data_table[field_name].apply(lambda x : (re.compile('^' + cbc_id + pattern_str).match(x) is None))]
        
        for i in single_invalid.index:
            if single_invalid[field_name][i] != '':
                error_msg = "ID is Not Valid Format, Expecting " + pattern_error
                self.add_error_values("Error",sheet_name,i+2,field_name,single_invalid[field_name][i],error_msg)
        for i in wrong_cbc_id.index:
            if int(cbc_id) == 0:
                error_msg = "ID is Valid however submission file is missing, unable to validate CBC code"
            else:
                error_msg = "ID is Valid however has wrong CBC code. Expecting CBC Code (" + str(cbc_id) + ")"
            self.add_error_values("Error",sheet_name,i+2,field_name,wrong_cbc_id[field_name][i],error_msg)
        self.sort_and_drop(field_name)
    def check_for_dup_ids(self,sheet_name,field_name):
        if sheet_name in self.File_dict:
            data_table = self.File_dict[sheet_name]['Data_Table']
            table_counts = data_table[field_name].value_counts(dropna=False).to_frame()
            dup_id_count = table_counts[table_counts[field_name] >  1]
            for i in dup_id_count.index:
                error_msg = "Id is repeated " + str(dup_id_count[field_name][i]) + " times, Multiple repeats are not allowed"
                self.add_error_values("Error",sheet_name,-3,field_name,i,error_msg)
    def check_if_substr(self,data_table,id_1,id_2,file_name,header_name):
        id_compare = data_table[data_table.apply(lambda x: x[id_1] not in x[id_2],axis = 1)]
        Error_Message = id_1 + " is not a substring of " + id_2 +".  Data is not Valid, please check data"
        self.update_error_table("Error",id_compare,file_name,header_name,Error_Message)
##########################################################################################################################
    def check_in_list(self,pd,sheet_name,data_table,header_name,depend_col,depend_val,list_values):
        if depend_col == "None":            #rule has no dependancy on another column
            error_msg = "Unexpected Value.  Value must be one of the following: " + str(list_values)
        elif depend_col != "None":          #rule has a dependancy on another column
            data_table,error_str = check_multi_rule(pd,data_table,depend_col,depend_val)
            error_msg = error_str + ".  Value must be one of the following: " + str(list_values)
        if len(data_table) == 0:
            return{}
        query_str = "{0} in @list_values or {0} in ['']".format(header_name)
        passing_values = data_table.query(query_str)
            
        row_index = [iterI for iterI in data_table.index if (iterI not in passing_values.index)]
        error_data = data_table.loc[row_index]
        
        self.update_error_table("Error",error_data,sheet_name,header_name,error_msg)
##########################################################################################################################
    def check_date(self,pd,datetime,sheet_name,data_table,header_name,depend_col,depend_val,na_allowed,time_check,lower_lim = 0,upper_lim = 24):
        data_table,error_str = check_for_dependancy(pd,data_table,depend_col,depend_val)
        if len(data_table) == 0:
            return{}
        
        date_only = data_table[header_name].apply(lambda x: isinstance(x,datetime.datetime))
        good_date = data_table[date_only]
        
        if time_check == "Date":
            error_msg = "Value must be a Valid Date MM/DD/YYYY"
        else:
            error_msg = "Value must be a Valid Time HH:MM:SS"
        if na_allowed == False:
            date_logic = data_table[header_name].apply(lambda x: isinstance(x,datetime.datetime) or x in [''])
        else:
            date_logic = data_table[header_name].apply(lambda x: isinstance(x,datetime.datetime) or x in ['N/A',''])
            error_msg =  error_msg + " Or N/A"
        error_data = data_table[[not x for x in date_logic]]
        self.update_error_table("Error",error_data,sheet_name,header_name,error_msg)
      
        if time_check == "Date":
            to_early = good_date[header_name].apply(lambda x: x.date() < lower_lim)
            to_late  = good_date[header_name].apply(lambda x: x.date() > upper_lim)
            if "Expiration_Date" in header_name:
                error_msg = "Expiration Date has already passed, check to make sure date is correct"
                self.update_error_table("Warning",good_date[to_early],sheet_name,header_name,error_msg)
            elif "Calibration_Due_Date" in header_name:
                error_msg = "Calibration Date has already passed, check to make sure date is correct"
                self.update_error_table("Warning",good_date[to_early],sheet_name,header_name,error_msg)
            else:
                error_msg = "Date is valid however must be between " + str(lower_lim) + " and " + str(upper_lim)
                self.update_error_table("Error",good_date[to_early],sheet_name,header_name,error_msg)
            error_msg = "Date is valid however must be between " + str(lower_lim) + " and " + str(upper_lim)
            self.update_error_table("Error",good_date[to_late],sheet_name,header_name,error_msg)
##########################################################################################################################
    def check_if_number(self,pd,sheet_name,data_table,header_name,depend_col,depend_val,na_allowed,lower_lim,upper_lim,num_type):
        data_table,error_str = check_for_dependancy(pd,data_table,depend_col,depend_val)
        if len(data_table) == 0:
            return{}
        if depend_col == "None":
            error_msg = "Value must be a number between " + str(lower_lim) + " and " + str(upper_lim)
        else:
            error_msg = error_str + ".  Value must be a number between " + str(lower_lim) + " and " + str(upper_lim)
        number_only = data_table[header_name].apply(lambda x: isinstance(x,(int,float)))        #if float allowed then so are intigers
        good_data = data_table[number_only]
        
        good_logic = data_table[header_name].apply(lambda x: isinstance(x,(int,float)) or x in [''])
        to_low  = good_data[header_name].apply(lambda x: x < lower_lim)
        to_high = good_data[header_name].apply(lambda x: x > upper_lim)
        if num_type == "int":
             is_float = good_data[header_name].apply(lambda x: x.is_integer() == False)
             error_msg = "Value must be an interger between " + str(lower_lim) + " and " + str(upper_lim) + ", decimal values are not allowed"
             self.update_error_table("Error",good_data[is_float],sheet_name,header_name,error_msg)
        if na_allowed == True:
             good_logic = data_table[header_name].apply(lambda x: isinstance(x,(int,float)) or x in ['N/A',''])
             
        error_data = data_table[[not x for x in good_logic]]
        self.update_error_table("Error",error_data,sheet_name,header_name,error_msg)
        self.update_error_table("Error",good_data[to_low],sheet_name,header_name,error_msg)
        self.update_error_table("Error",good_data[to_high],sheet_name,header_name,error_msg)
##########################################################################################################################
    def compare_total_to_live(self,pd,sheet_name,data_table,header_name):
        second_col = header_name.replace('Total_Cells','Live_Cells')
        data_table,error_str = check_for_dependancy(pd,data_table,header_name,"Is A Number")
        data_table,error_str = check_for_dependancy(pd,data_table,second_col,"Is A Number")
        error_data = data_table.query("{0} > {1}".format(second_col,header_name))
        error_msg = "Live Cell Count must be less than Total Cell Count"
        self.update_error_table("Error",error_data,sheet_name,header_name,error_msg)
    def compare_viability(self,pd,sheet_name,data_table,header_name):
        live_col = header_name.replace('Viability','Live_Cells')
        total_col = header_name.replace('Viability','Total_Cells')
        data_table,error_str = check_for_dependancy(pd,data_table,header_name,"Is A Number")
        data_table,error_str = check_for_dependancy(pd,data_table,live_col,"Is A Number")
        data_table,error_str = check_for_dependancy(pd,data_table,total_col,"Is A Number")
        error_data = data_table[data_table.apply(lambda x: round((x[live_col]/x[total_col])*100,1) != x[header_name],axis = 1)]
        error_msg = "Viability Count must be equal to (Live_Count / Total_Count) * 100"
        self.update_error_table("Error",error_data,sheet_name,header_name,error_msg)
##########################################################################################################################
    def check_if_string(self,pd,sheet_name,data_table,header_name,depend_col,depend_val,na_allowed):
        data_table,error_str = check_for_dependancy(pd,data_table,depend_col,depend_val)
        if len(data_table) == 0:
            return{}
        if depend_col == "None":
            error_msg = "Value must be a string and NOT N/A"  
        else:
            error_msg = error_str + ".  Value must be a string and NOT N/A"        
        good_logic = data_table[header_name].apply(lambda x: isinstance(x,str) or x in [''])        
        if na_allowed == True:
             good_logic = data_table[header_name].apply(lambda x: isinstance(x,str) or x in ['N/A',''])
             
        error_data = data_table[[not x for x in good_logic]]
        self.update_error_table("Error",error_data,sheet_name,header_name,error_msg)
##########################################################################################################################
    def check_icd10(self,sheet_name,data_table,header_name):
        number_data = data_table[data_table[header_name].apply(lambda x: not isinstance(x,str))]
        data_table = data_table[data_table[header_name].apply(lambda x: isinstance(x,str))]
        error_data = data_table[data_table[header_name].apply(lambda x: not (icd10.exists(x) or x in ["N/A"]))]
        Error_Message = "Invalid or unknown ICD10 code, Value must be Valid ICD10 code or N/A"
        self.update_error_table("Error",error_data,sheet_name,header_name,Error_Message)
        self.update_error_table("Error",number_data,sheet_name,header_name,Error_Message)
##########################################################################################################################
    def add_warning_msg(self,neg_values,neg_msg,neg_error_msg,pos_values,pos_msg,pos_error_msg,sheet_name,header_name):
         self.update_error_table(neg_msg,neg_values,sheet_name,header_name,neg_error_msg,True)
         self.update_error_table(pos_msg,pos_values,sheet_name,header_name,pos_error_msg,True)
    def get_missing_values(self,pd,sheet_name,data_table,header_name,Required_column):
        missing_data = data_table.query("{0} == '' ".format(header_name))
        if len(missing_data) > 0:
            if Required_column == "Yes":
                error_msg = "Missing Values are not allowed for this column.  Please recheck data"
                self.update_error_table("Error",missing_data,sheet_name,header_name,error_msg,True)
            elif Required_column == "No":
                error_msg = "Missing Values where found, this is a warning.  Please recheck data"
                self.update_error_table("Warning",missing_data,sheet_name,header_name,error_msg,True)
                
            elif Required_column in[ "Yes: SARS-Positive","Yes: SARS-Negative"]:
                neg_values = missing_data.query("SARS_CoV_2_PCR_Test_Result == 'Negative'")
                pos_values = missing_data.query("SARS_CoV_2_PCR_Test_Result == 'Positive'")
                warn_msg = "Missing Values where found, this is a warning.  Please recheck data"
                if Required_column ==  "Yes: SARS-Positive":
                    error_msg = "This column is requred for Sars Positive Patients, missing values are not allowed.  Please recheck data"
                    self.add_warning_msg(neg_values,'Warning',warn_msg,pos_values,'Error',error_msg,sheet_name,header_name)
                else:
                    error_msg = "This column is requred for Sars Negative Patients, missing values are not allowed.  Please recheck data"
                    self.add_warning_msg(neg_values,'Error',error_msg,pos_values,'Warning',warn_msg,sheet_name,header_name)
##########################################################################################################################
    def write_cross_sheet_id_error(self,merged_data,query_str,error_msg,field_name):
        check_id_only = merged_data.query(query_str.format("SARS_CoV_2_PCR_Test_Result","Age","Biospecimen_ID")) 
        for iterZ in range(len(check_id_only)):
            self.add_error_values("Error","Cross_Participant_ID.csv",-10,field_name,check_id_only.iloc[iterZ][field_name],error_msg)
        self.sort_and_drop(field_name,True)
##########################################################################################################################
    def write_cross_bio_errors(self,merged_data,table_name,sheet_name):
        error_data = merged_data.query("Biospecimen_Type != Biospecimen_Type and {0} == {0}".format(table_name))
        error_msg = "ID is found in " + sheet_name + ", however ID is missing from Biospecimen.csv"
        self.update_error_table("Error",error_data,"Cross_Biospecimen_ID.csv","Biospecimen_ID",error_msg)          
        if table_name in ["Aliquot_ID"]:
            error_data = merged_data.query("Biospecimen_Type == Biospecimen_Type and {0} != {0}".format(table_name))
            error_msg = "ID is found in Biospecimen.csv, however is missing from " + sheet_name
            self.update_error_table("Error",error_data,"Cross_Biospecimen_ID.csv","Biospecimen_ID",error_msg) 
        else:   
            error_data = merged_data.query("Biospecimen_Type != 'PBMC' and Biospecimen_Type == Biospecimen_Type and {0} == {0}".format(table_name))
            error_msg = "ID is found in " + sheet_name + ", and ID is found in Biospecimen.csv however has Biospecimen_Type NOT PBMC"
            self.update_error_table("Error",error_data,"Cross_Biospecimen_ID.csv","Biospecimen_ID",error_msg)
            error_data = merged_data.query("Biospecimen_Type == 'PBMC' and Biospecimen_Type == Biospecimen_Type and {0} != {0}".format(table_name))
            error_msg = "ID is found in Biospecimen.csv and has Biospecimen_Type of PBMC, however ID is missing from " + sheet_name
            self.update_error_table("Error",error_data,"Cross_Biospecimen_ID.csv","Biospecimen_ID",error_msg)
    def get_submitted_ids(self,pd,file_list,col_name,merged_data):
        all_pass= []
        for iterF in self.File_dict:
            if iterF in file_list:
                all_pass = all_pass + self.File_dict[iterF]['Data_Table'][col_name].tolist()
        
        if len(all_pass) == 0:
            return all_pass
        else:
            all_pass = pd.Series(all_pass, name = col_name)
            merged_data.merge(all_pass,on = col_name, how = "right")
            return merged_data
    def get_cross_sheet_Biospecimen_ID(self,pd,re,merged_data,valid_cbc,field_name):
        merged_data = merged_data[merged_data.isna().any(axis=1)]
        file_list = ['biospecimen.csv','aliquot.csv','equipment.csv','reagent.csv','consumable.csv']
        merged_data = merged_data[merged_data[field_name].apply(lambda x : (re.compile('^' + valid_cbc + '[_]{1}[0-9]{6}[_]{1}[0-9]{3}$').match(x) is not None))]
        merged_data  = self.get_submitted_ids(pd,file_list,'Biospecimen_ID',merged_data)
        if len(merged_data) > 0:
            self.write_cross_bio_errors(merged_data,"Aliquot_ID","Aliquot.csv")
            self.write_cross_bio_errors(merged_data,"Equipment_ID","Equipment.csv")
            self.write_cross_bio_errors(merged_data,"Reagent_Name","Reagent.csv")
            self.write_cross_bio_errors(merged_data,"Consumable_Name","Consumable.csv")
    def get_cross_sheet_Participant_ID(self,pd,re,merged_data,valid_cbc,field_name):
        merged_data = merged_data[merged_data.isna().any(axis=1)]
        if len(merged_data) > 0:                #if there are unmatcehd IDS then remove bad IDS and filter to submitted list
            file_list = ['prior_clinical_test.csv','demographic.csv','biospecimen.csv','confirmatory_clinical_test.csv']
            merged_data = merged_data[merged_data[field_name].apply(lambda x : (re.compile('^' + valid_cbc + '[_]{1}[0-9]{6}$').match(x) is not None))]
            merged_data  = self.get_submitted_ids(pd,file_list,'Research_Participant_ID',merged_data)
        if len(merged_data) > 0:                #only checks for errors if there are IDs left after the filtering
            error_msg = "ID is found in Prior_Clinical_Test, but is missing from Demographic and Biospecimen"
            self.write_cross_sheet_id_error(merged_data,"{0} == {0} and {1} != {1} and {2} != {2}",error_msg,field_name)
            error_msg = "ID is found in Demographic, but is missing from Prior_Clinical_Test and Biospecimen"
            self.write_cross_sheet_id_error(merged_data,"{0} != {0} and {1} == {1} and {2} != {2}",error_msg,field_name)
            error_msg = "ID is found in Biospecimen, but is missing from Prior_Clinical_Test and Demographic"
            self.write_cross_sheet_id_error(merged_data,"{0} != {0} and {1} != {1} and {2} == {2}",error_msg,field_name)
            error_msg = "ID is found in Prior_Clinical_Test and Demographic but is missing from Biospecimen"
            self.write_cross_sheet_id_error(merged_data,"{0} == {0} and {1} == {1} and {2} != {2}",error_msg,field_name)
            error_msg = "ID is found in Prior_Clinical_Test and Biospecimen but is missing from Demographic"
            self.write_cross_sheet_id_error(merged_data,"{0} == {0} and {1} != {1} and {2} == {2}",error_msg,field_name)
            error_msg = "ID is found in Demographic and Biospecimen but is missing from Prior_Clinical_Test"
            self.write_cross_sheet_id_error(merged_data,"{0} != {0} and {1} == {1} and {2} == {2}",error_msg,field_name)
    def get_passing_part_ids(self,check_list,check_field):
        all_pass = []
        for iterD in check_list:
            if iterD in self.File_dict:
                submit_ids = self.File_dict[iterD]['Data_Table'][check_field].tolist()
                error_table = self.Error_list.query("CSV_Sheet_Name == @iterD and Column_Name == @check_field and Row_Index >= 0")
                pass_ids = [i for i in submit_ids if i not in error_table['Column_Value'].tolist()]
                all_pass = all_pass + pass_ids
        all_pass_count = len((set(all_pass)))
        if (int(self.submit_Participant_IDs) != all_pass_count) and (check_field == "Research_Participant_ID"):
            error_msg = "After validation only " + str(all_pass_count) + " Participat IDS are valid"
            self.add_error_values("Error","submission.csv",-5,"submit_Participant_IDs",self.submit_Participant_IDs,error_msg)
        elif (int(self.submit_Biospecimen_IDs) != all_pass_count) and (check_field == "Biospecimen_ID"):
            error_msg = "After validation only " + str(all_pass_count) + " Biospecimen IDS are valid"
            self.add_error_values("Error","submission.csv",-5,"submit_Biospecimen_IDs",self.submit_Biospecimen_IDs,error_msg)     
##########################################################################################################################
    def write_error_file(self,pd_s3,s3_client):
        key_name, separator, after = self.Submission_Location_Path.rpartition('/')
        key_name = key_name[(len(self.Bucket_Name)+1):]
        uni_name = list(set(self.Error_list["CSV_Sheet_Name"]))
        for iterU in uni_name:
            curr_table = self.Error_list.query("CSV_Sheet_Name == @iterU")
            curr_name = iterU.replace('.csv','_Errors.csv')
            curr_key = key_name + "/Data_Validation_Results/" + curr_name
            if uni_name in ["Cross_Participant_ID.csv","Cross_Biospecimen_ID.csv","submission.csv"]:
                curr_table = curr_table.sort_index()
            else:
                curr_table = curr_table.sort_values('Row_Index')
            pd_s3.put_df(s3_client,curr_table,self.Bucket_Name,curr_key,format = "csv")
            print(iterU +  " has " + str(len(curr_table)) + " Errors")
##########################################################################################################################
    def get_submit_by(self,re):
        submision_string = self.Submission_Location_Path
        slash_index = [m.start() for m in re.finditer('/',submision_string)]
        file_submitted_by = submision_string[(slash_index[0]+1):(slash_index[1])]
        file_name = self.File_Name
        org_file_id = str(self.Orig_ID)
        return file_submitted_by,file_name,org_file_id
    def update_jobs_tables(self,pd,jobs_conn,current_sub_object,error_string,validation_date):
        sql_connect = jobs_conn.cursor()
        sql_connect.execute("select current_user();")
        curr_user = sql_connect.fetchall()
        curr_user = curr_user[0][0]
        if error_string == "Column_Error":
            error_table = self.Column_error_count
        else:
            error_table = self.Error_list
        
        column_list = ["orig_file_id", "data_validation_result_location", "data_validation_date", "unzipped_file_id", 
                       "data_validation_notification_arn","data_validation_status", "batch_validation_status", "data_validation_updatedby"]
        
        file_count = len(self.File_dict)
        key_name, separator, after = self.Submission_Location_Path.rpartition('/')
        curr_key = key_name + "/Data_Validation_Results/"
              
        status_field = []
        for iterZ in self.File_dict:
            if len(error_table.query("CSV_Sheet_Name == @iterZ and Message_Type == 'Error'")) > 0:
                if error_string == "Column_Error":
                    status_field.append("FILE_NOT_PROCESSED_COLUMN_ERRORS_FOUND")
                else:
                    status_field.append("FILE_PROCESSED_ERRORS_FOUND")
            elif len(current_sub_object.Error_list.query("CSV_Sheet_Name == @iterZ and Message_Type == 'Warning'")) > 0:
                status_field.append("FILE_PROCESSED_WARNINGS_FOUND")
            else:
                if error_string == "Column_Error":
                    status_field.append("FILE_NOT_PROCESSED")
                else:
                    status_field.append("FILE_PROCESSED_SUCCESS")

        if "FILE_PROCESSED_ERRORS_FOUND" in status_field:
            batch_status = ["FILE_VALIDATION_FAILURE"]*file_count
        elif "FILE_PROCESSED_WARNINGS_FOUND" in status_field:
            batch_status = ["FILE_VALIDATION_SUCCESS_WARNINGS"]*file_count
        elif "FILE_NOT_PROCESSED_COLUMN_ERRORS_FOUND" in status_field:
            batch_status = ["FILE_NOT_VALIDATED_COLUMN_ERRORS"]*file_count
        else:
            batch_status = ["FILE_VALIDATION_SUCCESS"]*file_count
        
        for iterZ in range(file_count):
            sql_statement = "Select * from table_data_validator where unzipped_file_id = %s"
            sql_connect.execute(sql_statement, (str(self.Unzipped_file_id.iloc[iterZ]),))
            rows = sql_connect.fetchall()
            tuple_list = (str(self.Orig_ID),curr_key,validation_date,str(self.Unzipped_file_id.iloc[iterZ]),"Unknown ARN",status_field[iterZ],batch_status[iterZ],curr_user)

            if len(rows) > 0:
                cols = ", ".join([str(i[1]) + " = '" + tuple_list[i[0]] + "'" for i in enumerate(column_list)])
                sql = f"UPDATE `table_data_validator` set {cols} where unzipped_file_id = %s"
                sql_connect.execute(sql, (str(self.Unzipped_file_id.iloc[iterZ]),))
            else:
                cols = "`,`".join([str(i) for i in column_list])
                sql = f"INSERT INTO `table_data_validator` (`{cols}`) VALUES (" + "%s,"*(len(column_list)-1) + "%s)"
                sql_connect.execute(sql, tuple_list)
            
            sql = "UPDATE `table_file_validator` set file_validation_status = %s where unzipped_file_id = %s"
            tuple_list = (status_field[iterZ],str(self.Unzipped_file_id.iloc[iterZ]))
            sql_connect.execute(sql, tuple_list)
        jobs_conn.commit()
        sql_connect.close()
##########################################################################################################################
def get_mysql_queries(pd,conn,index_name):
    if index_name == "prior_clinical_test.csv":
        sql_querry = ("SELECT Research_Participant_ID, Test_Result FROM `Participant_Prior_Test_Result` Where Test_Name = %s;")
        prior_test = pd.read_sql(sql_querry, conn,params=["SARS_Cov_2_PCR"])
        prior_test.rename(columns = {"Test_Result":"SARS_CoV_2_PCR_Test_Result"},inplace = True)
        return prior_test
    elif index_name == "demographic.csv":
        sql_querry = "SELECT Research_Participant_ID,Age FROM Participant;"
    elif index_name == "biospecimen.csv":
        sql_querry = "SELECT Research_Participant_ID,Biospecimen_ID,Biospecimen_Type FROM Biospecimen;"
    elif index_name == "aliquot.csv":
        sql_querry = "SELECT Aliquot_ID,Biospecimen_ID FROM Aliquot;"
    elif index_name == "equipment.csv":
        sql_querry = "SELECT Equipment_ID,Biospecimen_ID FROM Biospecimen_Equipment;"
    elif index_name == "reagent.csv":
         sql_querry = "SELECT Reagent_Name,Biospecimen_ID FROM Reagent_Biospecimen;"
    elif index_name == "consumable.csv":
         sql_querry = "SELECT Consumable_Name,Biospecimen_ID FROM Consumable_Biospecimen;"
    elif index_name == "assay.csv":
         sql_querry = "SELECT Assay_ID,Assay_Name FROM Assay;"
    elif index_name == "assay_target.csv":
          sql_querry = "SELECT Assay_ID,Assay_Target,Assay_Antigen_Source FROM Assay;"
    elif index_name ==  "confirmatory_clinical_test.csv":
        sql_querry = "SELECT Research_Participant_ID,Assay_ID FROM Participant_Confirmatory_Assay_Result;"
         
    curr_ids = pd.read_sql(sql_querry, conn)
    return curr_ids
def check_for_dependancy(pd,data_table,depend_col,depend_val):
    error_str = ""
    if depend_col != "None":          #rule has a dependancy on another column
        data_table,error_str = check_multi_rule(pd,data_table,depend_col,depend_val)
    return data_table,error_str
def check_multi_rule(pd,data_table,depend_col,depend_val):
    if depend_val == "Is A Number":             #dependant column must be a number
        data_table = data_table[data_table[depend_col].apply(lambda x: isinstance(x,(float,int)))]
        error_str = depend_col + " is a Number "
    elif depend_val == "Is A Date":             #dependant column must be a Date
        data_table = data_table[data_table[depend_col].apply(lambda x: isinstance(x,pd.Timestamp))]
        error_str = depend_col + " is a Date "
    else:                                       #dependant column must be a list or fixed value
        data_table = data_table[data_table[depend_col].apply(lambda x: x in depend_val)]
        error_str = depend_col + " is in " +  str(depend_val)
    return data_table,error_str