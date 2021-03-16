import pathlib
import icd10
######################################################################################################################
class Submission_Object:
    def __init__(self,pd,current_sub):                  #initalizes the Object
        """An Object that contains information for each Submitted File that Passed File_Validation"""
        first_index = current_sub.index[0]
        self.Orig_ID = current_sub['orig_file_id'][first_index]
        self.Submission_ID = current_sub['submission_file_id'][first_index]
        self.Submission_Location_Path = current_sub['submission_validation_file_location'][first_index]
        
        first_folder_cut = self.Submission_Location_Path.find('/')
        self.Bucket_Name = self.Submission_Location_Path[:(first_folder_cut)]
        self.File_Name = pathlib.PurePath(self.Submission_Location_Path).name
        self.File_dict = {}
        self.File_ids_dict = {"demo_id":[],"bio_id":[],"prior":[],"aliquot":[],"equip":[],"reagent":[], "consume":[]}
        
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
            self.Curr_col_errors["Sheet_Name"] = name_list
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
        return all_part_ids
    def merge_tables(self,file_name,data_table):
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
        return data_table,drop_list
##########################################################################################################################
    def add_error_values(self,msg_type,sheet_name,row_index,col_name,col_value,error_msg):
        new_row = {"Message_Type":msg_type,"CSV_Sheet_Name":sheet_name,"Row_Index":row_index,"Column_Name":col_name,"Column_Value":col_value,"Error_Message":error_msg}
        self.Error_list = self.Error_list.append(new_row, ignore_index=True)
    def sort_and_drop(self):
        self.Error_list.drop_duplicates(["Column_Value"],inplace = True)
        drop_idx = self.Error_list[self.Error_list['Column_Value'] == ''].index
        self.Error_list.drop(drop_idx , inplace=True)
        self.Error_list.sort_index()
    def update_error_table(self,msg_type,error_data,sheet_name,header_name,error_msg):
        for i in error_data.index:
            self.add_error_values(msg_type,sheet_name,i+2,header_name,error_data.loc[i][header_name],error_msg)
        self.sort_and_drop()
###########################################################################################################################
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
        self.sort_and_drop()
    def check_for_dup_ids(self,sheet_name,field_name):
        if sheet_name in self.File_dict:
            data_table = self.File_dict[sheet_name]['Data_Table']
            table_counts = data_table[field_name].value_counts(dropna=False).to_frame()
            dup_id_count = table_counts[table_counts[field_name] >  1]
            for i in dup_id_count.index:
                error_msg = "Id is repeated " + str(dup_id_count[field_name][i]) + " times, Multiple repeats are not allowed"
                self.add_error_values("Error",sheet_name,-3,field_name,i,error_msg)
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
        error_msg = error_str + ".  Value must be a string and NOT N/A"        
        good_logic = data_table[header_name].apply(lambda x: isinstance(x,str) or x in [''])        
        if na_allowed == True:
             good_logic = data_table[header_name].apply(lambda x: isinstance(x,(int,float)) or x in ['N/A',''])
             error_msg = error_str + ".  Value must be a string or N/A"
             
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
         self.update_error_table(neg_msg,neg_values,sheet_name,header_name,neg_error_msg)
         self.update_error_table(pos_msg,pos_values,sheet_name,header_name,pos_error_msg)
    def get_missing_values(self,pd,sheet_name,data_table,header_name,Required_column):
        missing_data = data_table.query("{0} == '' ".format(header_name))
        if len(missing_data) > 0:
            if Required_column == "Yes":
                error_msg = "Missing Values are not allowed for this column.  Please recheck data"
                self.update_error_table("Error",missing_data,sheet_name,header_name,error_msg)
            elif Required_column == "No":
                error_msg = "Missing Values where found, this is a warning.  Please recheck data"
                self.update_error_table("Warning",missing_data,sheet_name,header_name,error_msg)
                
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
            self.add_error_values("Error","Cross_Participant_ID.csv",-3,field_name,check_id_only.iloc[iterZ][field_name],error_msg)
        self.sort_and_drop()
##########################################################################################################################
    def get_cross_sheet_Participant_ID(self,re,merged_data,valid_cbc,field_name):
        merged_data = merged_data[merged_data.isna().any(axis=1)]
        merged_data = merged_data[merged_data[field_name].apply(lambda x : (re.compile('^' + valid_cbc + '[_]{1}[0-9]{6}$').match(x) is not None))]
        if len(merged_data) > 0:
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
##########################################################################################################################
    def write_error_file(self,pd_s3,s3_client):
        key_name, separator, after = self.Submission_Location_Path.rpartition('/')
        key_name = key_name[(len(self.Bucket_Name)+1):]
        uni_name = list(set(self.Error_list["CSV_Sheet_Name"]))
        for iterU in uni_name:
            curr_table = self.Error_list.query("CSV_Sheet_Name == @iterU")
            curr_name = iterU.replace('.csv','_Errors.csv')
            curr_key = key_name + "/Data_Validation_Results/" + curr_name
            curr_table.sort_index()
            pd_s3.put_df(s3_client,curr_table,self.Bucket_Name,curr_key,format = "csv")         
            print(iterU +  " has " + str(len(curr_table)) + " Errors")
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