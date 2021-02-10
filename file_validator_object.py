import difflib                                  #libraray to compare strings to determine spelling issues
from dateutil.parser import parse               #library to check if a value is a date type
import csv                                      #csv to read and write csv files
import icd10                                    #library that checks if a value is a valid ICD10 code
from io import StringIO
##########################################################################################################################
def get_mysql_queries(file_dbname,conn,index,pd):
    if index == 1:  ## pulls positive and negative participant ids from database for validation purposes
        sql_querry = ("SELECT %s FROM `Participant_Prior_Test_Result` Where Test_Name = %s and `Test_Result` = %s;")
        pos_participants = pd.read_sql(sql_querry, conn,params=["Research_Participant_ID","SARS_Cov_2_PCR",'Positive'])
        neg_participants = pd.read_sql(sql_querry, conn,params=["Research_Participant_ID","SARS_Cov_2_PCR",'Negative'])
        return pos_participants,neg_participants
    elif index == 2:
        bio_ids = "SELECT %s,%s FROM Biospecimen;"
        bio_ids = pd.read_sql(bio_ids, conn, params=['Biospecimen_ID','Biospecimen_Type'])
        return bio_ids
    elif index == 3:
        assay_ids = "SELECT %s FROM Assay;"
        assay_ids = pd.read_sql(assay_ids, conn, params=['Assay_ID'])
        return assay_ids
##########################################################################################################################
def split_participant_pos_neg_prior(file_object,pos_list,neg_list,pd):
    test_data = file_object.Data_Table[['Research_Participant_ID','SARS_CoV_2_PCR_Test_Result']]
    pos_list = pd.concat([pos_list,test_data[test_data['SARS_CoV_2_PCR_Test_Result'] == 'Positive']['Research_Participant_ID'].to_frame()])
    pos_list.drop_duplicates(inplace=True)

    neg_list = pd.concat([neg_list,test_data[test_data['SARS_CoV_2_PCR_Test_Result'] == 'Negative']['Research_Participant_ID'].to_frame()])
    neg_list.drop_duplicates(inplace=True)
    return pos_list,neg_list
##########################################################################################################################
def check_for_spelling_error(list_of_valid_file_names,uni_id):
    overall_match = 0
    for valid in list_of_valid_file_names:
       sequence = difflib.SequenceMatcher(isjunk = None,a = uni_id,b = valid)
       difference = sequence.ratio()*100
       if overall_match < difference:           #value of 100 is a perfect match
           overall_match = difference
    return overall_match
##########################################################################################################################
def string_logic_check(test_value):
    try:
        float(test_value)
        return False
    except Exception:
        try:
            parse(test_value, fuzzy=False)      #value is a date, return error
            return False
        except Exception:
            return True
##########################################################################################################################
def getKey(item):       #sorts the outputed data by the 3rd column (row value)
    return item[2]
##########################################################################################################################
class Submitted_file:
    def __init__(self, file_name,ID_col_name):         #initalizes the Object
        """Initailize the Object and assigns some inital parameters."""
        self.File_name = file_name                     #Name of the file
        self.ID_column_name = ID_col_name              #list of Primary Keys in the file
        self.header_name_validation = [['CSV_Sheet_Name','Column_Value','Error_message']]   #error list header for column name errors
##########################################################################################################################
    def set_error_list_header(self):    #initalizes header list for sheet wise errors
        if isinstance(self.ID_column_name,str):
            self.error_list_summary = [['Message_Type','CSV_Sheet_Name','Row_Number',self.ID_column_name,'Column_Name','Column_Value','Error_message']]
        else:
            list_1  = ['Message_Type','CSV_Sheet_Name','Row_Number']
            list_2 = ['Column_Name','Column_Value','Error_message']
            self.error_list_summary = [list_1 +  self.ID_column_name + list_2]
##########################################################################################################################
    def load_csv_file(self,s3_client,output_bucket_name,test_name,pd):  #retrieve file from S3 bucket and call load function
        csv_obj = s3_client.get_object(Bucket=output_bucket_name, Key=test_name)
        csv_string = csv_obj['Body'].read().decode('utf-8')
        load_string_name = StringIO(csv_string)
        self.get_csv_table(load_string_name,pd)
##########################################################################################################################
    def get_csv_table(self,file_name,pd):           #loads the data from a file into a pandas data frame
        self.Data_Table = pd.read_csv(file_name,encoding='utf-8',na_filter = False)           #flag to keep N/A values as string
        self.Data_Table.dropna(axis=0, how="all", thresh=None, subset=None, inplace=True)     #if a row is completely blank remove it
        self.Data_Table = self.Data_Table.loc[:,~ self.Data_Table.columns.str.startswith('Unnamed')]    #if a column is completely blank, remove it
        self.Column_Header_List = list(self.Data_Table.columns.values)                        #parse header of dataframe into a list

        blank_logic = [sum(self.Data_Table.iloc[i] == '') < len(self.Column_Header_List) for i in self.Data_Table.index]
        self.Data_Table = self.Data_Table[blank_logic]

        self.file_size = len(self.Data_Table)   #number of rows in the file
        self.set_error_list_header()
##########################################################################################################################
    def get_list_of_passing_IDS(self,Column_Name):  #used for demographic and biospecimen files
        list_of_submitted_ids = self.Data_Table[Column_Name].tolist()
        id_error_list = [i[5] for i in self.error_list_summary if (i[0] == "Error") and (i[4] == Column_Name)]
        list_of_valid_ids = [uni_id for uni_id in list_of_submitted_ids if uni_id not in id_error_list]
        return list_of_valid_ids
##########################################################################################################################
    def remove_unknown_sars_results_v2(self): #if a Participant has a SARS_Cov2_PCR test that is not positive or negative, write an error
        unk_sars_result = self.Data_Table[~(self.neg_list_logic | self.pos_list_logic)]
        for index in unk_sars_result.iterrows():
            self.error_list_summary.append(['Error',self.File_name,index[0]+2,unk_sars_result['Research_Participant_ID'][index[0]]," ",
                                            " ","Unknown Prior SARS_CoV-2 test, not valid Participant"])
        self.Data_Table = self.Data_Table[(self.neg_list_logic | self.pos_list_logic)]          #Filter table
        self.pos_list_logic = self.pos_list_logic[(self.neg_list_logic | self.pos_list_logic)]  #Update the positive logical vector
        self.neg_list_logic = self.neg_list_logic[(self.neg_list_logic | self.pos_list_logic)]  #Update the negative logical vector
###############################################################################
    def check_data_type(self,test_column):     ## logical vectors to see if there is data or missing values
        missing_logic = [(len(str(i)) == 0)  for i in test_column]
        has_logic = [(len(str(i)) > 0)  for i in test_column]
        
        has_data = test_column[has_logic]
        has_pos_data = test_column[(self.pos_list_logic & has_logic).tolist()]
        has_neg_data = test_column[(self.neg_list_logic & has_logic).tolist()]
        missing_data = test_column[missing_logic]
        missing_pos_data = test_column[(self.pos_list_logic & missing_logic).tolist()]
        missing_neg_data = test_column[(self.neg_list_logic & missing_logic).tolist()]     
        return has_data,has_pos_data,has_neg_data,missing_data,missing_pos_data,missing_neg_data
##########################################################################################################################
    def get_pos_neg_logic(self,pos_list,neg_list):  #logical vector for SARS_CoV_2_PCR_Test_Result Positive or Negative Participants
        self.pos_list_logic = self.Data_Table['Research_Participant_ID'].isin(pos_list['Research_Participant_ID']) #logic vector for positive Participants
        self.neg_list_logic = self.Data_Table['Research_Participant_ID'].isin(neg_list['Research_Participant_ID']) #logic vector for negative Participants
##########################################################################################################################
    def write_error_msg(self,test_value,column_name,error_msg,row_number,error_stat):  #updates the error message variable
        if isinstance(self.ID_column_name,str):
            self.error_list_summary.append([error_stat,self.File_name,row_number+2,self.Data_Table[self.ID_column_name][row_number],column_name,test_value,error_msg])
        else:
            error_list = [error_stat,self.File_name,row_number+2]
            for i in enumerate(self.ID_column_name):
                error_list = error_list + [self.Data_Table[i[1]][row_number]]
            error_list = error_list + [column_name,test_value,error_msg]
            self.error_list_summary.append(error_list)
##########################################################################################################################
    def in_list(self,column_name,test_value,check_str_list,error_msg,row_number,error_stat):           #writes error if test_value not in check_str_list
        try:
            check_str_list.index(test_value)
        except ValueError:
            self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
##########################################################################################################################
    def valid_ID(self,column_name,test_value,pattern,valid_cbc_id,error_msg,row_number,error_stat):    #see if the ID variable has valid format
        res = (pattern.match(test_value))
        current_cbc = valid_cbc_id
        try:
            if res.string == test_value:
                if int(test_value[:2]) != int(current_cbc):
                    error_msg = "two digit code on this ID does not match submission (expected CBC code: " + str(current_cbc) + ")"
                    self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
        except Exception:
            self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
##########################################################################################################################
    def is_numeric(self,column_name,na_allowed,test_value,lower_lim,error_msg,row_number,error_stat):   #writes error if test_value is not a number or N/A if allowed
        if (na_allowed == True):
             if (test_value =='N/A'):
                 test_value = 10000
        try:
            if float(test_value) > lower_lim:
                pass
            else:
                self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
        except ValueError:
            self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
##########################################################################################################################
    def is_date_time(self,column_name,test_value,na_allowed,error_msg,row_number,error_stat):           #writes an error if test_value is not a date or time, or N/A if allowed
        if (na_allowed == True) & (test_value == 'N/A'):
            pass
        else:
            try:
                parse(test_value, fuzzy=False)
            except ValueError:
                self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
##########################################################################################################################
    def is_string(self,column_name,test_value,na_allowed,error_msg,row_number,error_stat):              #see if the value is a string (check for initals)
        if (na_allowed == True) & ((test_value != test_value) | (test_value == "N/A")):       #value is N/A
            pass
        elif (na_allowed == False) & ((test_value != test_value) | (test_value == "N/A")):    #value is N/A
            self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
        else:
            logic_check = string_logic_check(test_value)
            if logic_check == False:
                self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
##########################################################################################################################
    def check_icd10(self,column_name,test_value,error_msg,row_number,error_stat):  #checks if test_value is valid ICD10 code, writes error if not
        if icd10.exists(test_value):
            pass
        elif (test_value != test_value) or (test_value == "N/A"):
            pass
        else:
            self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
##########################################################################################################################
    def pos_neg_errors(self,pos_list,neg_list,has_pos_data,has_neg_data,header_name):
        error_msg = "Participant is SARS_CoV2 Positive. Value must be: " + str(pos_list)
        for i in range(len(has_pos_data)):
            self.in_list(header_name,has_pos_data.values[i],pos_list,error_msg,has_pos_data.index[i],'Error')
    
        error_msg = "Participant is SARS_CoV2 Negative. Value must be: " + str(neg_list)
        for i in range(len(has_neg_data)):
            self.in_list(header_name,has_neg_data.values[i],neg_list,error_msg,has_neg_data.index[i],'Error') 
    def current_infection_check(self,current_index,check_val,header_name):
        if "Yes" in check_val:
            error_msg = "Participant has " + current_index + " set to Yes. Duration must be a value of 0 or greater"
        else:
            error_msg = "Participant has " + current_index + " set to ['No','Unknown','N/A']. Duration must be N/A"
        
        test_value = self.Data_Table.iloc[[i[0] for i in enumerate(self.Data_Table[current_index]) if i[1] in check_val]][header_name]
        for i in range(len(test_value)):
            if "Yes" in check_val:
                self.is_numeric(header_name,False,test_value.values[i],0,error_msg,test_value.index[i],'Error')
            else:
                self.in_list(header_name,test_value.values[i],["N/A"],error_msg,test_value.index[i],'Error')
    def get_duration_logic(self,test_string,list_values,error_message,error_stat):
        for i in range(len(test_string)):
            prior_valid_object.in_list(header_name,test_string.values[i],list_values,error_msg,test_string.index[i],error_stat)
    def biospeimen_type_wrong(self,bio_type,header_name):
        test_value = self.Data_Table[self.Data_Table['Biospecimen_Type'] != bio_type][header_name]
        for i in range(len(test_value)):
            if test_value.values[i] == "":  #data is blank
                error_msg = "Blank values were found, expected value to be N/A"
                self.write_error_msg(test_value.values[i],header_name,error_msg,test_value.index[i],'Warning')
            else:
                error_msg = "Biospecimen Type is not "+ bio_type + ", unexpected value found, expected value to be N/A"
                self.in_list(header_name,test_value.values[i],['N/A'],error_msg,test_value.index[i],'Warning')
    def biospecimen_count_check(self,Count):
        live_count = self.Data_Table[self.Data_Table['Biospecimen_Type'] == "PBMC"]["Live_Cells_" + Count]
        total_count = self.Data_Table[self.Data_Table['Biospecimen_Type'] == "PBMC"]["Total_Cells_" + Count]
        viabilty_count = self.Data_Table[self.Data_Table['Biospecimen_Type'] == "PBMC"]["Viability_" + Count]
        file_index = self.Data_Table[self.Data_Table['Biospecimen_Type'] == "PBMC"].index
        
        for iterI in file_index:
            if (str(live_count[iterI]).isdigit()) and (str(total_count[iterI]).isdigit()) and (str(viabilty_count[iterI]).isdigit()):
                if int(live_count[iterI]) > int(total_count[iterI]):
                    error_msg = "Total cell counts must be greater then Live cell counts( " + str(live_count[iterI]) +")"
                    self.write_error_msg(total_count[iterI],"Total_Cells_" + Count,error_msg,iterI,'Error')
                percent_check = round((float(live_count[iterI]) / float(total_count[iterI]))*100,1)
                if percent_check != round(float(viabilty_count[iterI]),1):
                     error_msg = "Percentage of Live cell counts to total cell counts (" + str(percent_check) + ") needs to equal viability counts"
                     self.write_error_msg(viabilty_count[iterI],"Viability_Cells_" + Count,error_msg,iterI,'Error')
##########################################################################################################################
    def get_in_list_logic(self,header_name,pos_list,neg_list,has_pos_data,has_neg_data):
        pos_error = "Participant is SARS_CoV-2 Positive.  Value must be one of the following: " + str(pos_list)
        neg_error = "Participant is SARS_CoV-2 Negative.  Value must be one of the following: " + str(neg_list)
        for i in range(len(has_pos_data)):
            self.in_list(header_name,has_pos_data.values[i],pos_list,pos_error,has_pos_data.index[i],'Error')
        for i in range(len(has_neg_data)):
            self.in_list(header_name,has_neg_data.values[i],neg_list,neg_error,has_neg_data.index[i],'Error')
##########################################################################################################################
    def particpant_no_symtpoms(self,header_name,test_string):
        error_msg = "Participant does not have symptomns (" + test_string + " == 'No or N/A'), value must be N/A"        
        test_value  =self.Data_Table.iloc[[i[0] for i in enumerate(self.Data_Table[test_string]) if i[1] in ["No","N/A"]]][header_name]
        
        for i in range(len(test_value)):
            self.in_list(header_name,test_value.values[i],["N/A"],error_msg,test_value.index[i],'Error')
        error_msg = "Participant has unknown value for " + test_string + ", Unable to validate value"
        test_value  =self.Data_Table.iloc[[i[0] for i in enumerate(self.Data_Table[test_string]) if i[1] not in ["Yes","No","N/A"]]][header_name]     
        
        for i in range(len(test_value)):
            self.write_error_msg(test_value.values[i],header_name,error_msg,test_value.index[i],'Error')
##########################################################################################################################
    def is_required(self,column_name,test_value,req_type,row_number,error_stat):        #creates error and warning messages
        if error_stat == 'Error':
            if req_type.lower() == 'all':
                error_msg = "Missing Values are not allowed for this column, please check data"
            elif req_type.lower() == "sars_pos":
                error_msg = "Missing Values are not allowed for SARS_CoV-2 Positive Participants, please check data"
            elif req_type.lower() == "sars_neg":
                error_msg = "Missing Vales are not allowed for SARS_CoV-2 Negative Participants, please check data"
            else:
                error_msg = " ";
        elif error_stat == 'Warning':
            if req_type.lower() == 'all':
                error_msg = "Missing Values were found, please check data"
            elif req_type.lower() == "sars_pos":
                error_msg = "Missing Values were found for SARS_CoV-2 Positive Participants, please check data"
            elif req_type.lower() == "sars_neg":
                error_msg = "Missing Vales were found for SARS_CoV-2 Negative Participants, please check data"
            else:
                error_msg = " ";
        self.write_error_msg(test_value,column_name,error_msg,row_number,error_stat)
    def missing_data_errors(self,Required_column,header_name,missing_data,missing_pos_data,missing_neg_data):
        if Required_column == "Yes":
            for i in range(len(missing_data)):
                self.is_required(header_name,missing_data.values[i],"All",missing_data.index[i],'Error')
        elif Required_column == "Yes: SARS-Negative":
            self.check_required(missing_pos_data,missing_neg_data,header_name,"Error","Warning")
        elif Required_column == "Yes: SARS-Positive":
            self.check_required(missing_pos_data,missing_neg_data,header_name,"Warning","Error")
        elif Required_column == "No":
            for i in range(len(missing_data)):
                self.is_required(header_name,missing_data.values[i],"All",missing_data.index[i],'Warning')
##########################################################################################################################
    def check_required(self,missing_pos_data,missing_neg_data,header_name,neg_status,pos_status):       #if column if only required for positive or negative Participants
        for i in range(len(missing_pos_data)):
            self.is_required(header_name,missing_pos_data.values[i],"sars_pos",missing_pos_data.index[i],pos_status)
        for i in range(len(missing_neg_data)):
            self.is_required(header_name,missing_neg_data.values[i],"sars_neg",missing_neg_data.index[i],neg_status)
##########################################################################################################################
    def write_error_file(self,file_name,s3_resource,temp_path,error_list,error_file):
        try:
            file_path = temp_path + "/" + file_name
            sort_list = sorted(self.error_list_summary[1:], key = getKey)
            self.error_list_summary = [self.error_list_summary[0]]
            error_count = len(sort_list)
            if error_count > 0:
                msg_status = list(zip(*sort_list))[0]
                err_cnt = msg_status.count('Error')
                warn_cnt = msg_status.count('Warning')
            else:
                err_cnt = 0
                warn_cnt = 0
    
            print(self.File_name + " containing " + str(self.file_size) + " rows has been checked for errors and warnings")
            print(str(err_cnt) + ' errors were found, and ' + str(warn_cnt) + ' warnings were found.')
    
            if (error_count + warn_cnt) > 0:
                print('A file has been created\n')
    
                with open(file_path, 'w', newline='') as csvfile:
                    submission_errors = csv.writer(csvfile, delimiter=',')
                    submission_errors.writerow(self.error_list_summary[0])
                    for file_indx in enumerate(sort_list):
                        if file_indx[1][0] == 'Error':
                            submission_errors.writerow(file_indx[1])
                    submission_errors.writerow([' ']*len(file_indx[1]))
                    for file_indx in enumerate(sort_list):
                        if file_indx[1][0] == 'Warning':
                            submission_errors.writerow(file_indx[1])
    
                s3_file_path = self.Error_dest_key + "/" + file_name
                s3_resource.meta.client.upload_file(file_path, self.File_Bucket, s3_file_path)
            else:
                print('\n')
        except Exception:
            print('An Error occurred while trying to write Error file')
            
            
      
        current_errors = (error_file,err_cnt)
        error_list.append(current_errors)
        return error_list