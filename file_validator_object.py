from dateutil.parser import parse               #library to check if a value is a date type
import csv                                      #csv to read and write csv files
import icd10                                    #library that checks if a value is a valid ICD10 code
from io import StringIO
from collections import Counter
##########################################################################################################################
def add_error_str(data_table,data_field,error_list):
    for i in data_table.index:  
        data_table.at[i,data_field] = error_list
    return data_table
##########################################################################################################################
def combine_tests(pd,pos_data,neg_data,ukn_data,pos_str,neg_str,ukn_str,pos_status,neg_status,ukn_status):
    pos_data = add_error_str(pos_data,"Error_Message",pos_str)
    pos_data = add_error_str(pos_data,"Message_Type",pos_status)
    neg_data = add_error_str(neg_data,"Error_Message",neg_str)
    neg_data = add_error_str(neg_data,"Message_Type",neg_status)
    ukn_data = add_error_str(ukn_data,"Error_Message",ukn_str)
    ukn_data = add_error_str(ukn_data,"Message_Type",ukn_status)
    
    missing_data = pd.concat([pos_data,neg_data,ukn_data])
    return missing_data
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
            self.error_list_summary = ["Message_Type",self.ID_column_name,"SARS_CoV_2_PCR_Test_Result","Column_Name","Column_Value","Error_Message"]
        else:
            list_1  = ["Message_Type"]
            list_2 = ["SARS_CoV_2_PCR_Test_Result","Column_Name","Column_Value","Error_Message"]
            self.error_list_summary = list_1 +  self.ID_column_name + list_2
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
    def get_missing_values(self,pd,header_name,Required_column):
        missing_data = self.Data_Table[self.Data_Table[header_name].apply(lambda x : x == "")]
        if len(missing_data) > 0:
            missing_data = self.filter_data_table(missing_data,header_name,pd,"Missing")
            
            pos_data = missing_data[missing_data['SARS_CoV_2_PCR_Test_Result'] == "Positive"]
            neg_data = missing_data[missing_data['SARS_CoV_2_PCR_Test_Result'] == "Negative"]
            ukn_data = missing_data[missing_data['SARS_CoV_2_PCR_Test_Result'].apply(lambda x: x not in ['Positive','Negative'])]
        
            if Required_column == "Yes":
               missing_data = add_error_str(missing_data,"Error_Message","Missing Values are not allowed for this column.  Please recheck data")
               missing_data = add_error_str(missing_data,"Message_Type","Error")
            elif Required_column == "No":
                missing_data = add_error_str(missing_data,"Error_Message","Missing Values where found, this is a warning.  Please recheck data")
                missing_data = add_error_str(missing_data,"Message_Type","Warning")
            elif Required_column == "Yes: SARS-Positive":
                pos_str = "Participant is SARS_CoV-2 Positive.  Missing Values are not allowed. Please check data"
                neg_str = "Participant is SARS_CoV-2 Negative.  Missing Value was found, this is a warning. Please check data"
                ukn_str = "Participant is has unknown SARS_CoV-2 Result.  Missing Value was found, this is a warning. Please check data"
                missing_data = combine_tests(pd,pos_data,neg_data,ukn_data,pos_str,neg_str,ukn_str,"Error","Warning","Warning")
            elif Required_column == "Yes: SARS-Negative":
                pos_str = "Participant is SARS_CoV-2 Positive.  Missing Value was found, this is a warning. Please check data"
                neg_str = "Participant is SARS_CoV-2 Negative.  Missing Values are not allowed. Please check data"
                ukn_str = "Participant is has unknown SARS_CoV-2 Result.  Missing Value was found, this is a warning. Please check data"
                missing_data = combine_tests(pd,pos_data,neg_data,ukn_data,pos_str,neg_str,ukn_str,"Warning","Error","Warning")
            self.All_Error_DF = pd.concat([self.All_Error_DF,missing_data])
##########################################################################################################################
    def filter_data_table(self,data_table,field_name,pd,data_type):
        if data_type == "Missing":
            data_table  = data_table[self.ID_column_name+["SARS_CoV_2_PCR_Test_Result",field_name]][data_table[field_name].apply(lambda x : x == "")]
        elif data_type == "Has_Data":
            data_table  = data_table[self.ID_column_name+["SARS_CoV_2_PCR_Test_Result",field_name]][data_table[field_name].apply(lambda x : x != "")]
        
        data_table.set_axis([*data_table.columns[:-1], "Column_Value"], axis=1, inplace=True)
        data_table = pd.concat([data_table,pd.DataFrame(
            [[field_name, '', '']],index=data_table.index,columns=['Column_Name','Error_Message', 'Message_Type'])], axis=1)
        
        data_table = data_table[data_table.columns[[5,0,1,3,2,4]]]
        return data_table
##########################################################################################################################
    def check_id_field(self,pd,re,field_name,pattern_str,valid_cbc_ids,pattern_error):
        data_table = self.filter_data_table(self.Data_Table,field_name,pd,"Has_Data")
          
        ID_table = pd.DataFrame(data_table.pivot_table(index=['Column_Value'], aggfunc='size'))
        ID_table.reset_index(inplace=True)
        ID_table.rename(columns = {0:"Count_Freq"}, inplace = True)
        
        single_count = data_table.merge(ID_table[ID_table["Count_Freq"] == 1],how="right",on = "Column_Value")
        dup_id_count = data_table.merge(ID_table[ID_table["Count_Freq"] >  1],how="right",on = "Column_Value")

        single_invalid = single_count[single_count[field_name].apply(lambda x : re.compile('^[0-9]{2}' + pattern_str).match(x) is None)]
        wrong_cbc_id   = single_count[single_count[field_name].apply(lambda x : (re.compile('^' + valid_cbc_ids + pattern_str).match(x) is None) and
                                                                     x not in single_invalid['Research_Participant_ID'].tolist())]
        
        for i in dup_id_count.index:
            dup_id_count.loc[i,"Error_Message"] = "Id is repeated " + str(dup_id_count.loc[i,"Count_Freq"]) + " times, Multiple repeats are not allowed"
        dup_id_count.drop_duplicates(inplace = True)

        single_invalid = add_error_str(single_invalid,"Error_Message","ID is Not Valid Format, Expecting " + pattern_error)
        wrong_cbc_id  = add_error_str(wrong_cbc_id,"Error_Message","ID is Valid however has wrong CBC code. Expecting CBC Code (" + str(valid_cbc_ids) + ")")
    
        all_id_errors = pd.concat([dup_id_count,single_invalid,wrong_cbc_id])
        all_id_errors  = add_error_str(all_id_errors,"Message_Type","Error")        
        
        all_id_errors.drop(["Count_Freq"], axis=1,inplace = True)
        self.All_Error_DF = pd.concat([self.All_Error_DF,all_id_errors])
##########################################################################################################################
    def add_error_values(self,pd,data_table,error_message,message_type):
        data_table = add_error_str(data_table,'Error_Message',error_message)
        data_table = add_error_str(data_table,'Message_Type',message_type)
        self.All_Error_DF = pd.concat([self.All_Error_DF,data_table])  
##########################################################################################################################
    def check_id_cross_sheet(self,pd,field_name,second_field,sheet_1,sheet_2):
        not_in_both = self.filter_data_table(self.Data_Table,field_name,pd,"Has_Data")
        not_in_both = not_in_both[not_in_both.isnull().any(axis=1)]
       
        Error_Message = "ID is found in " + sheet_1 + " but no matching ID in " + sheet_2 + " (or database)"
        self.add_error_values(pd,not_in_both,Error_Message,"Error")
##########################################################################################################################
    def check_in_list(self,pd,header_name,list_values):
        data_table = self.filter_data_table(self.Data_Table,header_name,pd,"Has_Data")
        if len(list_values) == 1:
            list_values = list_values[0]
            data_table = data_table[data_table["Column_Value"].apply(lambda x : x not in list_values)]
            Error_Message = "Unexpected Value.  Value must be one of the following " + str(list_values)
            self.add_error_values(pd,data_table,Error_Message,"Error")
        elif len(list_values) == 2:
            pos_data_table = data_table[data_table.apply(lambda x: (x['SARS_CoV_2_PCR_Test_Result'] in ['Positive']) 
                                                         and (x["Column_Value"] not in list_values[0]),axis=1)]
            neg_data_table = data_table[data_table.apply(lambda x: (x['SARS_CoV_2_PCR_Test_Result'] in ['Negative']) 
                                                         and (x["Column_Value"] not in list_values[1]),axis=1)]
            ukn_data_table = data_table[data_table.apply(lambda x: (x['SARS_CoV_2_PCR_Test_Result'] not in ['Positive','Negative']),axis=1)]
            
            Error_Message = "Unexpected Value Found. Participant is SARS_CoV-2 Positive.  Value must be one of the following " + str(list_values[0])
            self.add_error_values(pd,pos_data_table,Error_Message,"Error")
            Error_Message = "Unexpected Value Found. Participant is SARS_CoV-2 Negative.  Value must be one of the following " + str(list_values[1])
            self.add_error_values(pd,neg_data_table,Error_Message,"Error")
            Error_Message = "Participant has Unknown/Missing Prior SARS_CoV-2 Test Result.  Unable to Validate, please check data"
            self.add_error_values(pd,ukn_data_table,Error_Message,"Warning")
##########################################################################################################################
    def check_date(self,pd,header_name,na_allowed,Error_Message):
        data_table = self.filter_data_table(self.Data_Table,header_name,pd,"Has_Data")
        data_logic = data_table["Column_Value"].apply(lambda x: pd.to_datetime([x, 'asd'], errors="coerce"))
        nan_data   = data_table["Column_Value"].apply(lambda x: x != "N/A")
        logic_check = [i[0] != i[0] for i in data_logic]     #NaT is same as NaN where is defined by X != X

        if na_allowed:
             logic_check = [(x and y) for (x, y) in zip(logic_check, nan_data)]
        
        self.add_error_values(pd,data_table[logic_check],Error_Message,"Error")
##########################################################################################################################
    def get_duration_errors(self,pd,data_table,header_name,Error_Message):
        if len(data_table) > 0:
            data_table = self.filter_data_table(data_table,header_name,pd,"Has_Data")
            self.add_error_values(pd,data_table,Error_Message,"Error")
          
    def get_duration_unit(self,pd,duration_name,header_name,number_list):
        data_table = self.Data_Table
        number_check = data_table[data_table.apply(lambda x: (x[duration_name].isdigit()) > 0 and (x[header_name] not in number_list),axis=1)]       
        Error_Message = duration_name + " has a numerical value.  Unit Value must be in " + str(number_list)
        self.get_duration_errors(pd,number_check,header_name,Error_Message)

        nan_check = data_table[data_table.apply(lambda x: (x[duration_name] == "N/A") and (x[header_name] != "N/A"),axis=1)]        
        Error_Message = duration_name + " has a value of N/A.  Unit Value must be also be N/A"
        self.get_duration_errors(pd,nan_check,header_name,Error_Message)

        number_check = data_table[data_table.apply(lambda x: (x[duration_name].isdigit()) <= 0 or (x[header_name] not in ["N/A"]),axis=1)]       
        Error_Message = duration_name + " has an invlaid value, unable to Validate, please check data"
        self.get_duration_errors(pd,number_check,header_name,Error_Message)        
##########################################################################################################################
    def get_duration_check(self,pd,current_name,header_name):
        data_table = self.Data_Table
        number_check = data_table[data_table.apply(lambda x: (x[current_name] in ["Yes"]) and not (x[header_name].isdigit() > 0),axis=1)]
        Error_Message = current_name + " has a value of Yes.  Unit Value must be a number greater then 0"
        self.get_duration_errors(pd,number_check,header_name,Error_Message)
    
        nan_check = data_table[data_table.apply(lambda x: (x[current_name] in ['No','Unknown','N/A']) and (x[header_name] != "N/A"),axis=1)]
        Error_Message = current_name + " has a value of N/A.  Unit Value must be also be N/A"
        self.get_duration_errors(pd,nan_check,header_name,Error_Message) 

        nan_check = data_table[data_table.apply(lambda x: (x[current_name] not in ['Yes','No','Unknown','N/A']),axis=1)]
        Error_Message = current_name + " has an invlaid value, unable to Validate, please check data"
        self.get_duration_errors(pd,nan_check,header_name,Error_Message)         
##########################################################################################################################
    def write_error_file(self,file_name,s3_resource,temp_path,error_list,error_file):
        try:
            file_path = temp_path + "/" + file_name 
            
            error_count = len(self.All_Error_DF[self.All_Error_DF['Message_Type'] == "Error"])
            warn_cnt = len(self.All_Error_DF[self.All_Error_DF['Message_Type'] == "Warning"])
    
            if self.file_size > 0:
                print(self.File_name + " containing " + str(self.file_size) + " rows has been checked for errors and warnings")
            else:
                print(self.File_name + "rules have been checked for errors and warnings")
            print(str(error_count) + ' errors were found, and ' + str(warn_cnt) + ' warnings were found.')
    
            if (error_count + warn_cnt) > 0:
                print('A file has been created\n')
     
            self.All_Error_DF.to_csv(file_path, sep=',', header=True, index=False)
            s3_file_path = self.Error_dest_key + "/" + file_name
            s3_resource.meta.client.upload_file(file_path, self.File_Bucket, s3_file_path)
            return (error_count + warn_cnt)
        except Exception as e:
            print(e)
            print('An Error occurred while trying to write Error file')
            return 0