def Biospecimen_validator(current_object,prior_cov_test,demo_ids,re,pd,valid_cbc_ids):
    current_object.Data_Table = current_object.Data_Table.merge(prior_cov_test.drop_duplicates("Research_Participant_ID"),how='left',on="Research_Participant_ID")
    current_object.Data_Table = current_object.Data_Table.merge(demo_ids.drop_duplicates("Research_Participant_ID"),how='left',on="Research_Participant_ID")
    current_object.All_Error_DF = pd.DataFrame(columns=current_object.error_list_summary)
    pbmc_data = current_object.Data_Table[current_object.Data_Table['Biospecimen_Type'] == "PBMC"]
    serum_data = current_object.Data_Table[current_object.Data_Table['Biospecimen_Type'] == "Serum"] 
    for header_name in current_object.Column_Header_List:
        Required_column = "Yes"
        if (header_name.find('Automated_Count') > -1):
            Required_column = "No"
#################################################################################################################################################
        if 'Research_Participant_ID' in header_name:
            pattern_str = '[_]{1}[0-9]{6}$'
            current_object.check_id_field(pd,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX",True)
            current_object.check_id_cross_sheet(pd,header_name,"Age","biospecimen","demographic")
#################################################################################################################################################
        elif (header_name in ["Biospecimen_ID"]):
            pattern_str = '[_]{1}[0-9]{6}[_]{1}[0-9]{3}$$'
            current_object.check_id_field(pd,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX_XXX",False)
################################################################################################################################################
        elif(header_name in ["Biospecimen_Group"]):
            current_object.check_in_list(pd,header_name,[['Positive Sample'],['Negative Sample']])
#################################################################################################################################################
        elif(header_name in ["Biospecimen_Type"]):
            list_values = ["Serum", "EDTA Plasma", "PBMC", "Saliva", "Nasal swab"]
            current_object.check_in_list(pd,header_name,[list_values])
#################################################################################################################################################
        elif 'Time_of' in header_name:
            Error_Message = "Value must be a valid time in 24hour format HH:MM" 
            current_object.check_date(pd,current_object.Data_Table,header_name,False,Error_Message)
#################################################################################################################################################
        elif ('Date_of' in header_name) or ('Expiration_Date' in header_name):
            Error_Message = "Value must be a valid Date MM/DD/YYYY"
            current_object.check_date(pd,current_object.Data_Table,header_name,False,Error_Message)
#################################################################################################################################################
        elif(header_name in ["Storage_Time_at_2_8"]):
            error_msg = "Value must be a positive number or N/A"
            current_object.check_if_number(pd,current_object.Data_Table,header_name,1,1000,"All",True,Error_Message)
#################################################################################################################################################
        elif(header_name in ["Storage_Start_Time_at_2_8_Initials","Storage_End_Time_at_2_8_Initials"]):
            current_object.check_storage_time(pd,header_name,"Initials")
#################################################################################################################################################
        elif(header_name in ["Storage_Start_Time_at_2_8","Storage_End_Time_at_2_8"]):
             current_object.check_storage_time(pd,header_name,"Date")
#################################################################################################################################################
        elif ((header_name.find('Company_Clinic') > -1) or (header_name.find('Initials') > -1) or (header_name.find('Collection_Tube_Type') > -1)):
            Error_Message = "Value must be a string and NOT N/A"
            current_object.check_if_str(pd,current_object.Data_Table,header_name,Error_Message)
#################################################################################################################################################
        elif(header_name in ["Final_Concentration_of_Biospecimen"]) or (header_name.find('Hemocytometer_Count') > -1) or (header_name.find('Automated_Count') > -1):
            Error_Message = "Biospecimen Type == PBMC, Value must be a positive number"
            current_object.check_if_number(pd,pbmc_data,header_name,1,1e9,"All",True,Error_Message)            
#################################################################################################################################################
        elif(header_name in ["Centrifugation_Time","RT_Serum_Clotting_Time"]):
            Error_Message = "Biospecimen Type == Serum, Value must be a positive number"
            current_object.check_if_number(pd,serum_data,header_name,1,1e9,"All",True,Error_Message)
#################################################################################################################################################
        elif(header_name in ["Storage_Start_Time_80_LN2_storage"]):
            Error_Message = "Biospecimen Type == Serum, Value must be a Time in 24hour format HH:MM"
            current_object.check_date(pd,serum_data,header_name,False,Error_Message)
#################################################################################################################################################
        elif(header_name in ["Initial_Volume_of_Biospecimen"]):
            Error_Message = "Value must be a number greater than 0"
            current_object.check_if_number(pd,current_object.Data_Table,header_name,1,1000,"All",False,Error_Message)
#################################################################################################################################################
        current_object.get_missing_values(pd,header_name,Required_column)
#################################################################################################################################################
    current_object.biospecimen_count_check(pd,'Hemocytometer_Count',pbmc_data)
    current_object.biospecimen_count_check(pd,'Automated_Count',pbmc_data)
###############################################################################################################################
    id_compare = current_object.Data_Table[current_object.Data_Table.apply(lambda x: x["Research_Participant_ID"] not in x["Biospecimen_ID"],axis = 1)]
    Error_Message = "Research_Participant_ID is not a substring of Biospecimen ID.  Data is not Valid, please check data"
    current_object.get_duration_errors(pd,id_compare,'Research_Participant_ID',Error_Message)
    return current_object