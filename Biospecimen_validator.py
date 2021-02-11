def Biospecimen_validator(Biospecimen_object,neg_list,pos_list,re,valid_cbc_ids,current_demo):
    Biospecimen_object.get_pos_neg_logic(pos_list,neg_list)
    Biospecimen_object.remove_unknown_sars_results_v2()
    for header_name in Biospecimen_object.Column_Header_List:
        test_column = Biospecimen_object.Data_Table[header_name]
        has_data,has_pos_data,has_neg_data,missing_data,missing_pos_data,missing_neg_data = Biospecimen_object.check_data_type(test_column)
        Required_column = "Yes"
        if (header_name.find('Automated_Count') > -1):
            Required_column = "No"
#################################################################################################################################################
        if 'Research_Participant_ID' in header_name:
            error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX"
            pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}$')
            for i in range(len(has_data)):
                Biospecimen_object.valid_ID(header_name,has_data.values[i],pattern,valid_cbc_ids,error_msg,has_data.index[i],'Error')
            Biospecimen_object.get_participant_againt_list(has_data,pattern,current_demo,header_name)
#################################################################################################################################################
        elif (header_name in ["Biospecimen_ID"]):
            error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX_XXX"
            pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}[_]{1}[0-9]{3}$')
            for i in range(len(has_data)):
                Biospecimen_object.valid_ID(header_name,has_data.values[i],pattern,valid_cbc_ids,error_msg,has_data.index[i],'Error')
################################################################################################################################################
        elif(header_name in ["Biospecimen_Group"]):
            pos_error_msg = "Participant is SARS_CoV2 Positive. Value must be: Positive Sample"
            neg_error_msg = "Participant is SARS_CoV2 Negative. Value must be: Negative Sample"
            for i in range(len(has_pos_data)):
                Biospecimen_object.in_list(header_name,has_pos_data.values[i],['Positive Sample'],pos_error_msg,has_pos_data.index[i],'Error')
            for i in range(len(has_neg_data)):
                Biospecimen_object.in_list(header_name,has_neg_data.values[i],['Negative Sample'],neg_error_msg,has_neg_data.index[i],'Error')
    #################################################################################################################################################
        elif(header_name in ["Biospecimen_Type"]):
            test_string = ["Serum", "EDTA Plasma", "PBMC", "Saliva", "Nasal swab"]
            error_msg = "Value must be: " + str(test_string)
            for i in range(len(has_data)):
                Biospecimen_object.in_list(header_name,has_data.values[i],test_string,error_msg,has_data.index[i],'Error')
#################################################################################################################################################
        elif ((header_name.find('Date_of') > -1) or (header_name.find('Expiration_Date') > -1) or (header_name.find('Time_of') > -1)):
            if (header_name.find('Time_of') > -1):
                error_msg = "Value must be a valid time in 24hour format HH:MM"
            else:
                error_msg = "Value must be a valid Date MM/DD/YYYY"
            for i in range(len(has_data)):
                Biospecimen_object.is_date_time(header_name,has_data.values[i],False,error_msg,has_data.index[i],'Error')
 #################################################################################################################################################
        elif(header_name in ["Storage_Time_at_2_8"]):
            error_msg = "Value must be a positive number or N/A"
            for i in range(len(has_data)):
                Biospecimen_object.is_numeric(header_name,True,has_data.values[i],0,error_msg,has_data.index[i],'Error')
#################################################################################################################################################
        elif(header_name in ["Storage_Start_Time_at_2_8","Storage_End_Time_at_2_8","Storage_Start_Time_at_2_8_Initials","Storage_End_Time_at_2_8_Initials"]):
            storage_2_8 = Biospecimen_object.Data_Table["Storage_Time_at_2_8"]
            is_a_number = [i[0] for i in enumerate(storage_2_8) if str(i[1]).isdigit()]
            is_a_na =     [i[0] for i in enumerate(storage_2_8) if i[1] in ['N/A']]
            is_unknown =  [i[0] for i in enumerate(storage_2_8) if not((i[1] in ['N/A']) or (str(i[1]).isdigit()))]
            
            for i in is_a_number:
                if(header_name.find('Initials') > -1):
                    error_msg = "Storage Time at 2_8 is a Number.  Value must be a string NOT N/A"
                    Biospecimen_object.is_string(header_name,test_column.iloc[i],False,error_msg,i,'Error')
                else:
                    error_msg = "Storage Time at 2_8 is a Number.  Value must be a datetime MM/DD/YYYY HH:MM"
                    Biospecimen_object.is_date_time(header_name,test_column.iloc[i],False,error_msg,i,'Error')
            for i in is_a_na:
                error_msg = "Storage Time at 2_8 is N/A.  Value must be N/A"
                Biospecimen_object.is_string(header_name,test_column.iloc[i],False,error_msg,i,'Error')
            for i in is_unknown:
                error_msg = "Storage Time at 2_8 is missing/Unknown.  Unable to Validate Column"
                Biospecimen_object.write_error_msg(test_column.iloc[i],header_name,error_msg,i,'Error')
#################################################################################################################################################
        elif ((header_name.find('Company_Clinic') > -1) or (header_name.find('Initials') > -1) or (header_name.find('Collection_Tube_Type') > -1)):
            error_msg = "Value must be a string and NOT N/A"
            for i in range(len(has_data)):
                Biospecimen_object.is_string(header_name,has_data.values[i],False,error_msg,has_data.index[i],'Error')
#################################################################################################################################################                      
        elif(header_name in ["Final_Concentration_of_Biospecimen"]) or (header_name.find('Hemocytometer_Count') > -1) or (header_name.find('Automated_Count') > -1):
            error_msg = "Biospecimen Type == PBMC, Value must be a positive number"
            test_value = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] == "PBMC"][header_name]
            for i in range(len(test_value)):
                Biospecimen_object.is_numeric(header_name,False,test_value.values[i],0,error_msg,test_value.index[i],'Error')
            Biospecimen_object.biospeimen_type_wrong("PBMC",header_name)
#################################################################################################################################################
        elif(header_name in ["Centrifugation_Time","RT_Serum_Clotting_Time"]):
            error_msg = "Biospecimen Type == Serum, Value must be a positive number"
            test_value = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] == "Serum"][header_name]
            for i in range(len(test_value)):
                Biospecimen_object.is_numeric(header_name,True,test_value.values[i],0,error_msg,test_value.index[i],'Error')
            Biospecimen_object.biospeimen_type_wrong("Serum",header_name)
#################################################################################################################################################
        elif(header_name in ["Storage_Start_Time_80_LN2_storage"]):
            error_msg = "Biospecimen Type == Serum, Value must be a Time in 24hour format HH:MM"
            test_value = Biospecimen_object.Data_Table[Biospecimen_object.Data_Table['Biospecimen_Type'] == "Serum"][header_name]
            for i in range(len(test_value)):
                Biospecimen_object.is_date_time(header_name,test_value.values[i],False,error_msg,test_value.index[i],'Error')
            Biospecimen_object.biospeimen_type_wrong("Serum",header_name)
#################################################################################################################################################
        elif(header_name in ["Initial_Volume_of_Biospecimen"]):
            error_msg = "Value must be a number greater than 0"
            for i in range(len(has_data)):
                Biospecimen_object.is_numeric(header_name,False,has_data.values[i],0,error_msg,has_data.index[i],'Error')
        Biospecimen_object.missing_data_errors(Required_column,header_name,missing_data,missing_pos_data,missing_neg_data)
#################################################################################################################################################
    Biospecimen_object.biospecimen_count_check('Hemocytometer_Count')
    Biospecimen_object.biospecimen_count_check('Automated_Count')
###############################################################################################################################
    test_column = Biospecimen_object.Data_Table["Research_Participant_ID"]
    pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}$')
    matching_RPI_values = [i for i in enumerate(test_column) if pattern.match(i[1]) is not None]

    test_column = Biospecimen_object.Data_Table["Biospecimen_ID"]
    pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}[_]{1}[0-9]{3}$')
    matching_BIO_values = [i for i in enumerate(test_column) if pattern.match(i[1]) is not None]
    BIO_index,BIO_Value = map(list,zip(*matching_BIO_values))

    for i in enumerate(matching_RPI_values):
        if i[1][0] in BIO_index:
            if BIO_Value[BIO_index.index(i[1][0])].find(i[1][1]) == -1:
                error_msg = "Research_Participant_ID does not agree with Biospecimen ID(" + BIO_Value[BIO_index.index(i[1][0])] + "), first 9 characters should match"
                Biospecimen_object.write_error_msg(i[1][1],"Research_Participant_ID",error_msg,i[1][0],'Error')
###############################################################################################################################
        
    return Biospecimen_object