def prior_test_result_validator(prior_valid_object,neg_list,pos_list,re,valid_cbc_ids,current_demo):
    prior_valid_object.get_pos_neg_logic(pos_list,neg_list)
    for header_name in prior_valid_object.Column_Header_List:
        test_column = prior_valid_object.Data_Table[header_name]
        has_data,has_pos_data,has_neg_data,missing_data,missing_pos_data,missing_neg_data = prior_valid_object.check_data_type(test_column)
        Required_column = "Yes"
#################################################################################################################################################
        if 'Research_Participant_ID' in header_name:        #checks if Participant ID in valid format
            error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX"
            pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}$')
            for i in range(len(has_data)):
                prior_valid_object.valid_ID(header_name,has_data.values[i],pattern,valid_cbc_ids,error_msg,has_data.index[i],'Error')
            id_error_list = [i[5] for i in prior_valid_object.error_list_summary if (i[0] == "Error") and (i[4] == "Research_Participant_ID")]
            matching_values = [i for i in enumerate(has_data) if (pattern.match(i[1]) is not None) and (i[1] not in id_error_list)]
            if (len(matching_values) > 0) and (len(current_demo) > 0):
                error_msg = "Id is not found in database or in submitted demographic.csv file"
                for i in enumerate(matching_values):
                    prior_valid_object.in_list(header_name,i[1][1],current_demo,error_msg,i[1][0],'Error')
#################################################################################################################################################
        elif 'SARS_CoV_2_PCR_Test_Result_Provenance' in header_name:
            test_string = ['From Medical Record','Self-Reported']
            error_msg = "Value must be one of the following: " + str(test_string)
            for i in range(len(has_data)):
                prior_valid_object.in_list(header_name,has_data.values[i],test_string,error_msg,has_data.index[i],'Error')
#################################################################################################################################################
        elif 'SARS_CoV_2_PCR_Test_Result' in header_name:
            test_string = ['Positive', 'Negative']
            error_msg = "Value must be one of the following: " + str(test_string)
            for i in range(len(has_data)):
                prior_valid_object.in_list(header_name,has_data.values[i],test_string,error_msg,has_data.index[i],'Error')
#################################################################################################################################################
        elif 'Date_of_SARS_CoV_2_PCR_sample_collection' in header_name:
            error_msg = "Value must be a valid date MM/DD/YYYY"
            for i in range(len(has_data)):
                prior_valid_object.is_date_time(header_name,has_data.values[i],False,error_msg,has_data.index[i],'Error') 
#################################################################################################################################################
        elif 'Date_of' in header_name:                    #checks if column variables are in date format
            Required_column = "Yes: SARS-Negative"
            error_msg = "Value must be a valid date MM/DD/YYYY or N/A"
            for i in range(len(has_data)):
                prior_valid_object.is_date_time(header_name,has_data.values[i],True,error_msg,has_data.index[i],'Error')
#################################################################################################################################################
        elif 'Test_Result_Provenance' in header_name:     #checks result proveance for valid input options
            Required_column = "Yes: SARS-Negative"
            test_string = ['Self-Reported','From Medical Record','N/A']
            error_msg = "Participant is SARS CoV-2 positive, Value must be one of the following: " + str(test_string)
            for i in range(len(has_pos_data)):
                prior_valid_object.in_list(header_name,has_pos_data.values[i],test_string,error_msg,has_pos_data.index[i],'Error')
            test_string = ['Self-Reported','From Medical Record','N/A']
            error_msg = "Participant is SARS CoV-2 negative, Value must be one of the following: " + str(test_string)
            for i in range(len(has_neg_data)):
                prior_valid_object.in_list(header_name,has_neg_data.values[i],test_string,error_msg,has_neg_data.index[i],'Error')
#################################################################################################################################################
        elif 'Duration' in header_name:                #Is value a number OR is value == N/A
            Required_column = "No"
            if (header_name.find('HAART_Therapy') > -1):
                current_index = 'On_HAART_Therapy'
            else:
                current_index = header_name.replace('Duration_of','Current')
            prior_valid_object.current_infection_check(current_index,["Yes"],header_name)                          #has current infection, must be number
            prior_valid_object.current_infection_check(current_index,['No','Unknown','N/A'],header_name)           #does not have infection, must be N/A
            error_msg = "Unknown value for " + current_index + " for current Participant.  Unable to Validate Duration"
            has_data = prior_valid_object.Data_Table.iloc[[i[0] for i in enumerate(prior_valid_object.Data_Table[current_index]) if i[1] not in ['Yes','No','Unknown','N/A']]][header_name]
            for i in range(len(has_data)):
                prior_valid_object.write_error_msg(has_data.values[i],header_name,error_msg,has_data.index[i],'Error')
################################################################################################################################################
        elif 'infection_unit' in header_name:
            Required_column = "No"
            duration_index = header_name.replace('unit','')
            duration_data = prior_valid_object.Data_Table[duration_index]
        
            has_duration = [i for i in enumerate(duration_data) if str(i[1]).isdigit()]         #must be day/month/year
            error_msg = "Duration is a Number, value must be in ['Day','Month','Year']"
            prior_valid_object.get_duration_logic(has_data,['Day','Month','Year'],error_msg,'Error')
            
            duration_NA = [i for i in enumerate(duration_data) if i[1] in ['N/A']]              #must be N/A
            error_msg = "Duration is N/A, value must be N/A"
            prior_valid_object.get_duration_logic(has_data,['N/A'],error_msg,'Error')
            
            duration_missing = [i for i in enumerate(duration_data) if not((i[1] in ['N/A']) or (str(i[1]).isdigit()))] #unknown
            error_msg = "Duration is Missing/Unknown, Unable to Validate Column"
            prior_valid_object.get_duration_logic(has_data,[''],error_msg,'Warning')
#################################################################################################################################################
        elif ('Current' in header_name) | ('HAART_Therapy' in header_name):
            Required_column = "Yes: SARS-Negative"
            pos_list = ['Yes','No','Unknown','N/A']
            neg_list = ['Yes','No','Unknown']
            prior_valid_object.pos_neg_errors(pos_list,neg_list,has_pos_data,has_neg_data,header_name)
#################################################################################################################################################
        elif ('Test_Result' in header_name) | ('Seasonal_Coronavirus' in header_name):
            Required_column = "Yes: SARS-Negative"
            pos_list = ['Positive','Negative','Equivocal','Not Performed','N/A']
            neg_list = ['Positive','Negative','Equivocal','Not Performed']
            prior_valid_object.pos_neg_errors(pos_list,neg_list,has_pos_data,has_neg_data,header_name) 
#################################################################################################################################################
        prior_valid_object.missing_data_errors(Required_column,header_name,missing_data,missing_pos_data,missing_neg_data)
    return prior_valid_object