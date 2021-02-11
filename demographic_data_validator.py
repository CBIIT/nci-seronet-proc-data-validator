def demographic_data_validator(demo_data_object,neg_list,pos_list,re,valid_cbc_ids):
    demo_data_object.get_pos_neg_logic(pos_list,neg_list)
    demo_data_object.remove_unknown_sars_results_v2()
    for header_name in demo_data_object.Column_Header_List:
        test_column = demo_data_object.Data_Table[header_name]
        has_data,has_pos_data,has_neg_data,missing_data,missing_pos_data,missing_neg_data = demo_data_object.check_data_type(test_column)
        Required_column = "Yes: SARS-Positive"
#################################################################################################################################################
        if 'Research_Participant_ID' in header_name:        #checks if Participant ID in valid format
            Required_column = "Yes"
            error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX"
            pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}$')
            for i in range(len(has_data)):
                demo_data_object.valid_ID(header_name,has_data.values[i],pattern,valid_cbc_ids,error_msg,has_data.index[i],'Error')
            id_error_list = [i[5] for i in demo_data_object.error_list_summary if (i[0] == "Error") and (i[4] == "Research_Participant_ID")]
            matching_values = [i for i in enumerate(has_data) if (pattern.match(i[1]) is not None) and (i[1] not in id_error_list)]
            if (len(matching_values) > 0):
                error_msg = "ID is valid, however is not found in Prior_Test_Results, No Matching Prior_SARS_CoV-2 Result"
                check_list = pos_list['Research_Participant_ID'].tolist()+neg_list['Research_Participant_ID'].tolist()
                for i in enumerate(matching_values):
                    demo_data_object.in_list(header_name,i[1][1],check_list,error_msg,i[1][0],'Error')
#################################################################################################################################################
        elif (header_name == 'Age'):
            Required_column = "Yes"
            error_msg = "Value must be a number greater than 0"
            for i in range(len(has_data)):
                demo_data_object.is_numeric(header_name,False,has_data.values[i],0,error_msg,has_data.index[i],'Error')
        elif (header_name in ['Race','Ethnicity','Gender']):
            Required_column = "Yes"
            if (header_name == 'Race'):
                test_string =  ['White', 'American Indian or Alaska Native', 'Black or African American', 'Asian',
                                'Native Hawaiian or Other Pacific Islander', 'Other', 'Multirace','Not Reported', 'Unknown']
            elif (header_name == 'Ethnicity'):
                test_string = ['Hispanic or Latino','Not Hispanic or Latino']
            elif (header_name == 'Gender'):
                test_string = ['Male', 'Female', 'Other','Not Reported', 'Unknown']
            error_msg = "Value must be one of the following: " + str(test_string)
            for i in range(len(has_data)):
                demo_data_object.in_list(header_name,has_data.values[i],test_string,error_msg,has_data.index[i],'Error')
#################################################################################################################################################
        elif (header_name == 'Is_Symptomatic'):
            pos_list = ['Yes','No']
            neg_list = ['No','N/A']
            demo_data_object.get_in_list_logic(header_name,pos_list,neg_list,has_pos_data,has_neg_data)
#################################################################################################################################################
        elif (header_name == 'Date_of_Symptom_Onset'):
            error_msg = "Participant has symptomns (Is_Symptomatic == 'Yes'), value must be a valid Date MM/DD/YYYY"
            test_logic = (demo_data_object.Data_Table['Is_Symptomatic'] == "Yes") & (demo_data_object.pos_list_logic)
            test_value = demo_data_object.Data_Table[test_logic][header_name]
            for i in range(len(test_value)):
                demo_data_object.is_date_time(header_name,test_value.values[i],False,error_msg,test_value.index[i],'Error')
            demo_data_object.particpant_no_symtpoms(header_name,'Is_Symptomatic')
#################################################################################################################################################
        elif (header_name == 'Symptoms_Resolved'):
            test_string = ["Yes","No"]
            error_msg = "Participant previous had symptoms or currently has symptoms (Is_Symptomatic == 'Yes'), value must be: " + str(test_string)
            test_logic = (demo_data_object.Data_Table['Is_Symptomatic'] == "Yes") & (demo_data_object.pos_list_logic)
            test_value = demo_data_object.Data_Table[test_logic][header_name]
            for i in range(len(test_value)):
                demo_data_object.in_list(header_name,test_value.values[i],test_string,error_msg,test_value.index[i],'Error')
            demo_data_object.particpant_no_symtpoms(header_name,'Is_Symptomatic')
#################################################################################################################################################
        elif (header_name.find('Date_of_Symptom_Resolution') > -1):
            error_msg = "Participant symptoms have resolved (Symptoms_Resolved == 'Yes'), value must be valid Date MM/DD/YYYY"
            test_value = demo_data_object.Data_Table[(demo_data_object.Data_Table['Symptoms_Resolved'] == "Yes")][header_name]
            for i in range(len(test_value)):
                demo_data_object.is_date_time(header_name,test_value.values[i],False,error_msg,test_value.index[i],'Error')
            demo_data_object.particpant_no_symtpoms(header_name,'Symptoms_Resolved')
#################################################################################################################################################
        elif (header_name == 'Covid_Disease_Severity'):
            error_msg = "Participant is SARS_CoV2 Positive. Value must be a number betweeen 1 and 8"
            for i in range(len(has_pos_data)):
                demo_data_object.is_numeric(header_name,False,has_pos_data.index[i],0,error_msg,has_pos_data.index[i],'Error')
            error_msg = "Participant is SARS_CoV2 Negative. value must be 0"
            for i in range(len(has_neg_data)):
                demo_data_object.in_list(header_name,has_neg_data.values[i],[0,'0'],error_msg,has_neg_data.index[i],'Error')
#################################################################################################################################################
        elif (header_name in ["Diabetes_Mellitus","Hypertension","Severe_Obesity","Cardiovascular_Disease","Chronic_Renal_Disease",
                                             "Chronic_Liver_Disease","Chronic_Lung_Disease","Immunosuppressive_conditions","Autoimmune_condition","Inflammatory_Disease"]):

            pos_list = ["Yes", "No"]
            neg_list = ["Yes", "No", "Unknown", "N/A"]
            demo_data_object.get_in_list_logic(header_name,pos_list,neg_list,has_pos_data,has_neg_data)
#################################################################################################################################################
        elif (header_name in ["Other_Comorbidity"]):
            Required_column = "No"
            error_msg = "Invalid or unknown ICD10 code, Value must be Valid ICD10 code or N/A"
            for i in range(len(has_data)):
                demo_data_object.check_icd10(header_name,has_data.values[i],error_msg,has_data.index[i],'Error')
#################################################################################################################################################
        demo_data_object.missing_data_errors(Required_column,header_name,missing_data,missing_pos_data,missing_neg_data)
    return demo_data_object