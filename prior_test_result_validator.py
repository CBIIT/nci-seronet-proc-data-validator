def prior_test_result_validator(current_object,demo_ids,re,pd,valid_cbc_ids):
    current_object.Data_Table = current_object.Data_Table.merge(demo_ids.drop_duplicates("Research_Participant_ID"),how='left',on="Research_Participant_ID")
    current_object.All_Error_DF = pd.DataFrame(columns=current_object.error_list_summary)
    for header_name in current_object.Column_Header_List:
        Required_column = "Yes"
#################################################################################################################################################
        if 'Research_Participant_ID' in header_name:
            pattern_str = '[_]{1}[0-9]{6}$'
            current_object.check_id_field(pd,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX",False)
            current_object.check_id_cross_sheet(pd,header_name,"Age","prior_clinical_test","demographic")
#################################################################################################################################################
        elif 'SARS_CoV_2_PCR_Test_Result_Provenance' in header_name:
            list_values = ['From Medical Record','Self-Reported']
            current_object.check_in_list(pd,header_name,[list_values])
#################################################################################################################################################
        elif 'SARS_CoV_2_PCR_Test_Result' in header_name:
            list_values = ['Positive', 'Negative']
            current_object.check_in_list(pd,header_name,[list_values])
##############################################################################################################################################
        elif 'Test_Result_Provenance' in header_name:     #checks result proveance for valid input options
            Required_column = "Yes: SARS-Negative"
            pos_list = ['Self-Reported','From Medical Record','N/A']
            neg_list = ['Self-Reported','From Medical Record']
            current_object.check_in_list(pd,header_name,[pos_list,neg_list])
#################################################################################################################################################
        elif 'Date_of' in header_name:                    #checks if column variables are in date format
            Error_Message = "Value must be a valid date MM/DD/YYYY"
            if 'Date_of_SARS_CoV_2_PCR_sample_collection' in header_name:
                Required_column = "Yes"
                current_object.check_date(pd,current_object.Data_Table,header_name,False,Error_Message)
            else:
                Required_column = "No"
                current_object.check_date(pd,current_object.Data_Table,header_name,True,Error_Message)
#################################################################################################################################################
        elif ('infection_unit' in header_name) or ('HAART_Therapy_unit' in header_name):
            Required_column = "No"
            duration_name = header_name.replace('_unit','')
            current_object.get_duration_unit(pd,duration_name,header_name,['Day','Month','Year'])
#################################################################################################################################################
        elif 'Duration' in header_name:
            Required_column = "No"
            if 'HAART_Therapy' in header_name:
                current_name = 'On_HAART_Therapy'
            else:
                current_name = header_name.replace('Duration_of','Current')
            current_object.get_duration_check(pd,current_name,header_name)
#################################################################################################################################################
        elif ('Current' in header_name) or ('HAART_Therapy' in header_name):
            Required_column = "Yes: SARS-Negative"
            pos_list = ['Yes','No','Unknown','N/A']
            neg_list = ['Yes','No','Unknown']
            current_object.check_in_list(pd,header_name,[pos_list,neg_list])
#################################################################################################################################################
        elif ('Test_Result' in header_name) or ('Seasonal_Coronavirus' in header_name):
            Required_column = "Yes: SARS-Negative"
            pos_list = ['Positive','Negative','Equivocal','Not Performed','N/A']
            neg_list = ['Positive','Negative','Equivocal','Not Performed']
            current_object.check_in_list(pd,header_name,[pos_list,neg_list])
#################################################################################################################################################
        else:
            print("Column_Name: " + header_name + " has no validation rules set")
#################################################################################################################################################
        current_object.get_missing_values(pd,header_name,Required_column)
    return current_object