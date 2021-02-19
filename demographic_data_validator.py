def demographic_data_validator(current_object,prior_cov_test,re,pd,valid_cbc_ids):
    current_object.Data_Table = current_object.Data_Table.merge(prior_cov_test,how='left',on="Research_Participant_ID")
    current_object.All_Error_DF = pd.DataFrame(columns=current_object.error_list_summary)
    for header_name in current_object.Column_Header_List:
        Required_column = "Yes: SARS-Positive"
#################################################################################################################################################
        if 'Research_Participant_ID' in header_name:
            Required_column = "Yes"
            pattern_str = '[_]{1}[0-9]{6}$'
            current_object.check_id_field(pd,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX",False)
            current_object.check_id_cross_sheet(pd,header_name,"SARS_CoV_2_PCR_Test_Result","demographic","prior_clinical_test")
#################################################################################################################################################
        elif (header_name == 'Age'):
            Required_column = "Yes"
            Error_Message = "Value must be a number greater than 0"
            current_object.check_if_number(pd,current_object.Data_Table,header_name,1,1000,"All",False,Error_Message)
#################################################################################################################################################
        elif (header_name in ['Race','Ethnicity','Gender']):
            Required_column = "Yes"
            if (header_name == 'Race'):
                list_values =  ['White', 'American Indian or Alaska Native', 'Black or African American', 'Asian',
                                'Native Hawaiian or Other Pacific Islander', 'Other', 'Multirace','Not Reported', 'Unknown']
            elif (header_name == 'Ethnicity'):
                list_values = ['Hispanic or Latino','Not Hispanic or Latino']
            elif (header_name == 'Gender'):
                list_values = ['Male', 'Female', 'Other','Not Reported', 'Unknown']
            current_object.check_in_list(pd,header_name,[list_values])
#################################################################################################################################################
        elif (header_name == 'Is_Symptomatic'):
            pos_list = ['Yes','No']
            neg_list = ['No','N/A']
            current_object.check_in_list(pd,header_name,[pos_list,neg_list])
#################################################################################################################################################
        elif (header_name == 'Date_of_Symptom_Onset'):
            pos_error_msg = "Participant previous had symptoms or is currently symptomatic (Is_Symptomatic == 'Yes'), value must be a valid Date MM/DD/YYYY"
            neg_error_msg = "Participant does not have symptomns (Is_Symptomatic == 'No or N/A'), value must be N/A"
            current_object.symptom_logic_check(pd,header_name,"Is_Symptomatic",pos_error_msg,neg_error_msg,"Date")
#################################################################################################################################################
        elif (header_name == 'Symptoms_Resolved'):
            pos_error_msg = "Participant previous had symptoms or is currently symptomatic (Is_Symptomatic == 'Yes'), value must be: [Yes,No]"
            neg_error_msg = "Participant does not have symptomns (Is_Symptomatic == 'No or N/A'), value must be N/A"
            current_object.symptom_logic_check(pd,header_name,"Is_Symptomatic",pos_error_msg,neg_error_msg,["Yes","No"])
#################################################################################################################################################
        elif (header_name == 'Date_of_Symptom_Resolution'):
            pos_error_msg = "Participant symptoms have resolved (Symptoms_Resolved == 'Yes'), value must be valid Date MM/DD/YYYY"
            neg_error_msg = "Participant still has symptomns or never had symptoms (Symptoms_Resolved == 'No' or 'N/A'), value must be N/A"
            current_object.symptom_logic_check(pd,header_name,"Symptoms_Resolved",pos_error_msg,neg_error_msg,"Date")
#################################################################################################################################################
        elif (header_name == 'Covid_Disease_Severity'):
            Error_Message ="Participant is SARS_CoV2 Positive. Value must be a number betweeen 1 and 8"
            current_object.check_if_number(pd,current_object.Data_Table,header_name,1,8,"Positive",False,Error_Message)
            Error_Message ="Participant is SARS_CoV2 Negative. Value must be 0"
            current_object.check_if_number(pd,current_object.Data_Table,header_name,0,0,"Negative",False,Error_Message)
#################################################################################################################################################
        elif (header_name in ["Diabetes_Mellitus","Hypertension","Severe_Obesity","Cardiovascular_Disease","Chronic_Renal_Disease",
                              "Chronic_Liver_Disease","Chronic_Lung_Disease","Immunosuppressive_conditions","Autoimmune_condition","Inflammatory_Disease"]):
            pos_list = ["Yes", "No"]
            neg_list = ["Yes", "No", "Unknown", "N/A"]
            current_object.check_in_list(pd,header_name,[pos_list,neg_list])
#################################################################################################################################################
        elif (header_name in ["Other_Comorbidity"]):
            Required_column = "No"
            current_object.check_icd10(pd,header_name)
#################################################################################################################################################
        else:
            print("Column_Name: " + header_name + " has no validation rules set")
#################################################################################################################################################
        current_object.get_missing_values(pd,header_name,Required_column)
    return current_object