def Validation_Rules(pd,re,datetime,current_object,data_table,file_name,valid_cbc_ids,drop_list):
    min_date = datetime.date(1900, 1, 1)
    max_date = datetime.date.today()
    for header_name in data_table.columns:
        if header_name in drop_list:
            continue
        Required_column = "Yes"
        if 'Research_Participant_ID' in header_name:
            pattern_str = '[_]{1}[0-9]{6}$'
            current_object.check_id_field(file_name,data_table,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX")
        elif 'SARS_CoV_2_PCR_Test_Result_Provenance' in header_name:
            list_values = ['From Medical Record','Self-Reported']
            current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
        elif 'SARS_CoV_2_PCR_Test_Result' in header_name:
            list_values = ['Positive', 'Negative']
            current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
        elif 'Test_Result_Provenance' in header_name:     #checks result proveance for valid input options
            Required_column = "Yes: SARS-Negative"
            list_values = ['Self-Reported','From Medical Record','N/A']
            current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
        elif 'Date_of_SARS_CoV_2_PCR_sample_collection' in header_name:    
            current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Date",min_date,max_date)
        elif 'Date_of' in header_name:                    #checks if column variables are in date format
            Required_column = "No"
            current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",True,"Date",min_date,max_date)
        elif ('infection_unit' in header_name) or ('HAART_Therapy_unit' in header_name):
            Required_column = "No"
            duration_name = header_name.replace('_unit','')
            current_object.check_in_list(pd,file_name,data_table,header_name,duration_name,"Is A Number",["Day","Month","Year"])
            current_object.check_in_list(pd,file_name,data_table,header_name,duration_name,["N/A"],["N/A"])
        elif 'Duration' in header_name:
            Required_column = "No"
            if 'HAART_Therapy' in header_name:
                current_name = 'On_HAART_Therapy'
            else:
                current_name = header_name.replace('Duration_of','Current')
            current_object.check_in_list(pd,file_name,data_table,header_name,current_name,['No','Unknown','N/A'],["N/A"])
            current_object.check_if_number(pd,file_name,data_table,header_name,current_name,['Yes'],False,0,365,"int")
        elif ('Current' in header_name) or ('HAART_Therapy' in header_name):
            Required_column = "Yes: SARS-Negative"            
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Positive"],['Yes','No','Unknown','N/A'])
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Negative"],['Yes','No','Unknown'])
        elif ('Test_Result' in header_name) or ('Seasonal_Coronavirus' in header_name):
            Required_column = "Yes: SARS-Negative"
            pos_list = ['Positive','Negative','Equivocal','Not Performed','N/A']
            neg_list = ['Positive','Negative','Equivocal','Not Performed']
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Positive"],pos_list)
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Negative"],neg_list)     
#################################################################################################################################################
        elif (header_name == 'Age'):
            current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",False,0,200,"int")  
        elif (header_name in ['Race','Ethnicity','Gender']):
            if (header_name == 'Race'):
                list_values =  ['White', 'American Indian or Alaska Native', 'Black or African American', 'Asian',
                                'Native Hawaiian or Other Pacific Islander', 'Other', 'Multirace','Not Reported', 'Unknown']
            elif (header_name == 'Ethnicity'):
                list_values = ['Hispanic or Latino','Not Hispanic or Latino']
            elif (header_name == 'Gender'):
                list_values = ['Male', 'Female', 'Other','Not Reported', 'Unknown']
            current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
        elif (header_name == 'Is_Symptomatic'):
            Required_column = "Yes: SARS-Positive"
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Positive"],['Yes','No'])
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Negative"],['No','N/A'])
        elif (header_name == 'Date_of_Symptom_Onset'):
            Required_column = "Yes: SARS-Positive"
            current_object.check_date(pd,datetime,file_name,data_table,header_name,"Is_Symptomatic",["Yes"],False,"Date",min_date,max_date)
            current_object.check_in_list(pd,file_name,data_table,header_name,"Is_Symptomatic",["No","N/A"],["N/A"])
        elif (header_name == 'Symptoms_Resolved'):
            Required_column = "Yes: SARS-Positive"
            current_object.check_in_list(pd,file_name,data_table,header_name,"Is_Symptomatic",["Yes"],["Yes","No"])
            current_object.check_in_list(pd,file_name,data_table,header_name,"Is_Symptomatic",["No","N/A"],["N/A"])
        elif (header_name == 'Date_of_Symptom_Resolution'):
            Required_column = "Yes: SARS-Positive"
            current_object.check_date(pd,datetime,file_name,data_table,header_name,"Symptoms_Resolved",["Yes"],False,"Date",min_date,max_date)
            current_object.check_in_list(pd,file_name,data_table,header_name,"Symptoms_Resolved",["No","N/A"],["N/A"])
        elif (header_name == 'Covid_Disease_Severity'):
            Required_column = "Yes: SARS-Positive"
            current_object.check_if_number(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Positive"],False,1,8,"int")
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Negative"],[0])
        elif (header_name in ["Diabetes_Mellitus","Hypertension","Severe_Obesity","Cardiovascular_Disease","Chronic_Renal_Disease",
                              "Chronic_Liver_Disease","Chronic_Lung_Disease","Immunosuppressive_conditions","Autoimmune_condition","Inflammatory_Disease"]):
            Required_column = "Yes: SARS-Positive"
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Positive"],['Yes','No'])
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Negative"],["Yes", "No", "Unknown", "N/A"])          
        elif (header_name in ["Other_Comorbidity"]):
            Required_column = "No"
            current_object.check_icd10(file_name,data_table,header_name)
#################################################################################################################################################
        elif (header_name in ["Biospecimen_ID"]):
            pattern_str = '[_]{1}[0-9]{6}[_]{1}[0-9]{3}$'
            current_object.check_id_field(file_name,data_table,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX_XXX")
            id_compare = data_table[data_table.apply(lambda x: x["Research_Participant_ID"] not in x["Biospecimen_ID"],axis = 1)]
            Error_Message = "Research_Participant_ID is not a substring of Biospecimen ID.  Data is not Valid, please check data"
            current_object.update_error_table("Error",id_compare,file_name,header_name,Error_Message)
        elif(header_name in ["Biospecimen_Group"]):
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Positive"],['Positive Sample'])
            current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Negative"],['Negative Sample'])
        elif(header_name in ["Biospecimen_Type"]):
            list_values = ["Serum", "EDTA Plasma", "PBMC", "Saliva", "Nasal swab"]
            current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
        elif 'Time_of' in header_name:
            current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Time")
        elif ('Expiration_Date' in header_name):
            current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Date",max_date,datetime.date(3000, 1, 1))
        elif ('Date_of' in header_name):
            current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Date",min_date,max_date)
        elif(header_name in ["Storage_Time_at_2_8"]):
            current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",True,0,1000,"float")
        elif(header_name in ["Storage_Start_Time_at_2_8_Initials","Storage_End_Time_at_2_8_Initials"]):
            current_object.check_if_string(pd,file_name,data_table,header_name,"Storage_Time_at_2_8","Is A Number",False)
            current_object.check_in_list(pd,file_name,data_table,header_name,"Storage_Time_at_2_8",["N/A"],['N/A'])
        elif(header_name in ["Storage_Start_Time_at_2_8","Storage_End_Time_at_2_8"]):
             current_object.check_date(pd,datetime,file_name,data_table,header_name,"Storage_Time_at_2_8","Is A Number",False,"Date",min_date,max_date)
             current_object.check_in_list(pd,file_name,data_table,header_name,"Storage_Time_at_2_8",["N/A"],['N/A'])
        elif ((header_name.find('Company_Clinic') > -1) or (header_name.find('Initials') > -1) or (header_name.find('Collection_Tube_Type') > -1)):
            current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",False)
        elif(header_name in ["Final_Concentration_of_Biospecimen"]) or (header_name.find('Hemocytometer_Count') > -1) or (header_name.find('Automated_Count') > -1):
            current_object.check_if_number(pd,file_name,data_table,header_name,"Biospecimen_Type",["PBMC"],True,0,1e9,"float")
        elif(header_name in ["Centrifugation_Time","RT_Serum_Clotting_Time"]):
            current_object.check_if_number(pd,file_name,data_table,header_name,"Biospecimen_Type",["Serum"],True,0,1e9,"float")
        elif(header_name in ["Storage_Start_Time_80_LN2_storage"]):
             current_object.check_date(pd,datetime,file_name,data_table,header_name,"Biospecimen_Type",["Serum"],False,"Time")
        elif(header_name in ["Initial_Volume_of_Biospecimen"]):
            current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",True,0,1e9,"float")
#################################################################################################################################################
        elif (header_name in ["Aliquot_ID"]):
            pattern_str = '[_]{1}[0-9]{6}[_]{1}[0-9]{3}[_]{1}[0-9]{2}$$'
            current_object.check_id_field(file_name,data_table,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX_XXX_XX")
            id_compare = data_table[data_table.apply(lambda x: x["Biospecimen_ID"] not in x["Aliquot_ID"],axis = 1)]
            Error_Message = "Biospecimen ID is not a substring of Aliquot ID.  Data is not Valid, please check data"
            current_object.update_error_table("Error",id_compare,file_name,header_name,Error_Message)
        elif (header_name in ["Aliquot_Volume"]):
            current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",True,0,1e9,"float")
        elif ("Catalog_Number") in header_name or ("Lot_Number") in header_name or ("Aliquot" in header_name) or ("Equipment_ID" in header_name):
            current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",False)
        elif (header_name.find('Due_Date') > -1):
            current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Date",max_date,datetime.date(3000, 1, 1))
        elif (header_name in ["Equipment_Type","Reagent_Name","Consumable_Name"]):
            if (header_name in ["Equipment_Type"]):
                list_values = ['Refrigerator','-80 Refrigerator', 'LN Refrigerator', 'Microsope', 'Pipettor', 'Controlled-Rate Freezer', 'Automated-Cell Counter']
            elif (header_name in ["Reagent_Name"]):
                list_values =  (['DPBS', 'Ficoll-Hypaque','RPMI-1640','no L-Glutamine','Fetal Bovine Serum','200 mM L-Glutamine',
                                 '1M Hepes','Penicillin/Streptomycin','DMSO', 'Cell Culture Grade','Vital Stain Dye'])
            elif (header_name in ["Consumable_Name"]):
                list_values = ["50 mL Polypropylene Tube", "15 mL Conical Tube" ,"Cryovial Label"]
            current_object.check_in_list(pd,file_name,data_table,header_name,"Biospecimen_Type",["PBMC"],list_values)           
        else:
            print("Column_Name: " + header_name + " has no validation rules set")
        if (header_name in ['Total_Cells_Hemocytometer_Count', 'Total_Cells_Automated_Count']):
           current_object.compare_total_to_live(pd,file_name,data_table,header_name)
        if (header_name in ['Viability_Hemocytometer_Count', 'Viability_Automated_Count']):
           current_object.compare_viability(pd,file_name,data_table,header_name)
#################################################################################################################################################
        current_object.get_missing_values(pd,file_name,data_table,header_name,Required_column)
    return current_object