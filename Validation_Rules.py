def Validation_Rules(pd,re,datetime,current_object,data_table,file_name,valid_cbc_ids,drop_list):
    min_date = datetime.date(1900, 1, 1)
    max_date = datetime.date.today()
    for header_name in data_table.columns:
        if header_name in drop_list:
            continue
        Rule_Found = [True]*2
#################################################################################################################################################
        Required_column,Rule_Found = check_ID_validation(header_name,current_object,file_name,data_table,re,valid_cbc_ids,Rule_Found,0)
        if file_name in ["prior_clinical_test.csv"]:
            Required_column,Rule_Found = check_prior_clinical(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,1)
        if file_name in ["demographic.csv"]:
            Required_column,Rule_Found = check_demographic(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,1)
        if file_name in ["biospecimen.csv"]:
            Required_column,Rule_Found = check_biospecimen(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,1)
        if file_name in ["aliquot.csv","equipment.csv","reagent.csv","consumable.csv"]:
            Required_column,Rule_Found = check_processing_rules(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,1)
        if file_name in ["confirmatory_clinical_test.csv"]:
            Required_column,Rule_Found = check_confimation_rules(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,1)
        if file_name in ["assay.csv","assay_target.csv"]:
            Required_column,Rule_Found = check_assay_rules(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,1)
#################################################################################################################################################
        if (header_name in ['Total_Cells_Hemocytometer_Count', 'Total_Cells_Automated_Count']):
           current_object.compare_total_to_live(pd,file_name,data_table,header_name)
        if (header_name in ['Viability_Hemocytometer_Count', 'Viability_Automated_Count']):
           current_object.compare_viability(pd,file_name,data_table,header_name)
        if True not in Rule_Found:
            print("Column_Name: " + header_name + " has no validation rules set")
        else:
            current_object.get_missing_values(pd,file_name,data_table,header_name,Required_column)
            
    if ('Research_Participant_ID' in data_table.columns) and ('Research_Participant_ID' not in drop_list):
        current_object.Part_List.append(file_name)
    if ('Biospecimen_ID' in data_table.columns) and ('Biospecimen_ID' not in drop_list):
        current_object.Bio_List.append(file_name)
    return current_object
def check_ID_Cross_Sheet(current_object,pd,re):
    current_object.get_all_part_ids()
    current_object.get_all_bio_ids()
    
    current_object.get_cross_sheet_Participant_ID(pd,re,'Research_Participant_ID')
    current_object.get_cross_sheet_Biospecimen_ID(pd,re,'Biospecimen_ID')
    current_object.get_passing_part_ids('Research_Participant_ID')
    current_object.get_passing_part_ids('Biospecimen_ID')
            
def check_ID_validation(header_name,current_object,file_name,data_table,re,valid_cbc_ids,Rule_Found,index,Required_column = "Yes"):
    if header_name in ['Research_Participant_ID']:
        pattern_str = '[_]{1}[0-9]{6}$'
        current_object.check_id_field(file_name,data_table,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX")
        if (file_name not in ["biospecimen.csv"]):
            current_object.check_for_dup_ids(file_name,header_name)
    elif (header_name in ["Biospecimen_ID"]):
        pattern_str = '[_]{1}[0-9]{6}[_]{1}[0-9]{3}$'
        current_object.check_id_field(file_name,data_table,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX_XXX")
        if (header_name in ['Research_Participant_ID']) and (header_name in ["Biospecimen_ID"]):
            current_object.check_if_substr(data_table,"Research_Participant_ID","Biospecimen_ID",file_name,header_name)
        if (file_name in ["biospecimen.csv"]):
            current_object.check_for_dup_ids(file_name,header_name)
    elif (header_name in ["Aliquot_ID"]):
        pattern_str = '[_]{1}[0-9]{6}[_]{1}[0-9]{3}[_]{1}[0-9]{2}$'
        current_object.check_id_field(file_name,data_table,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX_XXX_XX")
        if (header_name in ["Aliquot_ID"]) and (header_name in ["Biospecimen_ID"]):
            current_object.check_if_substr(data_table,"Biospecimen_ID","Aliquot_ID",file_name,header_name)
        current_object.check_for_dup_ids(file_name,header_name)   
    elif (header_name in ["Assay_ID"]):
        pattern_str = '[_]{1}[0-9]{3}$'
        current_object.check_id_field(file_name,data_table,re,header_name,pattern_str,valid_cbc_ids,"XX_XXX")
        current_object.check_assay_special(data_table,header_name,file_name,"Assay_Name")
        if (file_name in ["assay.csv"]):
            current_object.check_for_dup_ids(file_name,header_name)
    else:
        Rule_Found[index] = False
  
    return Required_column,Rule_Found
def check_prior_clinical(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,index,Required_column = "Yes"):
    if header_name in ['SARS_CoV_2_PCR_Test_Result_Provenance']:
        list_values = ['From Medical Record','Self-Reported']
        current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
    elif header_name in ['SARS_CoV_2_PCR_Test_Result']:
        list_values = ['Positive', 'Negative']
        current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
    elif header_name in 'Date_of_SARS_CoV_2_PCR_sample_collection':    
        current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Date",min_date,max_date)
    elif 'Test_Result_Provenance' in header_name:     #checks result proveance for valid input options
        Required_column = "Yes: SARS-Negative"
        list_values = ['Self-Reported','From Medical Record','N/A']
        current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
    elif ('Date_of' in header_name) and ('Test' in header_name): 
        Required_column = "No"
        current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",True,"Date",min_date,max_date)

    elif ('Test_Result' in header_name) or (header_name in ["Seasonal_Coronavirus_Serology_Result","Seasonal_Coronavirus_Molecular_Result"]):
        Required_column = "Yes: SARS-Negative"
        pos_list = ['Positive','Negative','Equivocal','Not Performed','N/A']
        neg_list = ['Positive','Negative','Equivocal','Not Performed']
        current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Positive"],pos_list)
        current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Negative"],neg_list)  
    elif ('infection_unit' in header_name) or ('HAART_Therapy_unit' in header_name):
        Required_column = "No"
        duration_name = header_name.replace('_unit','')
        current_object.check_in_list(pd,file_name,data_table,header_name,duration_name,"Is A Number",["Day","Month","Year"])
        current_object.check_in_list(pd,file_name,data_table,header_name,duration_name,["N/A"],["N/A"])
    elif ('Duration_of' in header_name) and (('infection' in header_name) or ("HAART_Therapy" in header_name)):
        Required_column = "No"
        if 'HAART_Therapy' in header_name:
            current_name = 'On_HAART_Therapy'
        else:
            current_name = header_name.replace('Duration_of','Current')
        current_object.check_in_list(pd,file_name,data_table,header_name,current_name,['No','Unknown','N/A'],["N/A"])
        current_object.check_if_number(pd,file_name,data_table,header_name,current_name,['Yes'],False,0,365,"int")
    elif (('Current' in header_name) and ('infection' in header_name)) or (header_name in ["On_HAART_Therapy"]):
        Required_column = "Yes: SARS-Negative"            
        current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Positive"],['Yes','No','Unknown','N/A'])
        current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Negative"],['Yes','No','Unknown'])
    else:
        Rule_Found[index] = False
    return Required_column,Rule_Found
def check_demographic(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,index,Required_column = "Yes"):
    if (header_name in ['Age']):
        current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",False,1,200,"int")
    elif (header_name in ['Race','Ethnicity','Gender']):
        if (header_name in ['Race']):
            list_values =  ['White', 'American Indian or Alaska Native', 'Black or African American', 'Asian',
                            'Native Hawaiian or Other Pacific Islander', 'Other', 'Multirace','Not Reported', 'Unknown']
        elif (header_name in ['Ethnicity']):
            list_values = ['Hispanic or Latino','Not Hispanic or Latino']
        elif (header_name in ['Gender']):
            list_values = ['Male', 'Female', 'Other','Not Reported', 'Unknown']
        current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
    elif (header_name in ['Is_Symptomatic']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Positive"],['Yes','No'])
        current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Negative"],['No','N/A'])
    elif (header_name in ['Date_of_Symptom_Onset']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_date(pd,datetime,file_name,data_table,header_name,"Is_Symptomatic",["Yes"],False,"Date",min_date,max_date)
        current_object.check_in_list(pd,file_name,data_table,header_name,"Is_Symptomatic",["No","N/A"],["N/A"])
    elif (header_name in ['Symptoms_Resolved']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_in_list(pd,file_name,data_table,header_name,"Is_Symptomatic",["Yes"],["Yes","No"])
        current_object.check_in_list(pd,file_name,data_table,header_name,"Is_Symptomatic",["No","N/A"],["N/A"])
    elif (header_name in ['Date_of_Symptom_Resolution']):
        Required_column = "Yes: SARS-Positive"
        current_object.check_date(pd,datetime,file_name,data_table,header_name,"Symptoms_Resolved",["Yes"],False,"Date",min_date,max_date)
        current_object.check_in_list(pd,file_name,data_table,header_name,"Symptoms_Resolved",["No","N/A"],["N/A"])
    elif (header_name in ['Covid_Disease_Severity']):
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
    else:
        Rule_Found[index] = False
    return Required_column,Rule_Found
def check_biospecimen(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,index,Required_column = "Yes"):
    if(header_name in ["Biospecimen_Group"]):
        current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Positive"],['Positive Sample'])
        current_object.check_in_list(pd,file_name,data_table,header_name,'SARS_CoV_2_PCR_Test_Result',["Negative"],['Negative Sample'])
    elif(header_name in ["Biospecimen_Type"]):
        list_values = ["Serum", "EDTA Plasma", "PBMC", "Saliva", "Nasal swab"]
        current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
    elif(header_name in ["Initial_Volume_of_Biospecimen"]):
        current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",True,0,1e9,"float")   
    elif (header_name in ['Collection_Tube_Type_Expiration_Date']):
        Required_column = "No"
        current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Date",max_date,datetime.date(3000, 1, 1))
    elif ((header_name.find('Company_Clinic') > -1) or (header_name.find('Initials') > -1) or (header_name.find('Collection_Tube_Type') > -1)):
        if (header_name in ['Collection_Tube_Type_Lot_Number']):
            Required_column = "No"
        current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",False)
    elif ('Date_of' in header_name):
        current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Date",min_date,max_date)  
    elif 'Time_of' in header_name:
        current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Time")
    elif(header_name in ["Storage_Time_at_2_8"]):
        current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",True,0,1000,"float")
    elif(header_name in ["Storage_Start_Time_at_2_8_Initials","Storage_End_Time_at_2_8_Initials"]):
        current_object.check_if_string(pd,file_name,data_table,header_name,"Storage_Time_at_2_8","Is A Number",False)
        current_object.check_in_list(pd,file_name,data_table,header_name,"Storage_Time_at_2_8",["N/A"],['N/A'])
    elif(header_name in ["Storage_Start_Time_at_2_8","Storage_End_Time_at_2_8"]):
         current_object.check_date(pd,datetime,file_name,data_table,header_name,"Storage_Time_at_2_8","Is A Number",False,"Date",min_date,max_date)
         current_object.check_in_list(pd,file_name,data_table,header_name,"Storage_Time_at_2_8",["N/A"],['N/A'])
    elif(header_name in ["Final_Concentration_of_Biospecimen"]) or (header_name.find('Hemocytometer_Count') > -1) or (header_name.find('Automated_Count') > -1):
        current_object.check_if_number(pd,file_name,data_table,header_name,"Biospecimen_Type",["PBMC"],True,0,1e9,"float")
    elif(header_name in ["Centrifugation_Time","RT_Serum_Clotting_Time"]):
        current_object.check_if_number(pd,file_name,data_table,header_name,"Biospecimen_Type",["Serum"],True,0,1e9,"float")
    elif(header_name in ["Storage_Start_Time_80_LN2_storage"]):
         current_object.check_date(pd,datetime,file_name,data_table,header_name,"Biospecimen_Type",["Serum"],False,"Time")
    else:
        Rule_Found[index] = False
    return Required_column,Rule_Found
def check_processing_rules(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,index,Required_column = "Yes"):
    if (header_name in ["Aliquot_Volume"]):
        current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",True,0,1e9,"float")
    elif ('Expiration_Date' in header_name) or ('Calibration_Due_Date' in header_name):
         Required_column = "No"
         current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Date",max_date,datetime.date(3000, 1, 1)) 
    elif ('Lot_Number' in header_name) or ('Catalog_Number' in header_name):
         Required_column = "No"
         current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",False)
    elif (header_name in ["Equipment_Type","Reagent_Name","Consumable_Name"]):
        if (header_name in ["Equipment_Type"]):
            list_values = ['Refrigerator','-80 Refrigerator', 'LN Refrigerator', 'Microsope', 'Pipettor', 'Controlled-Rate Freezer', 'Automated-Cell Counter']
        elif (header_name in ["Reagent_Name"]):
            list_values =  (['DPBS', 'Ficoll-Hypaque','RPMI-1640','no L-Glutamine','Fetal Bovine Serum','200 mM L-Glutamine',
                             '1M Hepes','Penicillin/Streptomycin','DMSO', 'Cell Culture Grade','Vital Stain Dye'])
        elif (header_name in ["Consumable_Name"]):
            list_values = ["50 mL Polypropylene Tube", "15 mL Conical Tube" ,"Cryovial Label"]
        current_object.check_in_list(pd,file_name,data_table,header_name,"Biospecimen_Type",["PBMC"],list_values)           
    elif ("Aliquot" in header_name) or ("Equipment_ID" in header_name):
        current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",False)
    else:
        Rule_Found[index] = False
    return Required_column,Rule_Found
def check_confimation_rules(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,index,Required_column = "Yes"):
    if header_name in ["Assay_Target"]:
        current_object.check_assay_special(data_table,header_name,file_name,"Assay_Antigen_Source")
    elif (header_name in ["Instrument_ID","Test_Operator_Initials","Assay_Kit_Lot_Number"]):
        current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",False)
    elif ('Date_of' in header_name):
        current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Date",min_date,max_date)  
    elif 'Time_of' in header_name:
        current_object.check_date(pd,datetime,file_name,data_table,header_name,"None","None",False,"Time")
    elif (header_name in ["Assay_Target_Sub_Region","Measurand_Antibody","Interpretation"]):
        current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",False)
    elif (header_name in ["Assay_Replicate","Sample_Dilution"]):
        current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",False,0,200,"int")
    elif (header_name in ["Derived_Result","Raw_Result","Positive_Control_Reading","Negative_Control_Reading"]):
        current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",True,0,1e9,"float")
    elif header_name in ["Sample_Type"]:
        list_values = ['Serum','Plasma','Venous Whole Blood','Dried Blood Spot','Nasal Swab','Broncheolar Lavage','Sputum']
        current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
    elif header_name in ["Derived_Result_Units"]:
         current_object.check_if_string(pd,file_name,data_table,header_name,"Derived_Result","Is A Number",False)
         current_object.check_in_list(pd,file_name,data_table,header_name,"Derived_Result",["N/A"],["N/A"])
    elif header_name in ["Raw_Result_Units"]:
         current_object.check_if_string(pd,file_name,data_table,header_name,"Raw_Result","Is A Number",False)
         current_object.check_in_list(pd,file_name,data_table,header_name,"Raw_Result",["N/A"],["N/A"])
    else:
        Rule_Found[index] = False
    return Required_column,Rule_Found
def check_assay_rules(header_name,current_object,pd,data_table,file_name,datetime,min_date,max_date,Rule_Found,index,Required_column = "Yes"):
    if (header_name in ["Technology_Type","Assay_Name","Assay_Manufacturer","Target_Organism"]):
        current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",False)
    elif (header_name in ["EUA_Status","Assay_Multiplicity","Assay_Control_Type","Measurand_Antibody_Type","Assay_Result_Type",
                          "Peformance_Statistics_Source","Assay_Antigen_Source"]):
        if (header_name in ["EUA_Status"]):
            list_values = ['Approved','Submitted','Not Submitted','N/A']
        if (header_name in ["Assay_Multiplicity"]):
            list_values =  ['Multiplex', 'Singleplex']
        if (header_name in ["Assay_Control_Type"]):
            list_values =  ['Internal', 'External', 'Internal and External', 'N/A']
        if (header_name in ["Measurand_Antibody_Type"]):
            list_values = ['IgG', 'IgM', 'IgA' ,'IgG + IgM', 'Total', 'N/A']
        if (header_name in ["Assay_Result_Type"]):   
            list_values =  ['Qualitative','Quantitative', 'Semi-Quantitative']
        if (header_name in ["Peformance_Statistics_Source"]):
            list_values = ['Manufacturer', 'In-house']
        if (header_name in ["Assay_Antigen_Source"]):
            list_values = ['Manufacturer', 'In-house','N/A']
        current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",list_values)
    elif ("Target_biospecimen_is_" in header_name):
        current_object.check_in_list(pd,file_name,data_table,header_name,"None","None",["T","F"])
    elif (header_name in ["Postive_Control","Negative_Control","Calibration_Type","Calibrator_High_or_Positive","Calibrator_Low_or_Negative"]):
        current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",True)
    elif (header_name in ["Assay_Result_Unit","Cut_Off_Unit","Assay_Target"]):
        current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",False)
    elif (header_name in ["Positive_Cut_Off_Threshold","Negative_Cut_Off_Ceiling","Assay_Target_Sub_Region"]):
        current_object.check_if_string(pd,file_name,data_table,header_name,"None","None",True)
    elif (header_name in ["N_true_positive","N_true_negative","N_false_positive","N_false_negative"]):
        current_object.check_if_number(pd,file_name,data_table,header_name,"None","None",False,0,1e9,"int")
    else:
        Rule_Found[index] = False
    return Required_column,Rule_Found