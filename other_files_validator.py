def other_files_validator(current_object,prior_cov_test,biospec_ids,re,pd,valid_cbc_ids):
    current_object.Data_Table = current_object.Data_Table.merge(biospec_ids,how='left',on="Biospecimen_ID")
    current_object.All_Error_DF = pd.DataFrame(columns=current_object.error_list_summary)

    if current_object.File_name in ["equipment.csv","reagent.csv","consumable.csv"]:
        warning_data = current_object.Data_Table[current_object.Data_Table['Biospecimen_Type'] != "PBMC"]
        if len(warning_data)> 0:
            warning_data = current_object.filter_data_table(warning_data,'Biospecimen_Type',pd,"Has_Data")
            Error_Message = "Data is only expected for Biospecimen Type == PBMC.  Check to ensure this type is correct"
            current_object.add_error_values(pd,warning_data,Error_Message,"Warning")
        current_object.Data_Table =  current_object.Data_Table[current_object.Data_Table['Biospecimen_Type'] == "PBMC"]
#################################################################################################################################################
    for header_name in current_object.Column_Header_List:
        Required_column = "Yes"
#################################################################################################################################################
        if (header_name in ["Biospecimen_ID"]):
            pattern_str = '[_]{1}[0-9]{6}[_]{1}[0-9]{3}$$'
            current_object.check_id_field(pd,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX_XXX",True)
            current_object.check_id_cross_sheet(pd,header_name,"Biospecimen_Type",current_object.File_name,"biospecimen")
#################################################################################################################################################
        elif (header_name in ["Aliquot_ID"]):
            pattern_str = '[_]{1}[0-9]{6}[_]{1}[0-9]{3}[_]{1}[0-9]{2}$$'
            current_object.check_id_field(pd,re,header_name,pattern_str,valid_cbc_ids,"XX_XXXXXX_XXX_XX",False)
#################################################################################################################################################
        elif (header_name in ["Aliquot_Volume"]):
            Error_Message = "Value must be a number greater than 0"
            current_object.check_if_number(pd,current_object.Data_Table,header_name,1,1e9,"All",False,Error_Message)
#################################################################################################################################################
        elif ((header_name.find('Expiration_Date') > -1) or (header_name.find('Due_Date') > -1)):
            Error_Message = "Value must be a valid Date MM/DD/YYYY"
            current_object.check_date(pd,current_object.Data_Table,header_name,False,Error_Message)
#################################################################################################################################################
        elif ("Catalog_Number") in header_name or ("Lot_Number") in header_name or ("Aliquot" in header_name) or ("Equipment_ID" in header_name):
            Error_Message = "Value must be a string and NOT N/A"
            current_object.check_if_str(pd,current_object.Data_Table,header_name,Error_Message)
#################################################################################################################################################
        elif (header_name in ["Equipment_Type","Reagent_Name","Consumable_Name"]):
            if (header_name in ["Equipment_Type"]):
                list_values = ['Refrigerator','-80 Refrigerator', 'LN Refrigerator', 'Microsope', 'Pipettor', 'Controlled-Rate Freezer', 'Automated-Cell Counter']
            elif (header_name in ["Reagent_Name"]):
                list_values =  (['DPBS', 'Ficoll-Hypaque','RPMI-1640','no L-Glutamine','Fetal Bovine Serum','200 mM L-Glutamine',
                                 '1M Hepes','Penicillin/Streptomycin','DMSO', 'Cell Culture Grade','Vital Stain Dye'])
            elif (header_name in ["Consumable_Name"]):
                list_values = ["50 mL Polypropylene Tube", "15 mL Conical Tube" ,"Cryovial Label"]
            current_object.check_in_list(pd,header_name,[list_values])
#################################################################################################################################################
        else:
            print("Column_Name: " + header_name + " has no validation rules set")
#################################################################################################################################################
        current_object.get_missing_values(pd,header_name,Required_column)
#################################################################################################################################################
    if current_object.File_name in ["aliquot.csv"]:
        id_compare = current_object.Data_Table[current_object.Data_Table.apply(lambda x: x["Biospecimen_ID"] not in x["Aliquot_ID"],axis = 1)]
        Error_Message = "Biospecimen_ID is not a substring of Aliquot ID.  Data is not Valid, please check data"
        current_object.get_duration_errors(pd,id_compare,'Biospecimen_ID',Error_Message)
#################################################################################################################################################
    return current_object