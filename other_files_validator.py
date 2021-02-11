def other_files_validator(data_object,re,valid_cbc_ids,biospec_ids):
    biospec_ids.drop_duplicates(inplace=True)
    data_object.Data_Table = data_object.Data_Table.merge(biospec_ids, on=["Biospecimen_ID"],how="left")
    if data_object.File_name in ["equipment.csv","reagent.csv","consumable.csv"]:
        try:
            warning_data = data_object.Data_Table[data_object.Data_Table['Biospecimen_Type'] != "PBMC"]
            for curr_type in warning_data['Biospecimen_Type']:
                curr_type = warning_data['Biospecimen_Type'][warning_data.index[i[0]]]
                if curr_type == curr_type:
                   error_msg ="Unable to Validate, Expecting Biospecimen Type to be PMBC, Was found to be " + curr_type +" instead, please check data"
                elif curr_type != curr_type:        #value is missing, NULL cell when merged
                    error_msg ="Biospecimen ID not found, unable to determine Biospecimen Type. Please check data"
                data_object.write_error_msg("","All Columns",error_msg,warning_data.index[i[0]],'Warning')
        except Exception as e:
            print(e)    
#################################################################################################################################################    
    for header_name in data_object.Column_Header_List:
        test_column = data_object.Data_Table[header_name]
        has_data,missing_data = data_object.check_data_type_v2(test_column)
        Required_column = "Yes"
#################################################################################################################################################
        if (header_name in ["Biospecimen_ID"]):
            error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX_XXX"
            pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}[_]{1}[0-9]{3}$')
            for i in range(len(has_data)):
                data_object.valid_ID(header_name,has_data.values[i],pattern,valid_cbc_ids,error_msg,has_data.index[i],'Error')
            
            id_error_list = [i[6] for i in data_object.error_list_summary if (i[0] == "Error") and (i[5] == "Biospecimen_ID")]
            matching_values = [i for i in enumerate(test_column) if (pattern.match(i[1]) is not None) and (i[1] not in id_error_list)]
            if (len(matching_values) > 0):
                error_msg = "Id is not found in database or in submitted Biospecimen file"
                for i in enumerate(matching_values):
                    data_object.in_list(header_name,i[1][1],biospec_ids['Biospecimen_ID'].tolist(),error_msg,i[1][0],'Error')
#################################################################################################################################################
        elif (header_name in ["Aliquot_ID"]):
            error_msg = "Value it not a Valid id format, Expecting XX_XXXXXX_XXX_XX"
            pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}[_]{1}[0-9]{3}[_]{1}[0-9]{2}$')
            for i in range(len(has_data)):
                data_object.valid_ID(header_name,has_data.values[i],pattern,valid_cbc_ids,error_msg,has_data.index[i],'Error')
#################################################################################################################################################
        elif (header_name in ["Aliquot_Volume"]):
            error_msg = "Value must be a number greater than zero"
            for i in range(len(has_data)):
                data_object.is_numeric(header_name,False,has_data.values[i],0,error_msg,has_data.index[i],'Error')
#################################################################################################################################################
        elif ((header_name.find('Expiration_Date') > -1) or (header_name.find('Due_Date') > -1)):
            if (header_name not in ["Aliquot_Tube_Type_Expiration_Date"]):
                has_data_column = data_object.has_pmbc_data(header_name,has_data)
            else:
                has_data_column = has_data
            error_msg = "Value must be a valid Date MM/DD/YYYY"
            for i in range(len(has_data_column)):
                data_object.is_date_time(header_name,has_data_column.values[i],False,error_msg,has_data_column.index[i],'Error')
#################################################################################################################################################
        elif (header_name.find('Aliquot_Tube_Type') > -1) or (header_name.find('Aliquot_Initials') > -1):
            error_msg = "Value must be a string and NOT N/A"
            for i in range(len(has_data)):
                data_object.is_string(header_name,has_data.values[i],False,error_msg,has_data.index[i],'Error')
#################################################################################################################################################
        elif (header_name in ["Equipment_ID"] or (header_name.find('Catalog_Number') > -1) or (header_name.find('Lot_Number') > -1)):
            has_data_column = data_object.has_pmbc_data(header_name,has_data)
            error_msg = "Value must be a string and NOT N/A"
            for i in range(len(has_data_column)):
                data_object.is_string(header_name,has_data_column.values[i],False,error_msg,has_data_column.index[i],'Error')
#################################################################################################################################################
        elif (header_name in ["Equipment_Type","Reagent_Name","Consumable_Name"]):
            if (header_name in ["Equipment_Type"]):
                test_string = ['Refrigerator','-80 Refrigerator', 'LN Refrigerator', 'Microsope', 'Pipettor', 'Controlled-Rate Freezer', 'Automated-Cell Counter']
            elif (header_name in ["Reagent_Name"]):
                test_string =  (['DPBS', 'Ficoll-Hypaque','RPMI-1640','no L-Glutamine','Fetal Bovine Serum','200 mM L-Glutamine',
                                 '1M Hepes','Penicillin/Streptomycin','DMSO', 'Cell Culture Grade','Vital Stain Dye'])
            elif (header_name in ["Consumable_Name"]):
                test_string = ["50 mL Polypropylene Tube", "15 mL Conical Tube" ,"Cryovial Label"] 
            has_data_column = data_object.has_pmbc_data(header_name,has_data)
            error_msg = "Value must be one of the following: " + str(test_string)
            for i in range(len(has_data_column)):
                data_object.in_list(header_name,has_data_column.values[i],test_string,error_msg,has_data_column.index[i],'Error')
###############################################################################################################################
        data_object.missing_data_errors_v2(Required_column,header_name,missing_data)
    if data_object.File_name in ["aliquot.csv"]:  
        test_column = data_object.Data_Table["Biospecimen_ID"]
        pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}[_]{1}[0-9]{3}$')
        matching_BIO_values = [i for i in enumerate(test_column) if pattern.match(i[1]) is not None]

        test_column = data_object.Data_Table["Aliquot_ID"]
        pattern = re.compile('^[0-9]{2}[_]{1}[0-9]{6}[_]{1}[0-9]{3}[_]{1}[0-9]{2}$')
        matching_ALI_values = [i for i in enumerate(test_column) if pattern.match(i[1]) is not None]
        ALI_index,ALI_Value = map(list,zip(*matching_ALI_values))

        for i in enumerate(matching_BIO_values):
            if i[1][0] in ALI_index:
                if ALI_Value[ALI_index.index(i[1][0])].find(i[1][1]) == -1:
                    error_msg = "Biospecimen_ID ("+ i[1][1] +")does not agree with Aliquot ID(" + ALI_Value[ALI_index.index(i[1][0])] + "), first 13 characters should match"
                    data_object.write_error_msg(i[1][1],'Biospecimen_ID',error_msg,i[1][0],'Error')
#################################################################################################################################################
    return data_object