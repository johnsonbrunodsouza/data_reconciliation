import numpy as np
import itertools
import os
import pandas as pd
import json
import dask.dataframe as dask_dd
import sys
from collections import OrderedDict
import ntpath
###
CSV_ENCODING  = "ISO-8859-1"
####
FILE_TYPE_CSV = 'CSV'
FILE_TYPES_EXCEL = ['XLS','XLSX']

def get_partition_info(input_file,delimiter,column_num,output_file,is_header=True):
    row_count = OrderedDict()
    null_count = 0

    with open(input_file,encoding=CSV_ENCODING) as in_handler:
        for line in in_handler:
            # skip header
            if is_header:
                is_header = False
                continue

            columns = line.split(delimiter)
            if len(columns) >= column_num:
                column_value = columns[column_num-1]
                if len(column_value) > 0:
                    if column_value in row_count:
                        row_count[column_value] = row_count[column_value] + 1
                    else:
                        row_count[column_value] = 1
                else:
                    null_count = null_count + 1

    row_count['NULL_KEY'] = null_count

    #with open(output_file,"w") as out_handler:
    #    for key,value in row_count.items():
    #        out_handler.write(str(key) + "\t" + str(value) + "\n")
    return row_count

def partition_data_files(key_position,common_keys,source_primary_keys_dict,target_primary_keys_dict,config,temp_dir,header_exists=True):
    file_counter = 1
    partition_size = 20001
    buffer = []

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    header = ''
    extracted_keys = []

    with open(config.source_file,encoding=CSV_ENCODING) as in_handler:
        for line in in_handler:

            if header_exists:
                header = line
                buffer.append(line)
                header_exists = False

            columns = line.split(config.source_file_delimiter)
            columns_len = len(columns)
            if columns_len >= key_position:
                column_value = columns[key_position-1]

                if column_value in common_keys:
                    key_count = source_primary_keys_dict.get(column_value,-1)

                    if key_count > 0:
                        buffer.append(line)
                        if key_count > 1:
                            source_primary_keys_dict[column_value] = source_primary_keys_dict[column_value] - 1
                        else:
                            del source_primary_keys_dict[column_value]
                            extracted_keys.append(column_value)
                    
                    if len(buffer) == partition_size:
                        temp_file = os.path.join(temp_dir,"Source_" + str(file_counter) + ".csv")
                        with open(temp_file,encoding=CSV_ENCODING,mode="w") as output:
                            output.writelines(buffer)
                        buffer.clear()
                        buffer.append(header)
                        extract_from_target_file(key_position,extracted_keys,target_primary_keys_dict,config,temp_dir,file_counter)
                        file_counter =  file_counter + 1

def partition_data_files_2(key_position,common_keys,source_primary_keys_dict,target_primary_keys_dict,config,temp_dir,header_exists=True):
    file_counter = 1
    partition_size = 5000
    buffer = []

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    header = ''
    extracted_keys = []
    keys = []

    with open(config.source_file,encoding=CSV_ENCODING) as in_handler:
        for line in in_handler:

            if header_exists:
                header = line
                buffer.append(line)
                header_exists = False
                continue

            columns = line.split(config.source_file_delimiter)
            columns_len = len(columns)
            if columns_len >= key_position:
                column_value = columns[key_position-1]

                if column_value in common_keys:
                    key_count = source_primary_keys_dict.get(column_value,-1)

                    if key_count > 0:
                        buffer.append(line)
                        if key_count > 1:
                            source_primary_keys_dict[column_value] = source_primary_keys_dict[column_value] - 1
                        else:
                            del source_primary_keys_dict[column_value]
                            keys.append(column_value)
                    
                    if len(buffer) == partition_size:
                        temp_file = os.path.join(temp_dir,"Source_" + str(file_counter) + ".csv")
                        with open(temp_file,encoding=CSV_ENCODING,mode="w") as output:
                            output.writelines(buffer)
                        buffer.clear()
                        buffer.append(header)
                        extracted_keys.append( (file_counter,keys))
                        keys = []
                        file_counter =  file_counter + 1

    extract_from_target_file_2(key_position,extracted_keys,target_primary_keys_dict,config,temp_dir)

def extract_from_target_file_2(key_position,extracted_keys,target_primary_keys_dict,config,temp_dir,header_exists=True):
    file_dump_dict = {}
    #buffer = []
    header = ''

    with open(config.target_file ,encoding=CSV_ENCODING) as in_handler:
        for line in in_handler:

            if header_exists:
                header = line
                header_exists = False
                continue

            columns = line.split(config.target_file_delimiter)
            columns_len = len(columns)
            if columns_len >= key_position:
                column_value = columns[key_position-1]

                for key_tuple in extracted_keys:
                    if column_value in key_tuple[1]:
                        key_count = target_primary_keys_dict.get(column_value,-1)
                
                        if key_count > 0:
                            if key_tuple[0] in file_dump_dict:
                                file_dump_dict[key_tuple[0]].append(line)
                            else:
                                file_dump_dict[key_tuple[0]] = []
                                file_dump_dict[key_tuple[0]].append(header)
                                file_dump_dict[key_tuple[0]].append(line)

                            if key_count > 1:
                                target_primary_keys_dict[column_value] = target_primary_keys_dict[column_value] - 1
                            else:
                                del target_primary_keys_dict[column_value]
                                del key_tuple[1][ key_tuple[1].index(column_value) ]

                                if len(key_tuple[1]) == 0:
                                    temp_file = os.path.join(temp_dir,"Target_" + str(key_tuple[0]) + ".csv")
                                    with open(temp_file,encoding=CSV_ENCODING,mode="w") as output:
                                        output.writelines(file_dump_dict[key_tuple[0]])
                                        file_dump_dict[key_tuple[0]].clear()
                        break    

def extract_from_target_file(key_position,extracted_keys,target_primary_keys_dict,config,temp_dir,file_counter,header_exists=True):
    buffer = []

    with open(config.target_file ,encoding=CSV_ENCODING) as in_handler:
        for line in in_handler:

            if header_exists:
                buffer.append(line)
                header_exists = False

            columns = line.split(config.target_file_delimiter)
            columns_len = len(columns)
            if columns_len >= key_position:
                column_value = columns[key_position-1]

                if column_value in extracted_keys:
                    key_count = target_primary_keys_dict.get(column_value,-1)

                    if key_count > 0:
                        buffer.append(line)
                        if key_count > 1:
                            target_primary_keys_dict[column_value] = target_primary_keys_dict[column_value] - 1
                        else:
                            del target_primary_keys_dict[column_value]
                            del extracted_keys[ extracted_keys.index(column_value) ]
                            
            if len(extracted_keys) == 0:
                temp_file = os.path.join(temp_dir,"Target_" + str(file_counter) + ".csv")
                with open(temp_file,encoding=CSV_ENCODING,mode="w") as output:
                    output.writelines(buffer)
                return

def is_subkey(newkey,keys):
    for key in keys:
        if set(key).issubset(newkey):
            return True
    return False

def get_primarykey_columns(dataframe):
    bool_sum = -1
    p_keys = []
    for i in range(1,4):
        columns = list(itertools.combinations(dataframe.columns,i))
        for key in columns:
            if not is_subkey(key,p_keys):
                bools = np.array(dataframe.duplicated(subset=list(key)))
                if bool_sum == -1 or np.sum(bools) < bool_sum:
                    bool_sum = np.sum(bools)
                    p_keys=list(key)
    return p_keys

def get_datafile_type(filename):
    _,extension = os.path.splitext(filename)
    return extension.replace('.','')

def get_dataframe(filename,delimiter,header):
    ### revisit
    default_delimiter = ''
    default_header = 'infer'

    if delimiter != '':
        default_delimiter = delimiter
    
    if header == '' or header.upper() == 'NO':
        default_header = None

    if get_datafile_type(filename).upper() == FILE_TYPE_CSV:
        df = pd.read_csv(filename,delimiter=default_delimiter,header=default_header,encoding = CSV_ENCODING)
        df.dropna(axis=0,how='any',thresh=None,subset=None,inplace=True)
        return df
    ### revisit
    if get_datafile_type(filename).upper() in FILE_TYPES_EXCEL:
        return pd.read_excel(filename)
    
    return None

def get_dataframe2(filename,delimiter,header='Yes'):
    
    default_delimiter = ''
    default_header = 'infer'

    if delimiter != '':
        default_delimiter = delimiter
    
    if header == '' or header.upper() == 'NO':
        default_header = None

    if get_datafile_type(filename).upper() == FILE_TYPE_CSV:
        return dask_dd.read_csv(filename,delimiter=default_delimiter,header=default_header,encoding = CSV_ENCODING)
    if get_datafile_type(filename).upper() in FILE_TYPES_EXCEL:
        return pd.read_excel(filename)
    
    return None

def add_header(dataframe):
    header = [ 'Col_' + str(i) for i in range(1, len(dataframe.columns) +1) ]
    dataframe.columns = header
    return dataframe

def resolve_column_names(dataframe,values):
    columns = []
    for item in values:
        if item in dataframe.keys():
            columns.append(item)
        elif item < len(dataframe.keys()):
            columns.append( dataframe.keys()[item - 1])
        else:
            ### need to add exception
            pass
    return columns

def get_summary(filename,dataframe):
    summary = {}
    summary['File name'] = filename
    summary['Total rows'] = dataframe.shape[0]
    summary['Total Columns']  = dataframe.shape[1]


    for column in list(dataframe.columns):
        column_summary = {}

        duplicates = np.sum(np.array(dataframe.duplicated(subset=column)))
        column_summary['duplicates'] = duplicates
        column_summary['null'] = dataframe[column].isnull().sum()

        summary[column] = column_summary

    return summary

def find_differences_2(source_df,target_df,source_primary_keys):
    """
    """
    sdf_unique_rows = source_df.drop_duplicates(source_primary_keys, keep='first')#
    sdf_ml_rows = pd.MultiIndex.from_arrays([sdf_unique_rows[col] for col in source_primary_keys])#

    tdf_unique_rows = target_df.drop_duplicates(source_primary_keys, keep='first')
    tdf_ml_rows = pd.MultiIndex.from_arrays([tdf_unique_rows[col] for col in source_primary_keys])

    sdf_drop_duplicates = source_df.drop_duplicates(source_primary_keys, keep=False)#1

    sdf_dd_rows = pd.MultiIndex.from_arrays([sdf_drop_duplicates[col] for col in source_primary_keys])

    rows_to_insert = sdf_unique_rows.loc[~sdf_ml_rows.isin(tdf_ml_rows)]#
    rows_to_delete = tdf_unique_rows.loc[~tdf_ml_rows.isin(sdf_ml_rows)]

    sdf_rows_to_update = sdf_unique_rows.loc[sdf_ml_rows.isin(tdf_ml_rows)]
    tdf_rows_to_update = tdf_unique_rows.loc[tdf_ml_rows.isin(sdf_ml_rows)]

    sdf_duplicate_rows = sdf_unique_rows.loc[~sdf_ml_rows.isin(sdf_dd_rows)]
    sdf_duplicate_rows['flag'] = 'DUP'

    sdf_rows_to_update = sdf_rows_to_update.sort_values(source_primary_keys)
    tdf_rows_to_update = tdf_rows_to_update.sort_values(source_primary_keys)

    rows_to_insert.insert(0, 'flag', 'I')
    rows_to_delete.insert(0, 'flag', 'D')
    sdf_rows_to_update.insert(0, 'flag', 'U')
    tdf_rows_to_update.insert(0, 'flag', 'U')

    return rows_to_insert,rows_to_delete,sdf_rows_to_update,tdf_rows_to_update,sdf_duplicate_rows

def find_differences(source_df,target_df,source_primary_keys):
    """
    """
    sdf_unique_rows = source_df.drop_duplicates(source_primary_keys, keep='first')
    sdf_ml_rows = pd.MultiIndex.from_arrays([sdf_unique_rows[col] for col in source_primary_keys])

    tdf_unique_rows = target_df.drop_duplicates(source_primary_keys, keep='first')
    tdf_ml_rows = pd.MultiIndex.from_arrays([tdf_unique_rows[col] for col in source_primary_keys])

    sdf_drop_duplicates = source_df.drop_duplicates(source_primary_keys, keep=False)

    sdf_dd_rows = pd.MultiIndex.from_arrays([sdf_drop_duplicates[col] for col in source_primary_keys])

    rows_to_insert = sdf_unique_rows.loc[~sdf_ml_rows.isin(tdf_ml_rows)]
    rows_to_delete = tdf_unique_rows.loc[~tdf_ml_rows.isin(sdf_ml_rows)]

    sdf_rows_to_update = sdf_unique_rows.loc[sdf_ml_rows.isin(tdf_ml_rows)]
    tdf_rows_to_update = tdf_unique_rows.loc[tdf_ml_rows.isin(sdf_ml_rows)]

    sdf_duplicate_rows = sdf_unique_rows.loc[~sdf_ml_rows.isin(sdf_dd_rows)]
    sdf_duplicate_rows['flag'] = 'DUP'

    sdf_rows_to_update = sdf_rows_to_update.sort_values(source_primary_keys)
    tdf_rows_to_update = tdf_rows_to_update.sort_values(source_primary_keys)

    rows_to_insert.insert(0, 'flag', 'I')
    rows_to_delete.insert(0, 'flag', 'D')
    sdf_rows_to_update.insert(0, 'flag', 'U')
    tdf_rows_to_update.insert(0, 'flag', 'U')

    return rows_to_insert,rows_to_delete,sdf_rows_to_update,tdf_rows_to_update,sdf_duplicate_rows

## revisit
## 
##

def resolve_cell_value(dataframe,row,col):
    cell_value = dataframe.iloc[row, col]
    if dataframe.iloc[row, col] == None:
        cell_value = '[NULL]'
    
    return cell_value
    
def reconcile(keys_to_ignore,rows_to_insert,rows_to_delete,sdf_rows_to_update,tdf_rows_to_update,sdf_duplicate_rows):
    df_row_reindex = pd.concat([sdf_duplicate_rows, rows_to_insert, rows_to_delete ], ignore_index=True)

    comparison_values = sdf_rows_to_update.values == tdf_rows_to_update.values

    rows, cols = np.where(comparison_values == False)

    for item in zip(rows, cols):
        if item[1] not in keys_to_ignore:
            
            #target_field = resolve_cell_value(tdf_rows_to_update,item[0],item[1])
            #source_field = sdf_rows_to_update.iloc[item[0], item[1]]
            #source_field = resolve_cell_value(sdf_rows_to_update,item[0],item[1])

            sdf_rows_to_update.iloc[item[0], item[1]] = '{} --> {}'.format(sdf_rows_to_update.iloc[item[0],item[1]], tdf_rows_to_update.iloc[item[0],item[1]])

    result = []
    update_count = 0
    for i in zip(rows, cols):
        if i[0] not in result and i[1] not in keys_to_ignore:
            result.append(i[0])
            df_row_reindex = pd.concat([df_row_reindex, sdf_rows_to_update.iloc[i[0]:i[0] + 1]], ignore_index=True)
            update_count+= 1
    return df_row_reindex