import json
import sys
from dc import api
from dc import Configuration
import logging
import copy
import pandas as pd
import os

### This should change to argv
configuration_file = "configuration.json"

### get configuration
def read_configuration(conf_file):
    try:
        with open(conf_file) as c_file:
            json_conf = json.load(c_file)
            config  = Configuration.Configuration(**json_conf)
    
        return config
    except:
        print("Exception:" + str(sys.exc_info()[1]))
    return None

### entry point function
def do_reconciliation(config):

    ############################################
    #Operation = config.operation
    #source_file_type = api.get_datafile_type(config.source_file)
    #target_file_type = api.get_datafile_type(config.target_file)
    ############################################ 
    logging.info("Started loading " + config.source_file + " into dataframe ")
    source_df = api.get_dataframe(config.source_file,config.source_file_delimiter,config.source_file_header)
    logging.info("loading complete")

    logging.info("Started loading " + config.target_file + " into dataframe ")
    target_df = api.get_dataframe(config.target_file,config.target_file_delimiter,config.target_file_header)
    logging.info("loading complete")

    source_primary_keys = ''
    if len(config.comparison_keys) > 0:
        logging.info("Comparison keys are:" + config.comparison_keys)
        source_primary_keys = api.resolve_column_names(source_df, config.comparison_keys)
        logging.info("Resolved source primary keys are:" + str(source_primary_keys))
    else:
        logging.info("Finding primary keys, scanning source dataframe")
        source_primary_keys = api.get_primarykey_columns(source_df)
        logging.info("Found keys:" + str(source_primary_keys))

    
    ### 
    source_ignore_keys = api.resolve_column_names(source_df,config.ignore_keys)

    logging.info("Find summary of source file")
    summary_dict = api.get_summary(config.source_file,source_df)
    logging.info("Summary of vital attributes:" + str(summary_dict))
    print(summary_dict)
            
    source_row_count = len(source_df)
    target_row_count = len(target_df)
    logging.info("Source row count:" + str(source_row_count))
    logging.info("Target row count:" + str(target_row_count))

    if source_row_count != 0 and target_row_count != 0:
        logging.info("Started finding differences...")
        rows_to_insert,rows_to_delete,sdf_rows_to_update,tdf_rows_to_update,sdf_duplicate_rows = api.find_differences(source_df,target_df,source_primary_keys)
        logging.info("completed finding differences")
                        
        keys_to_ignore = []
        for item in source_ignore_keys:
            keys_to_ignore.append(source_df.columns.get_loc(item))
        
        logging.info("Starting reconciliation...")
        df_row_reindex = api.reconcile(keys_to_ignore,rows_to_insert,rows_to_delete,sdf_rows_to_update,tdf_rows_to_update,sdf_duplicate_rows)
        logging.info("completed")

        logging.info("Writing result data to file...")
        df_row_reindex.to_csv(config.reconciliation_report,index=False) # dont need row numbers
        logging.info("completed")

@delayed
def do_reconciliation_2(source_file,target_file,reconciliation_report):
    logging.info("Started loading " + source_file + " into dataframe ")
    source_df = api.get_dataframe(source_file,',','Yes')
    logging.info("loading complete")

    logging.info("Started loading " + target_file + " into dataframe ")
    target_df = api.get_dataframe(target_file,',','Yes')
    logging.info("loading complete")

    source_primary_keys = ''
    comparison_keys = []
    if len(comparison_keys) > 0:
        logging.info("Comparison keys are:" + comparison_keys)
        source_primary_keys = api.resolve_column_names(source_df, comparison_keys)
        logging.info("Resolved source primary keys are:" + str(source_primary_keys))
    else:
        logging.info("Finding primary keys, scanning source dataframe")
        source_primary_keys = api.get_primarykey_columns(source_df)
        logging.info("Found keys:" + str(source_primary_keys))

    
    ### 
    ignore_keys = []
    source_ignore_keys = api.resolve_column_names(source_df,ignore_keys)

    logging.info("Find summary of source file")
    summary_dict = api.get_summary(source_file,source_df)
    logging.info("Summary of vital attributes:" + str(summary_dict))
    print(summary_dict)
            
    source_row_count = len(source_df)
    target_row_count = len(target_df)
    logging.info("Source row count:" + str(source_row_count))
    logging.info("Target row count:" + str(target_row_count))

    if source_row_count != 0 and target_row_count != 0:
        logging.info("Started finding differences...")
        rows_to_insert,rows_to_delete,sdf_rows_to_update,tdf_rows_to_update,sdf_duplicate_rows = api.find_differences(source_df,target_df,source_primary_keys)
        logging.info("completed finding differences")
                        
        keys_to_ignore = []
        for item in source_ignore_keys:
            keys_to_ignore.append(source_df.columns.get_loc(item))
        
        logging.info("Starting reconciliation...")
        df_row_reindex = api.reconcile(keys_to_ignore,rows_to_insert,rows_to_delete,sdf_rows_to_update,tdf_rows_to_update,sdf_duplicate_rows)
        logging.info("completed")

        logging.info("Writing result data to file...")
        df_row_reindex.to_csv(reconciliation_report,index=False) # dont need row numbers
        logging.info("completed")
    return 0 ## revisit

def mysum(l):
    return sum(l)

def previous_main():

    logging.info('Comparing ' +  config.source_file + " with " + config.target_file)

    logging.info("Extractiong primary key info from " +  config.source_file)
    source_primary_keys_dict = api.get_partition_info(config.source_file,config.source_file_delimiter,1,config.source_file+".primary_key.tsv")
    logging.info("completed")

    logging.info("Extractiong primary key info from " +  config.target_file)
    target_primary_keys_dict = api.get_partition_info(config.target_file,config.source_file_delimiter,1,config.source_file+".primary_key.tsv")
    logging.info("completed")

    common_keys = set(source_primary_keys_dict.keys()).intersection(target_primary_keys_dict.keys())
    
    do_reconciliation(config)

    #logging.info("Starting partitioning data files...")
    #api.partition_data_files_2(key_position,common_keys,source_primary_keys_dict,target_primary_keys_dict, config,config.file_partition_directory)
    #logging.info("completed")

    # ### 
    # if not os.path.exists(reconciliation_output):
    #     os.makedirs(reconciliation_output)


    # return_values = [ delayed(do_reconciliation_2)(config.file_partition_directory + "\\Source_" + str(partition_num) + ".csv",\
    #                                                config.file_partition_directory + "\\Target_" + str(partition_num) + ".csv",\
    #                                                config.reconciliation_output + "\\Recon_report_" + str(partition_num) + ".csv")  for partition_num in range(1,128)  ]
    # total = delayed(mysum)(return_values)
    # total.compute()


def column_names(dd_df):
    return [col for col in dd_df]

def testing():
    source_file = "file1.csv"
    target_file = "file2.csv"

    sdf = api.get_dataframe(source_file,',',"Yes")
    tdf = api.get_dataframe(target_file,',',"Yes")
    print(sdf.count,tdf.count)
    exit()

if __name__ == "__main__":
    logging.basicConfig(filename='data_comparator.log',level=logging.INFO,format='%(asctime)s %(message)s')
    key_position  = 1
    config = read_configuration(configuration_file)
    
    previous_main()

    logging.shutdown()