from ast import Import
import sys
from urllib.parse import ParseResultBytes
sys.path.append('C:\BCBS_HDM\hashmap_data_migrator_teradata')
from hdm.core.dao.snowflake import Snowflake
from hdm.core.utils.project_config import ProjectConfig
from hdm.core.sink.snowflake_s3_copy_sink import SnowflakeS3CopySink
from hdm.core.state_management.state_manager import StateManager
from hdm.core.state_management.snowflake_state_manager import SnowflakeStateManager
from hdm.core.error.hdm_error import HDMError
from hdm.core.query_templates.query_templates import QueryTemplates
import yaml
import pandas as pd
import numpy as np
import click
import os
from hdm.core.utils.parse_config import ParseConfig
from hdm.core.utils.project_config import ProjectConfig
import logging.config
import uuid
import datetime 
from jinjasql import JinjaSql
from hdm.core.utils.generic_functions import GenericFunctions
import time
from hdm.core.dao.teradata import Teradata
import traceback

default_log_settings = "/core/logs/log_settings.yml"


@click.command()
@click.option('-m', '--manifest', type=str, help='path of manifest to run')
@click.option('-l', '--log_settings', type=str, default=default_log_settings, help="log settings path")
@click.option('-e', '--env', default="prod", type=str, help="environment to take connection information from in hdm_profiles.yml")
@click.option('-s', '--data_staging', type=str, help="path where data files will be staged locally")    

def run(manifest, log_settings, env, data_staging):    
    os.environ['HDM_ENV'] = env
    if log_settings == default_log_settings:
        log_settings = os.path.abspath(os.path.dirname(__file__) + default_log_settings)

    log_setting_values = ParseConfig.parse(config_path=log_settings)
    
    os.environ['HDM_MANIFEST'] = manifest
    os.environ['HDM_DATA_STAGING'] = data_staging
    with open(f"{ProjectConfig.hdm_home()}/{ProjectConfig.profile_path()}", 'r') as stream:
        conn_conf = yaml.safe_load(stream)[ProjectConfig.hdm_env()]

    with open(manifest, 'r') as stream:
        workflow = yaml.safe_load(stream)

    # Prepare the tables dictionary for easy reference later
    current_timestamp = str(datetime.datetime.now()).replace(' ','').replace('-','').replace(':','').replace('.','')
    table_dict = {}
    for table in workflow['source']['conf']['scenarios']:
        table_dict[table['table_name']] = table

    run_id = None
    # Get the latest run id for corresponding import run for the same manifest file
    fetch_run_id_attempt = 0
    fetch_run_id_max_attempts = ProjectConfig.wait_for_iterations_max_attempt()
    fetch_run_id_timeout = ProjectConfig.wait_for_iterations()
    state_manager_connector = workflow['state_manager']['conf']['connection']
    
    logpathsf = os.path.join(workflow['source']['conf']['directorypath'], 
                                workflow['sink']['name'], 
                                'SnowflakeImporter_' + current_timestamp + "_Process.log")
    loggersf = getlogger(log_setting_values, logpathsf)    

    while fetch_run_id_attempt < fetch_run_id_max_attempts:
        try:            
            loggersf.info(f"Fetching latest run_id for {manifest}")
            run_id = fetch_latest_run_id_for_manifest(manifest, state_manager_connector)        
            fetch_run_id_attempt = fetch_run_id_attempt + 1
            if run_id != None:                
                loggersf.info(f"Run Id found for {manifest}: {str(run_id)}")
                break
            else:                
                if fetch_run_id_attempt < fetch_run_id_max_attempts:
                    loggersf.info(f"Run Id not found for {manifest}. Sleeping for {str(fetch_run_id_timeout)} seconds")
                    time.sleep(fetch_run_id_timeout)
                    fetch_run_id_timeout = fetch_run_id_timeout * fetch_run_id_max_attempts
                else:
                    raise ValueError(f"Could not fetch run_id for {manifest} after {str(fetch_run_id_max_attempts)} attempts")
        except Exception:
            error_handler(loggersf, traceback.format_exc())
            raise HDMError(traceback.format_exc())

    if run_id != None:        
        
        # Check if any iterations are pending on summary table
        while get_pending_iterations_from_state_manager_summary(run_id, manifest, len(table_dict), state_manager_connector) > 0:
            loggersf = getlogger(log_setting_values, logpathsf)
            importiterations = pd.DataFrame()
            # Check if the Teradata to S3 export has completed
            importiterations = get_state_manager_entries_to_begin_import(run_id, manifest, state_manager_connector) 
            import_iterations_completed = 0            
            previous_table = ''   

            if importiterations.shape[0] > 0:                
                loggersf.info(f"Found {str(importiterations.shape[0])} entry/entries to import.")
                for index, row in importiterations.iterrows():
                    logpath = os.path.join(workflow['source']['conf']['directorypath'], 
                                workflow['sink']['name'], 
                                GenericFunctions.table_to_folder(row['SOURCE_ENTITY']), 'logs',
                                row['SOURCE_ENTITY'][len(row['SOURCE_ENTITY'].split('.')[0]) + 1:] + '_' + current_timestamp + "_Import.log")
                    logger = getlogger(log_setting_values, logpath)                    

                    last_iteration = False
                    if previous_table != row['SOURCE_ENTITY']:
                        import_iterations_completed = row['IMPORT_ITERATIONS_COMPLETED']
                    previous_table = row['SOURCE_ENTITY']

                    data_dict = {}
                    
                    if import_iterations_completed == row['EXPORT_ITERATIONS'] - 1:
                        last_iteration = True

                    if 'cdc' in table_dict[row['SOURCE_ENTITY']]:
                        merge_mode = table_dict[row['SOURCE_ENTITY']]['cdc']['mode']
                    
                    if merge_mode != None and (merge_mode in ['merge', 'deleteinsert'] or (merge_mode == 'incremental' and last_iteration)):                        
                        logger.info(f"Getting primary keys for table {row['SOURCE_ENTITY']}")
                        data_dict['primarykeys'] = get_primary_keys(table_dict[row['SOURCE_ENTITY']]['cdc']['keytable']['tablename'],
                                                              table_dict[row['SOURCE_ENTITY']]['cdc']['keytable']['tablenamecolumn'],
                                                              table_dict[row['SOURCE_ENTITY']]['cdc']['keytable']['keycolumn'],
                                                              row['SOURCE_ENTITY'].split('.')[1],
                                                              workflow['source']['conf']['env']
                                                             )
                    else:
                        data_dict['primarykeys'] = []
                    
                    
                    
                    state_manager = generate_state_manager(workflow, manifest, run_id, loggersf) 

                    for column in importiterations.columns:
                        data_dict[column.lower()] = row[column]

                    snowflakeobj = SnowflakeS3CopySink(env=workflow['sink']['conf'], stage_name=row['SOURCE_ENTITY'], 
                                               iteration=row['SINK_ENTITY'].replace(f"{conn_conf[workflow['sink']['conf']['source_env']]['url']}/migration/{GenericFunctions.table_to_folder(row['SOURCE_ENTITY'])}/",""), 
                                               state_manager= state_manager, data_dict = data_dict, table = table_dict[row['SOURCE_ENTITY']], 
                                               last_iteration = last_iteration, source_for_copy=row['SINK_ENTITY'])
                    snowflakeobj.produce()
                    update_teradata_source_to_success(row['STATE_ID'], state_manager_connector)
                    update_iteration_on_summary(run_id, manifest, row['SOURCE_ENTITY'], state_manager_connector)
                    import_iterations_completed = import_iterations_completed + 1

            else:                
                loggersf = getlogger(log_setting_values, logpathsf)
                loggersf.info(f"No entries available yet that are ready to import. Sleeping for {str(ProjectConfig.wait_for_extract_to_be_queued_for_import())} seconds")
                time.sleep(ProjectConfig.wait_for_extract_to_be_queued_for_import())   

def getlogger(log_setting_values, logpathsf):    
    if not os.path.isdir(os.path.dirname(logpathsf)):
        os.makedirs(os.path.dirname(logpathsf))

    log_setting_values['handlers']['file']['filename'] = logpathsf
    logging.config.dictConfig(log_setting_values)
    loggersf = logging.getLogger(__name__)
    loggersf.disabled = False
    return loggersf

def update_iteration_on_summary(run_id, manifest, table_name, conn):
    query_template = QueryTemplates.update_iteration_on_summary
    params = {
        'run_id': run_id,
        'summary_table': ProjectConfig.state_manager_summary_table_name(),
        'table_name': table_name,
        'manifest': manifest,
        'last_update': str(datetime.datetime.now())
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params
    try:
        with Snowflake(connection=conn).connection as snowconn:
            cursor = snowconn.cursor()        
            cursor.execute(query)            
    except Exception as e:
        raise HDMError(f"Error in updating state manager TeradataSource entry to success: {str(e)}")
    finally:
        cursor.close()

def error_handler(loggersf, exception_message: str) -> None:
    error_message = f"Error occurred in Source: {exception_message}"
    loggersf.exception(error_message)

def get_time_in_logger_format():
    return str(datetime.datetime.now()) + " - HashMapSnowflakeImporter - INFO - "
def update_teradata_source_to_success(state_id, conn):
    query_template = QueryTemplates.teradata_update_source_to_success
    params = {
        'state_id': state_id,
        'state_manager_table': ProjectConfig.state_manager_table_name(),
        'last_update': str(datetime.datetime.now())
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params
    try:
        with Snowflake(connection=conn).connection as snowconn:
            cursor = snowconn.cursor()        
            cursor.execute(query)        
    except Exception as e:
        raise HDMError(f"Error in updating state manager TeradataSource entry to success: {str(e)}")
    finally:
        cursor.close()

def generate_state_manager(data_link_config: dict, manifest_name: str, run_id, logger) -> StateManager:
    state_manager = SnowflakeStateManager(configuration=data_link_config['state_manager'])
    if not isinstance(state_manager, StateManager):
        error = f'State manager {type(state_manager)} is not a StateManager.'
        logger.error(error)
        raise HDMError(error)

    state_manager.run_id = run_id
    state_manager.job_id = StateManager.generate_id()
    state_manager.manifest_name = manifest_name
    state_manager.source = {'name': 's3_stg', 'type': 'S3Source'}                                 
    state_manager.sink = {'name': 's3_stg', 'type': 'SnowflakeS3CopySink'} 

    return state_manager

def get_primary_keys(keytablename, keytablenamecolumn, keycolumn, table_name, conn):        
    primarykeys = []
    
    query_template = QueryTemplates.teradata_get_primary_keys
    params = {
        'keytablename': keytablename,
        'keytablenamecolumn': keytablenamecolumn,
        'keycolumn': keycolumn,
        'table_name': table_name
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params

    try:
        with Teradata(connection=conn).connection as conn:
            df = pd.read_sql(query, conn)
            primarykeys = df[keycolumn].to_list()
    except Exception as e:
        raise HDMError(f"Error in fetching primary keys: {str(e)}")

    return primarykeys

def get_pending_iterations_from_state_manager_summary(run_id, manifest_name, table_count, conn):
    pending_iterations_count = 0
    query_template = QueryTemplates.get_pending_iterations_from_state_manager_summary
    params = {
        'manifest_name': manifest_name,
        'run_id': run_id,
        'summary_table': ProjectConfig.state_manager_summary_table_name(),
        'table_count': table_count
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params
    try:
        with Snowflake(connection=conn).connection as snowconn:
            cursor = snowconn.cursor()        
            results = cursor.execute(query).fetchall()
            for result in results:
                pending_iterations_count = result[0]
                break        
    except Exception as e:
        raise HDMError(f"Error in fetching pending transactions from state manager summary: {str(e)}")
    finally:
        cursor.close()
    return pending_iterations_count 

def get_state_manager_entries_to_begin_import(run_id, manifest_name, conn):
    query_template = QueryTemplates.get_state_manager_entries_to_begin_import
    params = {
        'manifest_name': manifest_name,
        'run_id': run_id,
        'summary_table': ProjectConfig.state_manager_summary_table_name(),
        'state_manager_table': ProjectConfig.state_manager_table_name()
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params
    importiterations = pd.DataFrame()
    columns = []
    try:
        with Snowflake(connection=conn).connection as snowconn:
            cursor = snowconn.cursor()
            data = cursor.execute(query).fetchall()   
            for column in cursor.description:
                columns.append(column[0])
            importiterations = pd.DataFrame(data, columns=columns)            
    except Exception as e:
        raise HDMError(f"Error in fetching state manager entries to begin import: {str(e)}")
    finally:
        cursor.close()
    return importiterations 

def fetch_latest_run_id_for_manifest(manifest, conn):
    run_id = None
    query_template = QueryTemplates.get_run_id_from_summary_using_manifest
    params = {
        'manifest': manifest,
        'summary_table': ProjectConfig.state_manager_summary_table_name()
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params
    try:
        with Snowflake(connection=conn).connection as snowconn:
            cursor = snowconn.cursor()        
            results = cursor.execute(query).fetchall()
            for result in results:
                run_id = result[0]
                break                    
    except Exception as e:
        raise HDMError(f"Error in fetching latest run id: {str(e)}")
    finally:
        cursor.close()
    return run_id   

if __name__ == "__main__":
    run()
