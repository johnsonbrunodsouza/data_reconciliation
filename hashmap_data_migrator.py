from ast import Import
import sys
from urllib.parse import ParseResultBytes
sys.path.append('C:\BCBS_HDM\hashmap_data_migrator_teradata')
from hdm.core.dao import teradata
from hdm.core.dao.snowflake import Snowflake
from hdm.core.dao.teradata import Teradata
from hdm.core.utils.project_config import ProjectConfig
from hdm.core.source.teradata_source import TeradataSource
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
   
    run_id = uuid.uuid4().hex

    for table in workflow['source']['conf']['scenarios']:  
        try:            
            current_timestamp = str(datetime.datetime.now()).replace(' ','').replace('-','').replace(':','').replace('.','')
            logpath = os.path.join(workflow['source']['conf']['directorypath'], 
                                workflow['source']['name'], 
                                GenericFunctions.table_to_folder(table['table_name']), 'logs',
                                table['table_name'][len(table['table_name'].split('.')[0]) + 1:] + '_' + current_timestamp + "_Process.log")
                                        
            if not os.path.isdir(os.path.dirname(logpath)):
                os.makedirs(os.path.dirname(logpath))   

            log_setting_values['handlers']['file']['filename'] = logpath
            logging.config.dictConfig(log_setting_values)
            logger = logging.getLogger('__main__')
            logger.disabled = False
            last_iteration = False
            iterations = 1 
            year_month_iteration = False
            year_iteration = False
            last_watermark_year = 0
            last_watermark_month = 0
            df = pd.DataFrame()
            stringsuppercase = False
            
            logger.info(f"Initiating export for {table['table_name']}")
            if not table['query']:
                if (not table['cdc']['mode'] or table['cdc']['mode'].lower() == 'incremental') and table['watermark']['column']:
                    logger.info("Building Query for table size")
                    query = build_query_for_sizing(table['table_name'], conn_conf[workflow['source']['conf']['env']]['database'])
                    with Teradata(connection=workflow['source']['conf']['env']).connection as conn:
                        df_size = pd.read_sql(query, conn)
                        if str(df_size.iloc[0, 3]) != 'Full':
                            if str(df_size.iloc[0, 3]) == 'Year':
                                year_iteration = True
                            else:
                                year_month_iteration = True

                            if table['cdc']['mode'] != None and table['cdc']['mode'].lower() == 'incremental':   
                                with Snowflake(connection=workflow['sink']['conf']['env']).connection as snowconn:
                                    cursor = snowconn.cursor()
                                    last_iteration_query = build_query_for_max_watermark(table['table_name'], table['watermark']['column'])
                                    last_watermark_year, last_watermark_month = cursor.execute(last_iteration_query).fetchone()
                                    cursor.close()
                            logger.info("Building Query for iterations")
                            query = build_query_for_iterations(table['table_name'], table['watermark']['column'],str(df_size.iloc[0, 3]), last_watermark_year, last_watermark_month, table['cdc']['mode'])
                            
                            df = pd.read_sql(query, conn)
                            iterations = df.shape[0]
                                
                        else:
                            df = pd.DataFrame()
                            year_month_iteration = False
                            year_iteration = False
                            
            if 'stringsuppercase' in workflow['source']['conf']:
                stringsuppercase = workflow['source']['conf']['stringsuppercase']

            if 'stringsuppercase' in table:
                stringsuppercase = table['stringsuppercase']
            logger.info(f"Config picked: year_month_iteration -> {str(year_month_iteration)}, year_iteration -> {str(year_iteration)}, stringsuppercase -> {stringsuppercase}")           
            # Code for split, insert the iterations values in STATE_MANAGER_SUMMARY
            logger.info(f"Total number of iterations: {str(iterations)}")
            logger.info(f"Insert iterations into STATE_MANAGER_SUMMARY")
            with Snowflake(connection=workflow['state_manager']['conf']['connection']).connection as snowconn:
                cursor = snowconn.cursor()
                summary_table_query = build_query_for_summary_table(run_id, manifest, table['table_name'], df.shape[0])
                cursor.execute(summary_table_query)
                cursor.close()

            logger.info(f"Build schema for table {table['table_name']}")
            schema, query_select_list = build_schema_for_table(table['table_name'], workflow, stringsuppercase)         
            
            for iteration in np.arange(iterations):
                if iteration == iterations - 1:            
                    last_iteration = True
                year = '0'
                month = '0'
                if df.shape[0] > 0:
                    year = str(df.iloc[iteration, 0])
                    month = str(df.iloc[iteration, 1])
                state_manager = generate_state_manager(workflow, manifest, run_id, logger)   
                teradataobj = TeradataSource(workflow=workflow, conn_conf=conn_conf, table=table, 
                                            iteration=f"{current_timestamp}_Iteration_{str(iteration)}", 
                                            state_manager= state_manager, schema = schema, query_select_list = query_select_list, 
                                            year = year, month = month, year_month_iteration = year_month_iteration, year_iteration = year_iteration,
                                            last_iteration = last_iteration)
                ret = teradataobj.consume()
                #state_manager.source = {'name': 's3_stg', 'type': 'S3Source'}                                 
                #state_manager.sink = {'name': 's3_stg', 'type': 'SnowflakeS3CopySink'}     
                #snowflakeobj = SnowflakeS3CopySink(env=workflow['sink']['conf'], stage_name=table['table_name'], 
                #                                   iteration=f"{current_timestamp}_Iteration_{str(iteration)}", 
                #                                   state_manager= state_manager, data_dict = ret, table = table, 
                #                                   last_iteration = last_iteration)
                #snowflakeobj.produce()
        except ValueError as Argument:
            error_handler(logger, Argument)
        except Exception:
            error_handler(logger, traceback.format_exc())
            
            
def generate_state_manager(data_link_config: dict, manifest_name: str, run_id, logger) -> StateManager:
    state_manager = SnowflakeStateManager(configuration=data_link_config['state_manager'])
    if not isinstance(state_manager, StateManager):
        error = f'State manager {type(state_manager)} is not a StateManager.'
        logger.error(error)
        raise HDMError(error)

    state_manager.run_id = run_id
    state_manager.job_id = StateManager.generate_id()
    state_manager.manifest_name = manifest_name
    state_manager.source = {
        'name': data_link_config['source']['name'],
        'type': data_link_config['source']['type'],
    }
    state_manager.sink = {
        'name': 's3_stg',
        'type': 'S3Sink',
    }

    return state_manager

def build_query_for_summary_table(run_id, manifest, table_name, export_iterations):
    query_template = QueryTemplates.snowflake_summary_table_insert
    params = {
        'run_id': run_id,
        'manifest': manifest,
        'table_name': table_name,
        'export_iterations': export_iterations if export_iterations != 0 else export_iterations + 1,
        'create_date': str(datetime.datetime.now()),
        'update_date': str(datetime.datetime.now()),
        'summary_table': ProjectConfig.state_manager_summary_table_name()
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params
    return query

def build_query_for_sizing(table_name, database_name):
    query_template = QueryTemplates.teradata_fetch_sizing_for_table
    params = {
        'table_name': table_name.split('.')[1],
        'database_name': database_name
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params
    return query

def build_query_for_iterations(table_name, watermark_column, iteration, last_watermark_year, last_watermark_month, cdcmode):
    query_template = QueryTemplates.teradata_fetch_iterations_new
    params = {
        'table_name': table_name,
        'watermark_column': watermark_column,
        'iteration': iteration,
        'last_watermark_year': last_watermark_year,
        'last_watermark_month': last_watermark_month,
        'cdcmode': cdcmode
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params
    return query

def build_query_for_max_watermark(table_name, watermark_column):
    query_template = QueryTemplates.snowflake_fetch_max_watermark_values
    params = {
        'table_name': table_name,
        'watermark_column': watermark_column
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params
    return query

def build_query_for_tpt_schema(table):
    query_template = QueryTemplates.teradata_schema_template
    params = {
        'table_name': table.split('.')[1],
        'database_name': table.split('.')[0]
    }
    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(query_template, params)
    query = query % bind_params
    return query

def build_schema_for_table(table, workflow, stringsuppercase):
    try:
        with Teradata(connection=workflow['source']['conf']['env']).connection as conn:               
            df = pd.read_sql(build_query_for_tpt_schema(table), conn)
            
        schema = ""
        query_select_list = ""
        for index, row in df.iterrows():
            schema = schema + f"    \"{row['ColumnName']}\" {row['ColumnDataType']}, \n"
            if row['ColumnType'] == "CV":
                if stringsuppercase:
                    query_select_list = query_select_list + f"UPPER(TRIM(\"{row['ColumnName']}\")), "
                else:
                    query_select_list = query_select_list + f"TRIM(\"{row['ColumnName']}\"), "
            elif row['ColumnType'] == "CF":
                if stringsuppercase:
                    query_select_list = query_select_list + f"UPPER(RTRIM(LTRIM(\"{row['ColumnName']}\"))), "
                else:
                    query_select_list = query_select_list + f"RTRIM(LTRIM(\"{row['ColumnName']}\")), "
            elif row['ColumnType'] == "N" and row['ColumnDataType'] == "NUMBER(38,0)":
                query_select_list = query_select_list + f"CAST(\"{row['ColumnName']}\" AS {row['ColumnDataType']}), "
            else:
                query_select_list = query_select_list + f"\"{row['ColumnName']}\", "
        return schema[:-3], query_select_list[:-2]
    except Exception:        
        raise ValueError("Unable to connect to Teradata source. Please check if source is up. Check the configuration: %s" % workflow['source']['conf']['env'])        

def error_handler(logger, exception_message: str) -> None:
    error_message = f"Error occurred in Source: {exception_message}"
    logger.exception(error_message)

if __name__ == "__main__":
    run()


