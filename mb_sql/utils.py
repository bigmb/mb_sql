## file for extra sql functions

import sqlalchemy as sa
import pandas as pd
from .sql import trim_sql_query,read_sql,engine_execute
import os

__all__ = ['list_schemas','rename_table','drop_table','drop_schema','create_schema','create_index','clone_db']

def list_schemas(engine,logger=None) -> pd.DataFrame:
    """
    Returns list of schemas in database.
    
    Args:
        engine (sqlalchemy.engine.base.Engine): Engine object.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        df (pandas.core.frame.DataFrame): DataFrame object.
    """
    q1 ="SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema') ORDER BY schema_name;"
    return read_sql(q1,engine,logger=logger)

def list_tables(engine,schema=None,logger=None) -> pd.DataFrame:
    """
    Returns list of tables in database.
    
    Args:
        engine (sqlalchemy.engine.base.Engine): Engine object.
        schema (str): Name of the schema. Default: None
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        df (pandas.core.frame.DataFrame): DataFrame object.
    
    """
    q1 = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"
    return read_sql(q1,engine,logger=logger)
    
    

def rename_table(new_table_name,old_table_name,engine,schema_name=None,logger=None):
    """
    Rename table in database.
    
    Args:
        new_table_name (str): New name of the table.
        old_table_name (str): Old name of the table.
        engine (sqlalchemy.engine.base.Engine): Engine object.
        schema_name (str): Name of the schema. Default: None
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        None
    """
    if schema_name:
        q1 = f"ALTER TABLE {schema_name}.{old_table_name} RENAME TO {schema_name}.{new_table_name};"
    else:
        q1 = f"ALTER TABLE {old_table_name} RENAME TO {new_table_name};"
    engine_execute(q1,engine,logger=logger)
    if logger:
        logger.info(f'Table {old_table_name} renamed to {new_table_name}.')

def drop_table(table_name,engine,schema_name=None,logger=None):
    """
    Drop table in database.
    
    Args:
        table_name (str): Name of the table.
        engine (sqlalchemy.engine.base.Engine): Engine object.
        schema_name (str): Name of the schema. Default: None
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        None
    """
    if schema_name:
        q1 = f"DROP TABLE {schema_name}.{table_name};"
    else:
        q1 = f"DROP TABLE {table_name};"
    engine_execute(q1,engine,logger=logger)
    if logger:
        logger.info(f'Table {table_name} dropped.')
        
def drop_schema(schema_name,engine,logger=None):
    """
    Drop schema in database.
    
    Args:
        schema_name (str): Name of the schema.
        engine (sqlalchemy.engine.base.Engine): Engine object.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        None
    """
    q1 = f"DROP SCHEMA {schema_name};"
    engine_execute(q1,engine,logger=logger)
    if logger:
        logger.info(f'Schema {schema_name} dropped.')
        
def create_schema(schema_name,engine,logger=None):
    """
    Create schema in database.
    
    Args:
        schema_name (str): Name of the schema.
        engine (sqlalchemy.engine.base.Engine): Engine object.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        None
    """
    q1 = f"CREATE SCHEMA {schema_name};"
    engine_execute(q1,engine,logger=logger)
    if logger:
        logger.info(f'Schema {schema_name} created.')

def create_index(table,index_col,engine,logger=None):
    """
    Create an Index for a table in database.
    
    Args:
        table (str): Name of the table.
        index_col (str): Name of the index_col in the table.
        engine (sqlalchemy.engine.base.Engine): Engine object.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        None
    """
    q1 = f"CREATE INDEX {index_col} ON {table};"
    engine_execute(q1,engine,logger=logger)
    if logger:
        logger.info(f'Index {index_col} created for table {table}.')
        
def clone_db(ori_db_location,copy_db_location, logger=None):
    """
    Clone a database.
    
    Args:
        ori_db_location (str): Location of the original database.
        copy_db_location (str): Location for the new database.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        None
    """
    if not os.path.exists(ori_db_location):
        raise FileNotFoundError("The original location does not exist.")

    if os.path.exists(copy_db_location):
        copy_db_location = os.path.join(copy_db_location,'copy_db')
    
    if os.path.exists(copy_db_location)==False:
        os.makedirs(copy_db_location)
    
    cmd = f"pg_dump -U postgres -h localhost -p 5432 {ori_db_location} | psql -U postgres -h localhost -p 5432 {copy_db_location}"
    os.system(cmd)

    if logger:
        logger.info(f'Database cloned from {ori_db_location} to {copy_db_location}.')    
    