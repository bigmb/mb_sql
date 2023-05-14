import sqlalchemy as sa
from mb_utils.src.logging import logger
import pandas as pd

__all__ = ['read_sql','engine_execute','to_sql']

def read_sql(query,engine,index_col=None,chunk_size=10000,logger=logger):
    """Read SQL query into a DataFrame.
    
    Args:
        engine (sqlalchemy.engine.base.Engine): Engine object.
        query (str): SQL query.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        df (pandas.core.frame.DataFrame): DataFrame object.
    """
    try:
        df = pd.read_sql(query,engine,index_col=index_col,chunksize=chunk_size)
        if logger:
            logger.info(f'SQL query executed successfully.')
        return df
    except Exception as e:
        if logger:
            logger.error(f'Error executing SQL query.')
        raise e
    
def engine_execute(engine,query_str,logger=logger):
    """
    Execute a SQL query.
    
    Args:
        engine (sqlalchemy.engine.base.Engine): Engine object.
        query_str (str): SQL query.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        None
    """
    try:
        engine.execute(query_str)
        if logger:
            logger.info(f'SQL query executed successfully.')
    except Exception as e:
        if logger:
            logger.error(f'Error executing SQL query.')
        raise e
    
def to_sql(df,engine,table_name,schema=None,if_exists='replace',index=False,index_label=None,chunksize=10000,logger=logger):
    """Write records stored in a DataFrame to a SQL database.
    
    Args:
        df (pandas.core.frame.DataFrame): DataFrame object.
        engine (sqlalchemy.engine.base.Engine): Engine object.
        table_name (str): Name of the table.
        schema (str): Name of the schema. Default: None
        if_exists (str): How to behave if the table already exists. Default: 'replace'
        index (bool): Write DataFrame index as a column. Default: False
        index_label (str): Column label for index column(s). If None is given (default) and index is True, then the index names are used. A sequence should be given if the DataFrame uses MultiIndex. Default: None
        chunksize (int): Number of rows to write at a time. Default: 10000
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        None
    """
    try:
        if index:
            if index_label is None:
                index_label = df.index.name
        df.to_sql(table_name,engine,schema=schema,if_exists=if_exists,index=index,index_label=index_label,chunksize=chunksize)
        if logger:
            logger.info(f'DataFrame written to {table_name} table.')
    except Exception as e:
        if logger:
            logger.error(f'Error writing DataFrame to {table_name} table.')
        raise e
    
    