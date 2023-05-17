import sqlalchemy as sa
import pandas as pd

__all__ = ['read_sql','engine_execute','to_sql']

def trim_sql_query(sql_query: str) -> str:
    """
    Remove extra whitespace from a SQL query.
    """
    sql_query = " ".join(sql_query.splitlines())
    sql_query = " ".join(sql_query.split())
    return sql_query

def read_sql(query,engine,index_col=None,chunk_size=10000,logger=None):
    """Read SQL query into a DataFrame.
    
    Args:
        engine (sqlalchemy.engine.base.Engine): Engine object.
        query (str): SQL query.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        df (pandas.core.frame.DataFrame): DataFrame object.
    """
    try:

        if isinstance(query, str):
            query = trim_sql_query(query)
            query = sa.text(query)
        elif isinstance(query, sa.sql.selectable.Select):
            query = query
        
        if chunk_size==None or chunk_size==0:
            with engine.begin() as conn:
                df = pd.read_sql(query,conn,index_col=index_col)
            return df
            
        with engine.begin() as conn:
            df= pd.DataFrame()
            for chunk in pd.read_sql(query,conn,index_col=index_col,chunksize=chunk_size):
                df = pd.concat([df,chunk],ignore_index=True)
            
        if logger:
            logger.info(f'SQL query executed successfully.')
        return df
    except sa.exc.SQLAlchemyError as e:
        if logger:
            logger.error(f'Error executing SQL query.')
        raise e
    
def engine_execute(engine, query_str):
    """
    Execute a query on a SQLAlchemy engine object.
    
    Args:
        engine (sqlalchemy.engine.base.Engine): Engine object.
        query_str (str): Query string.
    Returns:
        result (sqlalchemy.engine.result.ResultProxy): Result object.
    """
    if isinstance(query_str, str):
        query = sa.text(query_str)
    else:
        query = query_str
    
    if isinstance(engine, sa.engine.Engine):
        with engine.begin() as conn:
            return conn.execute(query)
    elif isinstance(engine, sa.engine.Connection):
        return engine.execute(query)
    
    
def to_sql(df,engine,table_name,schema=None,if_exists='replace',index=False,index_label=None,chunksize=10000,logger=None):
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
    
    