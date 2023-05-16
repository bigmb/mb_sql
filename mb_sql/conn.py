import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker,declarative_base
from mb_utils.src.logging import logger

__all__ = ['get_engine', 'get_session','get_base','get_metadata','engine_execute']

def get_engine(name:str , db: str, user: str , password: str , host: str , port=5432 , logger=logger, echo=False):
    """Get a SQLAlchemy engine object.

    Args:
        name (str): Name of the database.
        db (str): Database name.
        user (str): Username for the database.
        password (str): Password for the database.
        host (str): Hostname for the database.
        port (str): Port for the database. Default: 5432
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
        echo (bool): Echo SQL statements to stdout. Default: False
    Returns:
        engine (sqlalchemy.engine.base.Engine): Engine object.

    """
    try:
        engine = sa.create_engine(f'{name}://{user}:{password}@{host}:{port}/{db}', echo=echo)
        if logger:
            logger.info(f'Engine created for {name} database.')
        return engine
    except Exception as e:
        if logger:
            logger.error(f'Error creating engine for {name} database.')
        raise e    

def get_session(engine, logger=logger):
    """Get a SQLAlchemy session object.

    Args:
        engine (sqlalchemy.engine.base.Engine): Engine object.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        session (sqlalchemy.orm.session.Session): Session object.

    """
    try:
        session = sessionmaker(bind=engine)()
        if logger:
            logger.info(f'Session created for {engine.url.database} database.')
        return session
    except Exception as e:
        if logger:
            logger.error(f'Error creating session for {engine.url.database} database.')
        raise e
    
def get_base(logger=logger):
    """Get a SQLAlchemy declarative base object.
    
    Args:
        engine (sqlalchemy.engine.base.Engine): Engine object.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        base (sqlalchemy.ext.declarative.api.DeclarativeMeta): Declarative base object.
    """
    try:
        Base = declarative_base()
        if logger:
            logger.info(f'Base created for database.')
        return Base
    except Exception as e:
        if logger:
            logger.error(f'Error creating base for database.')
    
def get_metadata(base,conn, logger=logger):
    """Get a SQLAlchemy metadata object.
    
    Args:
        base (sqlalchemy.ext.declarative.api.DeclarativeMeta): Declarative base object.
        conn (sqlalchemy.engine.base.Connection): Connection object.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
    Returns:
        metadata (sqlalchemy.sql.schema.MetaData): Metadata object.
    """
    try:
        metadata = base.metadata.create_all(conn)
        if logger: 
            logger.info(f'Metadata created for database.')
        return metadata
    except Exception as e:
        if logger: 
            logger.error(f'Error creating metadata for database.')
            

def engine_execute(engine, query_str,logger=logger):
    """
    Execute a query on a SQLAlchemy engine object.
    
    Args:
        engine (sqlalchemy.engine.base.Engine): Engine object.
        query_str (str): Query string.
        logger (logging.Logger): Logger object. Default: mb_utils.src.logging.logger
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
    