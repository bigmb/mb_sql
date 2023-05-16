## Tables to be updated every night by the cron job

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
import typing as tp
from conn import get_engine



def get_public_engine(name,db,user,password,host,port=5432):
    return get_engine('public')

class TableConfig:
    """
    Table configuration object.
    """
    def __init__(
        self,
        schema: str,
        table: str,
        index_col: str,
        chunk_size: int,
        updated_col: str,
        dst_engine: str = "ml",
        dtype: tp.Optional[dict] = None,
    ):
        self.schema = schema
        self.table = table
        self.index_col = index_col
        self.chunk_size = chunk_size
        self.updated_col = updated_col
        self.dst_engine = dst_engine
        self.dtype = dtype

    def get_src_engine(self):
        if self.schema == "public":
            return get_engine('public')

