## Tables to be updated every night by the cron job

import sqlalchemy as sa
import typing as tp
from .conn import get_engine


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
        dst_engine: str = "mb_public2",
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
        if self.schema == "mb_public1":
            self.src_engine = get_engine(name='postgresql' , db= 'postgres', user='postgres' , password= 'postgres', host= 'localhost', port= 5432, echo=False)
            return self.src_engine
        
    def get_dst_engine(self):
        if self.schema == "mb_public2":
            self.dst_engine =get_engine(name='postgresql' , db= 'postgres_2', user='postgres' , password= 'postgres', host= 'localhost', port= 5432, echo=False)
        return self.dst_engine



mutable_tables = {
    'table_to_update1': TableConfig('mb_public1',
        "test2",
        None,
        10000,
        None,
        dtype={
            'name': sa.Text,
            'age' : sa.Integer
            },
    ),
    
    'table_to_update2': TableConfig('mb_public1',
        "test3",
        'id',
        10000,
        None,
        dtype={
            'id':sa.Integer,
            'num': sa.Integer,
            'data': sa.Text
        },)   
    }
