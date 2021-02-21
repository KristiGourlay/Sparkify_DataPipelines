from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Loads data to dimension tables"""
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_load_data="",
                 truncated_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.sql_load_data=sql_load_data
        self.truncated_data=truncated_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncated_data:
            
            redshift.run(f'TRUNCATE TABLE {self.table}')
            self.log.info(f'Trunicating data for table {self.table}')
            
        redshift.run(f'INSERT INTO {self.table} {self.sql_load_data}')                     
        self.log.info(f'Loading data into {self.table} table')
