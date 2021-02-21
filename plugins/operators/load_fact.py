from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table = "",
                 redshift_conn_id="",
                 sql_load_data="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_load_data = sql_load_data
        

    def execute(self, context):
        
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        redshift.run(f'INSERT INTO {self.table} {self.sql_load_data}')
        self.log.info(f'Loading data into {self.table} table')
