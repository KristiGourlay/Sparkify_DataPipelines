from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator

class CreateTablesOperator(BaseOperator):
    """Creates fact and dimension tables in schema"""

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 create_statements = [],
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.create_statements = create_statements

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for table, create_statement in zip(self.tables, self.create_statements):
            
            self.log.info(f'Creating {table} table')

            redshift.run(f'DROP TABLE IF EXISTS {table}')
            redshift.run(create_statement)