from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """Checks number of rows to confirm data extraction"""
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_query="",
                 expected_result = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_query = dq_query
        self.expected_result = expected_result

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        records = redshift_hook.get_records(self.dq_query)
        
        
        if records[0][0] != self.expected_result:
            raise ValueError(f"Data quality check failed")
        else:
            self.log.info(f"Data quality on table check passed")
