from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """Stages data in Redshift"""
    
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            {} ;
            """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",                
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 copy_json_option="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region= region
        self.copy_json_option = copy_json_option
       


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Copying data from s3 to Redshift')
        redshift.run("""COPY {}
                        FROM '{}'
                        ACCESS_KEY_ID '{}'
                        SECRET_ACCESS_KEY '{}'
                        REGION 'us-west-2'
                        FORMAT AS JSON '{}'""".format(self.table,
                                                      's3://' + self.s3_bucket + '/' + self.s3_key,
                                                      credentials.access_key,
                                                      credentials.secret_key,
                                                      self.copy_json_option))

