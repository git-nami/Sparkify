from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_table_statement="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_table_statement = insert_table_statement
        self.table = table


    def execute(self, context):

        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f"Loading data into Redshift Fact table {self.table}")
        redshift.run("INSERT INTO {} {}".format(self.table,self.insert_table_statement))
