from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_table_statement="",
                 table="",
                 truncate_ind="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_table_statement = insert_table_statement
        self.table = table
        self.truncate_ind = truncate_ind

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f"Clearing data from destination Redshift table {self.table} based on truncate_ind")
        if self.truncate_ind == "Y":
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Loading data into the Redshift Dimension table {self.table}")
        redshift.run("INSERT INTO {} {}".format(self.table,self.insert_table_statement))

        
