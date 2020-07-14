from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_cases={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Performing data quality checks on the tables") 
        
        for test_case,expected_result in self.test_cases.items():
            records = redshift_hook.get_records(f"{test_case}")           
            num_records = records[0][0]            
            if num_records < expected_result:
                self.log.info(f"Test case: {test_case} failed, expected number of records > {expected_result}")
                raise ValueError(f"Test case: {test_case} failed, expected number of records > {expected_result}")
            else:
                self.log.info(f"Test case: {test_case} passed with {num_records}")

        