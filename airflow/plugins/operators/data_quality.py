from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class PostgresDataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id = "",
                 data_quality_checks = {},
                 *args, **kwargs):

        super(PostgresDataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.data_quality_checks = data_quality_checks
        
    def execute(self, context):
        self.log.info('DataQualityOperator')
        postgres = PostgresHook(self.postgres_conn_id)
        
        error_count = 0
        failed_cases = []
        for sql, expect_result in self.data_quality_checks.items():
            result = postgres.get_records(sql)[0][0]
            if result != expect_result:
                error_count += 1
                failed_cases.append(sql)
        
        if error_count:
            self.log.warning(f"number of failed test case : {error_count}")
            self.log.warning(failed_cases)
            raise ValueError("Data quality check failed")
            
        self.log.warning("Data quality check passed")