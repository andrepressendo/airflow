from airflow.plugins_manager import AirflowPlugin
from operators.extract_operator import ExtractOperator

class ExtractPlugin(AirflowPlugin):
    name = "extract"
    operators = [ExtractOperator] 

if __name__ == '__main__':
    print('### Extract Plugin: ExtractOperator ', dir(ExtractOperator))