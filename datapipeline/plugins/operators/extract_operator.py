#https://s3.amazonaws.com/gupy5/production/companies/7198/emails/1669313089815/d0dc9130-6c21-11ed-b5b6-af1378b9873a/vendedores.json
import requests
import json
from pathlib import Path
from os.path import join
from pandas import DataFrame

from airflow.models import DAG, BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta

class ExtractOperator(BaseOperator):

    template_fields =[
        "query",
        "file_path",
        "url",
        "file"
    ]

    @apply_defaults
    def __init__(
        self,
        query,
        file_path,
        url,
        file,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.url = url
        self.file = file

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        ## Produtos: 'https://s3.amazonaws.com/gupy5/production/companies/7198/emails/1669313089802/d0d7fd50-6c21-11ed-b062-6369d69cae2d/produtos.json'
        ## Vendedores: 'https://s3.amazonaws.com/gupy5/production/companies/7198/emails/1669313089815/d0dc9130-6c21-11ed-b5b6-af1378b9873a/vendedores.json'
        url = self.url
        json_obj = requests.get(url)
        self.create_parent_folder()
        with open(self.file_path, 'w') as output_file:
            #for obj in json_obj.json():
            obj = json_obj.json()
            obj = json.loads(DataFrame(obj).to_json(orient='records'))
            json.dump(obj, output_file, ensure_ascii=False)
            output_file.write("\n")


if __name__=="__main__":
    with DAG(dag_id="base_franq", start_date=datetime.now()) as dag:
        to = ExtractOperator(
            query="Produtos",
            url="https://s3.amazonaws.com/gupy5/production/companies/7198/emails/1669313089815/d0dc9130-6c21-11ed-b5b6-af1378b9873a/vendedores.json",
            file="vendedores",
            file_path=join(
                "/home/andre/airflow/datalake",
                "vendedores",
                #"extract_date={{ ds }}",
                "Vendedores_{{ ds }}.json"
            ),
            task_id="teste_run"
        )
        
        to.run()
