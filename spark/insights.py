from os.path import join
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, concat_ws, split, reverse, col
from pathlib import Path


BASE_FOLDER = join(
    str(Path("~/airflow").expanduser()),
    "/home/andre/airflow/datalake/dim_vendas/{stage}/{partition}"
)

def create_parent_folder(file_path):
    Path(Path(file_path)).mkdir(exist_ok=True)

def produtos(spark, src):
    produtos = spark.read.json(src)
    produtos = produtos.toPandas()

    return produtos

def vendedores(spark, src):
    vendedores = spark.read.json(src)
    vendedores = vendedores.toPandas()

    return vendedores

def export_json(df, dest, file):
    create_parent_folder(dest)
    caminho = join(dest, file)

    df.to_json(caminho, orient="records", force_ascii=False)

def transform(spark, src, dest):

    df_produtos = produtos(spark, 
        BASE_FOLDER.format(stage=src, partition="produtos")
    )
    df_vendedores = vendedores(spark, 
        BASE_FOLDER.format(stage=src, partition="vendedores")
    )
    
    export_json(df_produtos,
        BASE_FOLDER.format(stage=dest, partition="produtos"),
        'produtos.json'
    )
    export_json(df_vendedores, 
        BASE_FOLDER.format(stage=dest, partition="vendedores"),
        'vendedores.json'
    )

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description="Json Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    #parser.add_argument("--process-date", required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("gold")\
        .getOrCreate()
    
    #transform(spark, args.src, args.dest, args.process_date)
    transform(spark, args.src, args.dest)