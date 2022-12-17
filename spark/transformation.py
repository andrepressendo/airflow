from os.path import join
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, concat_ws, split, reverse, col


def produtos(df):
    df_produto = df\
        .withColumn("desconto_maximo", regexp_replace("desconto_maximo", "%", "")\
        .cast("double")/100)

    return df_produto

def vendedores(df):
    df_vendedores = df\
        .withColumn("idade", col("idade").cast("int"))\
        .withColumn("data_de_entrada", concat_ws("-", reverse(split("data_de_entrada", "/"))))\
        .withColumn("salario_bruto", regexp_replace("salario_bruto", "[{,}, { }, {R$}]", "").cast("double"))
    return df_vendedores

def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)

def transform(spark, src, dest):
    
    df_p = spark.read.json(src+"/produtos.json")
    df_v = spark.read.json(src+"/vendedores.json")

    df_produtos = produtos(df_p)
    df_vendedores = vendedores(df_v)

    table_dest = join(dest, "{table_name}")

    export_json(df_produtos, table_dest.format(table_name="produtos"))
    export_json(df_vendedores, table_dest.format(table_name="vendedores"))



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
        .appName("extract_transformation")\
        .getOrCreate()
    
    #transform(spark, args.src, args.dest, args.process_date)
    transform(spark, args.src, args.dest)